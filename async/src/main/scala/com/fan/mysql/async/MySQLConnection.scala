package com.fan.mysql.async

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import com.fan.mysql.async.codec.{MySQLConnectionHandler, MySQLHandlerDelegate}
import com.fan.mysql.async.db._
import com.fan.mysql.async.exceptions.{ConnectionStillRunningQueryException, DatabaseException, InsufficientParametersException, MySQLException}
import com.fan.mysql.async.message.client.{AuthenticationSwitchResponse, HandshakeResponseMessage, QueryMessage, QuitMessage}
import com.fan.mysql.async.message.server._
import com.fan.mysql.async.pool.TimeoutScheduler
import com.fan.mysql.async.util.ChannelFutureTransformer._
import com.fan.mysql.async.util.{CharsetMapper, ExecutorServiceUtils, Log, NettyUtils}
import io.netty.channel.{ChannelHandlerContext, EventLoopGroup}
import org.slf4j.Logger

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

object MySQLConnection {
  final val Counter: AtomicLong = new AtomicLong()
  final val log: Logger = Log.get[MySQLConnection]
}

class MySQLConnection(
                       configuration: Configuration,
                       charsetMapper: CharsetMapper = CharsetMapper.Instance,
                       group: EventLoopGroup = NettyUtils.DefaultEventLoopGroup,
                       implicit val executionContext: ExecutionContext = ExecutorServiceUtils.CachedExecutionContext
                     ) extends MySQLHandlerDelegate with Connection with TimeoutScheduler {

  import MySQLConnection.log

  // validate that this charset is supported
  charsetMapper.toInt(configuration.charset)

  private final val connectionCount = MySQLConnection.Counter.incrementAndGet()
  private final val connectionId = s"[mysql-connection-$connectionCount]"

  private final val connectionHandler = new MySQLConnectionHandler(
    configuration,
    charsetMapper,
    this,
    group,
    executionContext,
    connectionId)

  private final val connectionPromise = Promise[Connection]()
  private final val disconnectionPromise = Promise[Connection]()

  private val queryPromiseReference = new AtomicReference[Option[Promise[QueryResult]]](None)
  private var connected = false
  private var _lastException: Throwable = _
  private var serverVersion: String = _

  def version: String = this.serverVersion

  def lastException: Throwable = this._lastException

  def count: Long = this.connectionCount

  override def eventLoopGroup: EventLoopGroup = group

  def connect: Future[Connection] = {
    this.connectionHandler.connect().onComplete {
      case Failure(e) => this.connectionPromise.tryFailure(e)
      case _ =>
    }

    this.connectionPromise.future
  }

  def close: Future[Connection] = {
    if (this.isConnected) {
      if (!this.disconnectionPromise.isCompleted) {
        val exception = new DatabaseException("Connection is being closed")
        exception.fillInStackTrace()
        this.failQueryPromise(exception)
        this.connectionHandler.clearQueryState()
        this.connectionHandler.write(QuitMessage.Instance).onComplete {
          case Success(_) =>
            this.connectionHandler.disconnect.onComplete {
              case Success(_) => this.disconnectionPromise.trySuccess(this)
              case Failure(e) => this.disconnectionPromise.tryFailure(e)
            }
          case Failure(exception) => this.disconnectionPromise.tryFailure(exception)
        }
      }
    }

    this.disconnectionPromise.future
  }

  override def connected(ctx: ChannelHandlerContext) {
    log.debug("Connected to {}", ctx.channel.remoteAddress)
    this.connected = true
  }

  override def exceptionCaught(throwable: Throwable) {
    log.error("Transport failure ", throwable)
    setException(throwable)
  }

  override def onError(message: ErrorMessage) {
    log.error("Received an error message -> {}", message)
    val exception = new MySQLException(message)
    exception.fillInStackTrace()
    this.setException(exception)
  }

  private def setException(t: Throwable) {
    this._lastException = t
    this.connectionPromise.tryFailure(t)
    this.failQueryPromise(t)
  }

  override def onOk(message: OkMessage) {
    if (!this.connectionPromise.isCompleted) {
      log.debug("Connected to database")
      this.connectionPromise.success(this)
    } else {
      if (this.isQuerying) {
        this.succeedQueryPromise(
          new MySQLQueryResult(
            message.affectedRows,
            message.message,
            message.lastInsertId,
            message.statusFlags,
            message.warnings
          )
        )
      } else {
        log.warn("Received OK when not querying or connecting, not sure what this is")
      }
    }
  }

  def onEOF(message: EOFMessage) {
    if (this.isQuerying) {
      this.succeedQueryPromise(
        new MySQLQueryResult(
          0,
          null,
          -1,
          message.flags,
          message.warningCount
        )
      )
    }
  }

  override def onHandshake(message: HandshakeMessage) {
    this.serverVersion = message.serverVersion

    this.connectionHandler.write(HandshakeResponseMessage(
      configuration.username,
      configuration.charset,
      message.seed,
      message.authenticationMethod,
      database = configuration.database,
      password = configuration.password
    ))
  }

  override def switchAuthentication(message: AuthenticationSwitchRequest) {
    this.connectionHandler.write(AuthenticationSwitchResponse(configuration.password, message))
  }

  def sendQuery(query: String): Future[QueryResult] = {
    this.validateIsReadyForQuery()
    val promise = Promise[QueryResult]()
    this.setQueryPromise(promise)
    this.connectionHandler.write(QueryMessage(query))
    addTimeout(promise, configuration.queryTimeout)
    promise.future
  }

  private def failQueryPromise(t: Throwable) {
    this.clearQueryPromise.foreach {
      _.tryFailure(t)
    }
  }

  private def succeedQueryPromise(queryResult: QueryResult) {
    this.clearQueryPromise.foreach {
      _.success(queryResult)
    }
  }

  def isQuerying: Boolean = this.queryPromise.isDefined

  def onResultSet(resultSet: ResultSet, message: EOFMessage) {
    if (this.isQuerying) {
      this.succeedQueryPromise(
        new MySQLQueryResult(
          resultSet.size,
          null,
          -1,
          message.flags,
          message.warningCount,
          Some(resultSet)
        )
      )
    }
  }

  def disconnect: Future[Connection] = this.close

  override def onTimeout(): Unit = disconnect

  def isConnected: Boolean = this.connectionHandler.isConnected

  def sendPreparedStatement(query: String, values: Seq[Any]): Future[QueryResult] = {
    this.validateIsReadyForQuery()
    val totalParameters = query.count(_ == '?')
    if (values.length != totalParameters) {
      throw new InsufficientParametersException(totalParameters, values)
    }
    val promise = Promise[QueryResult]()
    this.setQueryPromise(promise)
    this.connectionHandler.sendPreparedStatement(query, values)
    addTimeout(promise, configuration.queryTimeout)
    promise.future
  }


  override def toString: String = {
    "%s(%s,%d)".format(this.getClass.getName, this.connectionId, this.connectionCount)
  }

  private def validateIsReadyForQuery() {
    if (isQuerying) {
      throw new ConnectionStillRunningQueryException(this.connectionCount, false)
    }
  }

  private def queryPromise: Option[Promise[QueryResult]] = queryPromiseReference.get()

  private def setQueryPromise(promise: Promise[QueryResult]) {
    if (!this.queryPromiseReference.compareAndSet(None, Some(promise)))
      throw new ConnectionStillRunningQueryException(this.connectionCount, true)
  }

  private def clearQueryPromise: Option[Promise[QueryResult]] = {
    this.queryPromiseReference.getAndSet(None)
  }

}
