package com.fan.mysql.async

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import com.fan.mysql.async.binlog.BinlogDumpContext
import com.fan.mysql.async.codec.{MySQLConnectionHandler, MySQLHandlerDelegate}
import com.fan.mysql.async.db._
import com.fan.mysql.async.exceptions.{ConnectionStillRunningQueryException, DatabaseException, InsufficientParametersException, MySQLException}
import com.fan.mysql.async.message.client._
import com.fan.mysql.async.message.server._
import com.fan.mysql.async.pool.TimeoutScheduler
import com.fan.mysql.async.util.ChannelFutureTransformer._
import com.fan.mysql.async.util._
import io.netty.channel.{ChannelHandlerContext, EventLoopGroup}
import org.apache.commons.lang3.StringUtils
import org.slf4j.Logger

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Random, Success}

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

  // Java constructor
  def this(configuration: Configuration) = this(configuration, CharsetMapper.Instance)

  import MySQLConnection.log

  // validate that this charset is supported
  charsetMapper.toInt(configuration.charset)

  private[this] final val connectionCount = MySQLConnection.Counter.incrementAndGet()
  private[this] final val connectionId = s"[mysql-connection-$connectionCount]"

  private[this] final val connectionHandler = new MySQLConnectionHandler(
    configuration,
    charsetMapper,
    this,
    group,
    executionContext,
    connectionId)

  private[this] final val connectionPromise = Promise[Connection]()
  private[this] final val disconnectionPromise = Promise[Connection]()

  private[this] val queryPromiseReference = new AtomicReference[Option[Promise[QueryResult]]](None)
  private[this] var connected = false
  private[this] var _lastException: Throwable = _
  private[this] var serverVersion: String = _

  private[this] val slaveId = math.abs(Random.nextLong(99999999)) + 1

  private[this] val timeZone = "GMT-0:00"

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

  override def connected(ctx: ChannelHandlerContext): Unit = {
    log.debug("Connected to {}", ctx.channel.remoteAddress)
    this.connected = true
  }

  override def exceptionCaught(throwable: Throwable): Unit = {
    log.error("Transport failure ", throwable)
    setException(throwable)
  }

  override def onError(message: ErrorMessage): Unit = {
    log.error("Received an error message -> {}", message)
    val exception = new MySQLException(message)
    exception.fillInStackTrace()
    this.setException(exception)
  }

  private def setException(t: Throwable): Unit = {
    this._lastException = t
    this.connectionPromise.tryFailure(t)
    this.failQueryPromise(t)
  }

  override def onOk(message: OkMessage): Unit = {
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

  def onEOF(message: EOFMessage): Unit = {
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

  override def onHandshake(message: HandshakeMessage): Unit = {
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

  override def switchAuthentication(message: AuthenticationSwitchRequest): Unit = {
    this.connectionHandler.write(AuthenticationSwitchResponse(configuration.password, message))
  }

  def dump(dumpString: String): Unit = {
    this.validateIsReadyForQuery()

    log.info(s"COM_BINLOG_DUMP with position: $dumpString")

    // todo we don't check set statement result here, we have to check it.
    for {
      _ <- this.sendQuery("set wait_timeout=9999999")
      _ <- this.sendQuery("set net_write_timeout=1800")
      _ <- this.sendQuery("set net_read_timeout=1800")
      _ <- this.sendQuery("set names 'binary'")
      _ <- this.sendQuery("set @master_binlog_checksum= @@global.binlog_checksum")
      checksumType <- this.sendQuery("select @master_binlog_checksum")
    } yield {

      val dumpContext = checksumType.rows.map(_.apply(0).apply(0)) match {
        case None => new BinlogDumpContext(MySQLConstants.BINLOG_CHECKSUM_ALG_OFF)
        case Some(a: String) if a == "CRC32" =>
          log.debug(s"Dump binlog checksum type is CRC32")
          new BinlogDumpContext(MySQLConstants.BINLOG_CHECKSUM_ALG_CRC32)
        case _ => new BinlogDumpContext(MySQLConstants.BINLOG_CHECKSUM_ALG_OFF)
      }

      dumpContext.setTimeZone(timeZone)

      // todo we should provide two api to distinguish between dump file and dump gtid.
      if (StringUtils.isEmpty(dumpString)) {
        this.connectionHandler.write(
          BinlogDumpMessage("", 4, this.slaveId), dumpContext)
      } else if (dumpString.contains(".") && dumpString.contains(":")) {
        val splits = dumpString.split(":")
        this.connectionHandler.write(BinlogDumpMessage(splits(0), splits(1).toInt, this.slaveId), dumpContext)
      } else {
        this.connectionHandler.write(BinlogDumpGTIDMessage("", 4, dumpString, this.slaveId), dumpContext)
      }

    }

    // todo finish dump promise at here. return a Future which wrap a binlog event channel.
  }

  def sendQuery(query: String): Future[QueryResult] = {
    this.validateIsReadyForQuery()
    val promise = Promise[QueryResult]()
    this.setQueryPromise(promise)
    this.connectionHandler.write(QueryMessage(query))
    addTimeout(promise, configuration.queryTimeout)
    promise.future
  }

  private def failQueryPromise(t: Throwable): Unit = {
    this.clearQueryPromise.foreach {
      _.tryFailure(t)
    }
  }

  private def succeedQueryPromise(queryResult: QueryResult): Unit = {
    this.clearQueryPromise.foreach {
      _.success(queryResult)
    }
  }

  def isQuerying: Boolean = this.queryPromise.isDefined

  def onResultSet(resultSet: ResultSet, message: EOFMessage): Unit = {
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

  private def validateIsReadyForQuery(): Unit = {
    if (isQuerying) {
      throw new ConnectionStillRunningQueryException(this.connectionCount, false)
    }
  }

  private def queryPromise: Option[Promise[QueryResult]] = queryPromiseReference.get()

  private def setQueryPromise(promise: Promise[QueryResult]): Unit = {
    if (!this.queryPromiseReference.compareAndSet(None, Some(promise)))
      throw new ConnectionStillRunningQueryException(this.connectionCount, true)
  }

  private def clearQueryPromise: Option[Promise[QueryResult]] = {
    this.queryPromiseReference.getAndSet(None)
  }

}
