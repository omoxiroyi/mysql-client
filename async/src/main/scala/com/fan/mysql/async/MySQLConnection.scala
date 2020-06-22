package com.fan.mysql.async

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import java.util.concurrent.{Executor, ScheduledFuture}

import com.fan.mysql.async.binlog.event.BinlogEvent
import com.fan.mysql.async.binlog.{BinlogDumpContext, BinlogEventFilter, BinlogEventHandler}
import com.fan.mysql.async.codec.{MySQLConnectionHandler, MySQLHandlerDelegate}
import com.fan.mysql.async.db._
import com.fan.mysql.async.exceptions._
import com.fan.mysql.async.message.client._
import com.fan.mysql.async.message.server._
import com.fan.mysql.async.pool.TimeoutScheduler
import com.fan.mysql.async.util.ChannelFutureTransformer._
import com.fan.mysql.async.util._
import io.netty.channel.{ChannelHandlerContext, EventLoopGroup}
import org.apache.commons.lang3.StringUtils

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Random, Success}

object MySQLConnection {
  final val Counter: AtomicLong = new AtomicLong()
}

class MySQLConnection(
                       configuration: Configuration,
                       charsetMapper: CharsetMapper = CharsetMapper.Instance,
                       group: EventLoopGroup = NettyUtils.DefaultEventLoopGroup,
                       implicit val executionContext: ExecutionContext = ExecutorServiceUtils.CachedExecutionContext
                     ) extends MySQLHandlerDelegate with Connection with TimeoutScheduler {

  // Java constructor
  def this(configuration: Configuration) = this(configuration, CharsetMapper.Instance)

  // validate that this charset is supported
  charsetMapper.toInt(configuration.charset)

  private[this] final val connectionCount = MySQLConnection.Counter.incrementAndGet()
  private[this] final val connectionId = s"[mysql-connection-$connectionCount]"
  private[this] final val log = Log.getByName(connectionId)

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

  // dump binlog variables
  private[this] val slaveId = math.abs(Random.nextLong(99999999)) + 1
  private[this] val timeZone = "GMT-0:00"
  private[this] var enableHeartBeat = true
  private[this] val heartbeatInterval = 10
  private[this] var heartBeatScheduler: ScheduledFuture[_] = _
  @volatile private[this] var lastEventReadTime = -1L
  private[this] var eventHandler: BinlogEventHandler = _
  private[this] var eventFilter: BinlogEventFilter = _
  private[this] var eventHandlerExecutor: Executor = _

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

  // todo this close method should be rewrite.
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
    } else if (!this.disconnectionPromise.isCompleted) {
      this.disconnectionPromise.tryComplete(Success(this))
    }

    this.disconnectionPromise.future
  }

  override def onDisconnect(ctx: ChannelHandlerContext): Unit = {
    log.debug("Disconnect from {}", ctx.channel.remoteAddress)
    this.connected = false
    if (this.heartBeatScheduler != null) {
      log.debug("Stop dump binlog master heart beat schedule")
      this.heartBeatScheduler.cancel(true)
      this.heartBeatScheduler = null
    }
  }

  override def onConnect(ctx: ChannelHandlerContext): Unit = {
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

  override def onEvent(event: BinlogEvent): Unit = {
    this.lastEventReadTime = System.currentTimeMillis()

    log.trace(s"Receive a binlog event.\n$event\n")

    if (this.eventFilter != null && !this.eventFilter.accepts(event)) return

    this.eventHandlerExecutor.execute(() => eventHandler.handle(event))
  }

  def dump(dumpString: String, filter: BinlogEventFilter, handler: BinlogEventHandler,
           executor: Executor): Future[Connection] = {
    // todo check is dumping state as well
    this.validateIsReadyForQuery()

    require(handler != null, "You must give a event handler to consume event")
    require(executor != null, "You have to provide a ExecutionContext to handle event," +
      " because we can not do any block operation in netty worker thread pool.")

    this.eventHandler = handler
    this.eventFilter = filter
    this.eventHandlerExecutor = executor

    log.info(s"Start dump binlog with position: $dumpString")

    // init common variables for dump session
    (for {
      _ <- this.sendQuery("set wait_timeout=9999999")
      _ <- this.sendQuery("set net_write_timeout=1800")
      _ <- this.sendQuery("set net_read_timeout=1800")
      _ <- this.sendQuery("set names 'binary'")
      _ <- this.sendQuery("set @master_binlog_checksum= @@global.binlog_checksum")
      _ <- this.sendQuery(s"set @master_heartbeat_period=${heartbeatInterval * 1000000000L}")
      checksumType <- this.sendQuery("select @master_binlog_checksum")
    } yield {

      val dumpContext = checksumType.rows.map(_.apply(0).apply(0)) match {
        case Some(a: String) if a == "CRC32" =>
          log.debug(s"Dump binlog checksum type is CRC32")
          new BinlogDumpContext(MySQLConstants.BINLOG_CHECKSUM_ALG_CRC32)
        case _ => new BinlogDumpContext(MySQLConstants.BINLOG_CHECKSUM_ALG_OFF)
      }

      // init dump context here
      dumpContext.setTimeZone(timeZone)

      dumpContext
    }).flatMap { dumpContext =>
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
    }.map { _ =>
      // start heart beat check schedule finally
      this.lastEventReadTime = System.currentTimeMillis() // mark to remove from event loop
      this.heartBeatScheduler = scheduleAtFixedRate(checkHeartBeat(), heartbeatInterval.second)
      this
    }
  }

  def checkHeartBeat(): Unit = {
    val lastReadTime = this.lastEventReadTime

    if (lastReadTime < 0) {
      log.debug("Dump heartbeat check, not start")
      return
    }

    val currentTime = System.currentTimeMillis()

    val nextDelay = currentTime - lastReadTime

    log.trace(s"Dump heartbeat check, current time: $currentTime, last read: $lastReadTime, delay time: $nextDelay ms")

    if (nextDelay >= 2 * heartbeatInterval * 1000) {
      log.error("Dump binlog check heartbeat timeout, connection will be close")
      this.close.onComplete {
        case Success(_) => log.info("Close timed out connection successfully")
        case Failure(e) => log.error("Close timed out failed", e)
      }
    }
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
