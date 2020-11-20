package com.fan.mysql.driver

import java.io.IOException
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util
import java.util.{Timer, TimerTask}

import com.fan.mysql.binlog.{BinlogEventFilter, BinlogEventHandler}
import com.fan.mysql.dbsync.{BinlogContext, BinlogDecoder}
import com.fan.mysql.fetcher.ConnectionFetcher
import com.fan.mysql.packet.binlog.{BinlogDumpFilePacket, BinlogDumpGtidPacket, SemiSyncAckPacket}
import com.fan.mysql.packet.result.ResultSetPacket
import com.fan.mysql.packet.{CommandPacket, OkPacket}
import com.fan.mysql.util.{Logging, MySQLConstants, PacketManager}
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils

import scala.util.control.ControlThrowable

class MySQLConnection(address: InetSocketAddress, username: String, password: String)
    extends Logging {

  def this(host: String, port: Int, username: String, password: String) =
    this(new InetSocketAddress(host, port), username, password)

  private[this] val connector: MysqlConnector = new MysqlConnector(address, username, password)

  private[this] val heart_beat_interval = 10 * 1000000000L

  private[this] var slaveId               = 0L
  private[this] var binlogFormat          = BinlogFormat.ROW
  private[this] var semiSync              = false
  private[this] var heartBeat             = false
  private[this] var heartBeatTimer: Timer = _
  private[this] var timeZone              = "GMT-0:00"

  @throws[IOException]
  def connect(): Unit = {
    connector.connect()
  }

  @throws[IOException]
  def reconnect(): Unit = {
    connector.reconnect()
  }

  @throws[IOException]
  def disconnect(): Unit = {
    connector.disconnect()
  }

  def isConnected: Boolean = connector.isConnected

  @throws[IOException]
  def query(cmd: String): ResultSetPacket = {
    val executor = new MysqlQueryExecutor(connector)
    executor.query(cmd)
  }

  @throws[IOException]
  def query(cmd: String, queryHandler: MysqlConnQueryHandler): Unit = {
    val executor = new MysqlQueryExecutor(connector)
    executor.query(cmd, queryHandler)
  }

  @throws[IOException]
  def update(cmd: String): OkPacket = {
    val executor = new MysqlUpdateExecutor(connector)
    executor.update(cmd)
  }

  private class BreakControlAgain extends ControlThrowable

  @throws[IOException]
  def dump(dumpString: String, filter: BinlogEventFilter, handler: BinlogEventHandler): Unit = {
    updateSettings()
    val binlogChecksum = loadBinlogChecksum
    val format         = getBinlogFormat
    if (!format.isRow) logger.warn("binlog format is :" + format + ", row format is needed!")
    sendBinlogDump(dumpString)
    val decoder = new BinlogDecoder
    decoder.setFilter(filter)
    val context = new BinlogContext(binlogChecksum)
    context.setSemiSync(semiSync)
    context.setTimeZone(timeZone)
    // start fetcher
    val fetcher = new ConnectionFetcher(connector.getChannel)
    if (heartBeat) scheduleHeartBeat(fetcher)

    val myBreak = new BreakControlAgain

    try {
      import scala.util.control.Breaks._
      while (true) {
        breakable {
          fetcher.fetch()
          val event = decoder.decode(fetcher, context)
          if (event == null)
            break
          if (context.isNeedReply) { // send ack
            val packet = new SemiSyncAckPacket(context.getBinlogFileName, context.getBinlogPosition)
            PacketManager.write(
              connector.getChannel,
              Array[ByteBuffer](ByteBuffer.wrap(packet.toByteBuffer(connector.getCharset).array))
            )
            // reset to false
            context.setNeedReply(false)
          }
          // loss event when MYSQL and software crash here at the same time
          if (!handler.handle(event))
            throw new BreakControlAgain
        }
      }
    } catch {
      case ex: BreakControlAgain if ex eq myBreak =>
    } finally {
      // stop heart beat
      if (heartBeat && heartBeatTimer != null) heartBeatTimer.cancel()
    }
  }

  def quit(): Unit = connector.quit()

  @throws[IOException]
  private def loadBinlogChecksum: Int = {
    var binlogChecksum: Int = 0
    var rs: ResultSetPacket = null
    rs = query("select @master_binlog_checksum")
    val columnValues: util.List[String] = rs.getRows.get(0)
    if (columnValues.get(0).toUpperCase == "CRC32")
      binlogChecksum = MySQLConstants.BINLOG_CHECKSUM_ALG_CRC32
    else binlogChecksum = MySQLConstants.BINLOG_CHECKSUM_ALG_OFF
    binlogChecksum
  }

  @throws[IOException]
  private def sendBinlogDump(dumpString: String): Unit = {
    logger.info(s"COM_BINLOG_DUMP with position: $dumpString")
    var cmd: CommandPacket = null
    if (StringUtils.isEmpty(dumpString)) cmd = new BinlogDumpFilePacket("", 4, this.slaveId)
    else if (dumpString.contains(".")) {
      val splits = dumpString.split(":")
      cmd = new BinlogDumpFilePacket(splits(0), splits(1).toLong, this.slaveId)
    } else cmd = new BinlogDumpGtidPacket("", 4, dumpString, this.slaveId)
    cmd.packetId = 0
    PacketManager.write(
      connector.getChannel,
      Array[ByteBuffer](ByteBuffer.wrap(cmd.toByteBuffer(connector.getCharset).array))
    )
    connector.dumping = true
  }

  private def scheduleHeartBeat(fetcher: ConnectionFetcher): Unit = {
    heartBeatTimer = new Timer
    heartBeatTimer.schedule(
      new TimerTask() {
        override def run(): Unit = {
          val lastReadTime = fetcher.getLastReadTime
          if (lastReadTime < 0) {
            logger.debug("Heartbeat check, not fetching")
            return
          }
          val currentTime = System.currentTimeMillis
          val nextDelay   = currentTime - lastReadTime
          logger.debug(
            s"Heartbeat check, current time: $currentTime, last read: $lastReadTime, delay time: $nextDelay ms"
          )
          try if (nextDelay >= 2 * heart_beat_interval / 1000000) {
            logger.error("connection read timeout!")
            disconnect()
          } catch {
            case e: IOException =>
              logger.error("heartbeat error", e)
          }
        }
      },
      heart_beat_interval / 1000000,
      heart_beat_interval / 1000000
    )
  }

  def getBinlogFormat: BinlogFormat = {
    if (binlogFormat == null) this synchronized loadBinlogFormat()
    binlogFormat
  }

  /** 判断一下是否采用ROW模式
    */
  private def loadBinlogFormat(): Unit = {
    var rs: ResultSetPacket = null
    try rs = query("show variables like 'binlog_format'")
    catch {
      case e: IOException =>
        logger.error("load binlog format error", e)
    }
    val columnValues = rs.getRows.get(0)
    if (columnValues == null || columnValues.size != 2) {
      logger.warn(
        "unexpected binlog format query result, this may cause unexpected result, so throw exception to request network to io shutdown."
      )
      throw new IllegalStateException("unexpected binlog format query result:" + rs.getRows)
    }
    binlogFormat = BinlogFormat.valuesOf(columnValues.get(1))
    if (binlogFormat == null)
      throw new IllegalStateException("unexpected binlog format query result:" + rs.getRows)
  }

  /** the settings that will need to be checked or set:<br>
    * <ol>
    * <li>wait_timeout</li>
    * <li>net_write_timeout</li>
    * <li>net_read_timeout</li>
    * </ol>
    */
  @throws[IOException]
  private def updateSettings(): Unit = {
    try update("set wait_timeout=9999999")
    catch {
      case e: Exception =>
        logger.warn(ExceptionUtils.getStackTrace(e))
    }
    try update("set net_write_timeout=1800")
    catch {
      case e: Exception =>
        logger.warn(ExceptionUtils.getStackTrace(e))
    }
    try update("set net_read_timeout=1800")
    catch {
      case e: Exception =>
        logger.warn(ExceptionUtils.getStackTrace(e))
    }
    try // 设置服务端返回结果时不做编码转化，直接按照数据库的二进制编码进行发送，由客户端自己根据需求进行编码转化
    update("set names 'binary'")
    catch {
      case e: Exception =>
        logger.warn(ExceptionUtils.getStackTrace(e))
    }
    try // mysql5.6针对checksum支持需要设置session变量
    // 如果不设置会出现错误： Slave can not handle replication events with the
    // checksum that master is configured to log
    // 但也不能乱设置，需要和mysql server的checksum配置一致，不然RotateLogEvent会出现乱码
    update("set @master_binlog_checksum= @@global.binlog_checksum")
    catch {
      case e: Exception =>
        logger.warn(ExceptionUtils.getStackTrace(e))
    }
    // try {
    // // mariadb针对特殊的类型，需要设置session变量
    // update("SET @mariadb_slave_capability='" +
    // MysqlConstant.MARIA_SLAVE_CAPABILITY_GTID + "'");
    // } catch (Exception e) {
    // logger.warn(ExceptionUtils.getStackTrace(e));
    // }
  }

}
