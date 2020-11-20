package com.fan.mysql.async.binlog.parse

import com.fan.mysql.async.binlog.event.impl.FormatDescriptionEvent
import com.fan.mysql.async.binlog.event.{BinlogEvent, EventHeader}
import com.fan.mysql.async.binlog.{BinlogDumpContext, BinlogEventParser}
import com.fan.mysql.async.util.ChannelWrapper._
import com.fan.mysql.async.util.Log.Logging
import io.netty.buffer.ByteBuf

object FormatDescriptionEventParser {
  final val LOG_EVENT_HEADER_LEN = 19

  final val BINLOG_VER_OFFSET = 0
  final val SERVER_VER_OFFSET = 2
  final val SERVER_VER_LEN = 50
  final val CREATE_TIMESTAMP_OFFSET: Int = SERVER_VER_OFFSET + SERVER_VER_LEN
  final val CREATE_TIMESTAMP_LEN = 4
  final val EVENT_HEADER_LEN_OFFSET: Int = CREATE_TIMESTAMP_OFFSET + CREATE_TIMESTAMP_LEN

  final val checksumVersionSplit: Array[Int] = Array(5, 6, 1)
  final val checksumVersionProduct
    : Long = (checksumVersionSplit(0) * 256 + checksumVersionSplit(1)) * 256 + checksumVersionSplit(
    2)

  def doServerVersionSplit(serverVersion: String, versionSplit: Array[Int]): Unit = {
    val split = serverVersion.split("\\.")
    if (split.length < 3) {
      versionSplit(0) = 0
      versionSplit(1) = 0
      versionSplit(2) = 0
    } else {
      var j = 0
      for (i <- 0 to 2) {
        val str = split(i)
        j = 0
        import scala.util.control.Breaks._
        breakable {
          while (j < str.length) {
            if (!Character.isDigit(str.charAt(j)))
              break
            j += 1
          }
        }
        if (j > 0) versionSplit(i) = Integer.valueOf(str.substring(0, j), 10)
        else {
          versionSplit(0) = 0
          versionSplit(1) = 0
          versionSplit(2) = 0
        }
      }
    }
  }

  def versionProduct(versionSplit: Array[Int]): Long =
    (versionSplit(0) * 256 + versionSplit(1)) * 256 + versionSplit(2)
}

class FormatDescriptionEventParser extends BinlogEventParser with Logging {

  import FormatDescriptionEventParser._

  override def parse(buffer: ByteBuf,
                     header: EventHeader,
                     context: BinlogDumpContext): BinlogEvent = {
    val event = new FormatDescriptionEvent(header)

    val eventPos = buffer.readerIndex()

    // read version
    val binlogVersion = buffer.readUnsignedShort()
    val serverVersion = buffer.readFixedASCIString(SERVER_VER_LEN)

    // read create time
    buffer.readerIndex(eventPos - LOG_EVENT_HEADER_LEN + CREATE_TIMESTAMP_OFFSET)
    val createTime = buffer.readUnsignedInt()

    // read event header length
    buffer.readerIndex(eventPos + EVENT_HEADER_LEN_OFFSET)
    val eventHeaderLen = buffer.readUnsignedByte()
    if (eventHeaderLen < LOG_EVENT_HEADER_LEN)
      logger.error(s"common header length must >= $LOG_EVENT_HEADER_LEN")

    val numberOfEventTypes = buffer.writerIndex() - (eventPos + EVENT_HEADER_LEN_OFFSET + 1)

    val postHeaderLen = new Array[Short](numberOfEventTypes)

    (0 until numberOfEventTypes).foreach(i => postHeaderLen(i) = buffer.readUnsignedByte())

    event.setBinlogVersion(binlogVersion)
    event.setServerVersion(serverVersion)
    event.setCreateTime(createTime)
    event.setCommonHeaderLen(eventHeaderLen)
    event.setPostHeaderLen(postHeaderLen)
    context.setFormatDescription(event)

    event
  }
}
