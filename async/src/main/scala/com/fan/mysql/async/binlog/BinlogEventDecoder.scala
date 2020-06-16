package com.fan.mysql.async.binlog

import com.fan.mysql.async.binlog.event.{BinlogEvent, EventHeader}
import com.fan.mysql.async.binlog.parse.{FormatDescriptionEventParser, GtidEventParser, PreviousGtidsParser, QueryEventParser, RotateEventParser, RowDataEventParser, TableMapEventParser, XidEventParser}
import com.fan.mysql.async.decoder.MessageDecoder
import com.fan.mysql.async.exceptions.BinlogParseException
import com.fan.mysql.async.message.server.ServerMessage
import com.fan.mysql.async.util.ChannelWrapper._
import com.fan.mysql.async.util.MySQLConstants
import io.netty.buffer.ByteBuf

object BinlogEventDecoder {
  final val NET_HEADER_SIZE = 4
  final val LOG_EVENT_HEADER_LEN = 19
}

class BinlogEventDecoder(context: BinlogDumpContext) extends MessageDecoder {

  private val parsers = new Array[BinlogEventParser](128)

  private[this] var filter: BinlogEventFilter = _

  import BinlogEventDecoder._

  override def decode(buffer: ByteBuf): ServerMessage = {

    var event: BinlogEvent = null

    parseSemiSyncHeader(buffer)

    val bufferRemain = buffer.readableBytes()

    if (bufferRemain >= LOG_EVENT_HEADER_LEN) {
      // parse header
      val header = parseHeader(buffer)
      context.setBinlogPosition(header.nextPosition)

      if (bufferRemain >= header.eventLength) {
        // parse body

      }
    }

    event
  }

  private def parseSemiSyncHeader(buffer: ByteBuf): Unit = {
    if (!context.isSemiSync)
      return
    val magicNum = buffer.readByte()
    val needReply = buffer.readByte()

    if (magicNum != SemiSyncAckPacket.magicNum)
      throw new BinlogParseException("Magic number of semi-sync header is wrong!")

    context.setNeedReply(needReply == 1)
  }

  private def parseHeader(buffer: ByteBuf): EventHeader = {

    val headerPos = buffer.readerIndex()

    val timestamp = buffer.readUnsignedInt()
    val eventType = buffer.readUnsignedByte()
    val serverId = buffer.readUnsignedInt()
    val eventLength = buffer.readUnsignedInt()
    val nextPosition = buffer.readUnsignedInt()
    val flags = buffer.readUnsignedShort()

    var checksumAlg: Int = 0

    if (eventType == MySQLConstants.FORMAT_DESCRIPTION_EVENT) {
      buffer.readerIndex(headerPos + LOG_EVENT_HEADER_LEN + FormatDescriptionEventParser.EVENT_HEADER_LEN_OFFSET)
      val commonHeaderLen = buffer.readUnsignedByte()
      buffer.readerIndex(headerPos + commonHeaderLen + FormatDescriptionEventParser.SERVER_VER_OFFSET)
      val serverVersion = buffer.readFixedASCIString(FormatDescriptionEventParser.SERVER_VER_LEN)
      val versionSplit = Array[Int](0, 0, 0)
      FormatDescriptionEventParser.doServerVersionSplit(serverVersion, versionSplit)
      checksumAlg = MySQLConstants.BINLOG_CHECKSUM_ALG_UNDEF

      // we don't handle START_EVENT_V3 start event here.
      if (FormatDescriptionEventParser.versionProduct(versionSplit) >= FormatDescriptionEventParser.checksumVersionProduct) {
        buffer.readerIndex(headerPos + (eventLength - MySQLConstants.BINLOG_CHECKSUM_LEN - MySQLConstants.BINLOG_CHECKSUM_ALG_DESC_LEN).toInt)
        checksumAlg = buffer.readUnsignedByte().asInstanceOf[Int]
      } else {
        checksumAlg = context.getFormatDescription.getEventHeader.checksumAlg
      }

      buffer.readerIndex(headerPos + LOG_EVENT_HEADER_LEN)

      if (checksumAlg != MySQLConstants.BINLOG_CHECKSUM_ALG_UNDEF && (eventType == MySQLConstants.FORMAT_DESCRIPTION_EVENT || checksumAlg != MySQLConstants.BINLOG_CHECKSUM_ALG_OFF)) {
        buffer.writerIndex(buffer.writerIndex() - MySQLConstants.BINLOG_CHECKSUM_LEN)
      }
    }

    EventHeader(
      timestamp,
      eventType,
      serverId,
      eventLength,
      nextPosition,
      flags,
      context.getBinlogFileName,
      System.currentTimeMillis(),
      checksumAlg
    )
  }

  private def getEventParser(`type`: Int): BinlogEventParser = {
    var parser = parsers(`type`)
    if (parser != null)
      return parser

    `type` match {
      case MySQLConstants.FORMAT_DESCRIPTION_EVENT =>
        parser = new FormatDescriptionEventParser


      case MySQLConstants.ROTATE_EVENT =>
        parser = new RotateEventParser


      case MySQLConstants.QUERY_EVENT =>
        parser = new QueryEventParser


      case MySQLConstants.TABLE_MAP_EVENT =>
        parser = new TableMapEventParser
        val tableMapParser = parser.asInstanceOf[TableMapEventParser]
        tableMapParser.setFilter(filter)


      case MySQLConstants.DELETE_ROWS_EVENT =>
      case MySQLConstants.DELETE_ROWS_EVENT_V1 =>
      case MySQLConstants.UPDATE_ROWS_EVENT =>
      case MySQLConstants.UPDATE_ROWS_EVENT_V1 =>
      case MySQLConstants.WRITE_ROWS_EVENT =>
      case MySQLConstants.WRITE_ROWS_EVENT_V1 =>
        parser = new RowDataEventParser


      case MySQLConstants.XID_EVENT =>
        parser = new XidEventParser


      case MySQLConstants.GTID_LOG_EVENT =>
        parser = new GtidEventParser


      case MySQLConstants.PREVIOUS_GTIDS_LOG_EVENT =>
        parser = new PreviousGtidsParser


      case _ =>
        return null
    }
    parsers(`type`) = parser
    parser
  }
}
