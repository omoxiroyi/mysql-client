package com.fan.mysql.async.binlog.parse

import java.util

import com.fan.mysql.async.binlog.BinlogDumpContext
import com.fan.mysql.async.binlog.event.impl.TableMapEvent
import com.fan.mysql.async.binlog.event.{BinlogEvent, DefaultEvent, EventHeader}
import com.fan.mysql.async.util.ChannelWrapper._
import com.fan.mysql.async.util.Log.Logging
import com.fan.mysql.async.util.MySQLConstants
import io.netty.buffer.ByteBuf

class TableMapEventParser extends FilterableParser with Logging {

  override def parse(buffer: ByteBuf, header: EventHeader, context: BinlogDumpContext): BinlogEvent = {
    val event = new TableMapEvent(header)
    val eventPos = buffer.readerIndex()
    val postHeaderLen = context.getFormatDescription.getPostHeaderLen(header.eventType - 1)
    buffer.readerIndex(eventPos)
    var tableId = 0L
    if (postHeaderLen == 6) tableId = buffer.readUnsignedInt()
    else tableId = buffer.readUnsignedLong48()
    // flags = buffer.getUint16();
    /* Read the variable part of the event */
    buffer.readerIndex(eventPos + postHeaderLen)
    val dbname = buffer.readLengthASCIString()
    buffer.forward(1)
    /* termination null */
    val tblname = buffer.readLengthASCIString()
    buffer.forward(1)
    event.setTableId(tableId)
    event.setDbname(dbname)
    event.setTblname(tblname)
    // filter by table name
    if (filter != null && !filter.accepts(event))
      return new DefaultEvent(header)
    // Read column information from buffer
    val columnCnt = buffer.readBinaryLength.toInt
    val columnInfo = new Array[TableMapEvent.ColumnInfo](columnCnt)
    for (i <- 0 until columnCnt) {
      val info = new TableMapEvent.ColumnInfo
      info.`type` = buffer.readUnsignedByte()
      columnInfo(i) = info
    }
    var nullBits: util.BitSet = null
    if (buffer.readerIndex() < buffer.writerIndex()) {
      val fieldSize = buffer.readBinaryLength.toInt
      decodeFields(buffer, fieldSize, columnCnt, columnInfo)
      nullBits = buffer.readBitmap(columnCnt)
    }
    // set table map event

    event.setColumnCnt(columnCnt)
    event.setColumnInfo(columnInfo)
    event.setNullBits(nullBits)
    // put to context table map
    context.getTableMapEvents.put(tableId, event)
    event
  }

  private def decodeFields(buffer: ByteBuf, len: Int, columnCnt: Int, columnInfo: Array[TableMapEvent.ColumnInfo]): Unit = {
    val limit = buffer.writerIndex()
    buffer.writerIndex(len + buffer.readerIndex())

    for (i <- 0 until columnCnt) {
      val info = columnInfo(i)
      info.`type` match {
        case MySQLConstants.MYSQL_TYPE_TINY_BLOB =>
        case MySQLConstants.MYSQL_TYPE_BLOB =>
        case MySQLConstants.MYSQL_TYPE_MEDIUM_BLOB =>
        case MySQLConstants.MYSQL_TYPE_LONG_BLOB =>
        case MySQLConstants.MYSQL_TYPE_DOUBLE =>
        case MySQLConstants.MYSQL_TYPE_FLOAT =>
        case MySQLConstants.MYSQL_TYPE_GEOMETRY =>
        case MySQLConstants.MYSQL_TYPE_JSON =>
          /*
           * These types store a single byte.
           */
          info.meta = buffer.readUnsignedByte()

        case MySQLConstants.MYSQL_TYPE_SET =>
        case MySQLConstants.MYSQL_TYPE_ENUM =>
          /*
           * log_event.h : MYSQL_TYPE_SET & MYSQL_TYPE_ENUM : This enumeration value is
           * only used internally and cannot exist in a binlog.
           */
          logger.warn("This enumeration value is only used internally " + "and cannot exist in a binlog: type=" + info.`type`)

        case MySQLConstants.MYSQL_TYPE_STRING =>
          /*
           * log_event.h : The first byte is always MYSQL_TYPE_VAR_STRING (i.e., 253). The
           * second byte is the field size, i.e., the number of bytes in the
           * representation of size of the string: 3 or 4.
           */
          var x = buffer.readUnsignedByte() << 8 // real_type
          x += buffer.readUnsignedByte() // pack or field length
          info.meta = x

        case MySQLConstants.MYSQL_TYPE_BIT =>
          info.meta = buffer.readUnsignedShort()

        case MySQLConstants.MYSQL_TYPE_VARCHAR =>
          /*
           * These types store two bytes.
           */
          info.meta = buffer.readUnsignedShort()

        case MySQLConstants.MYSQL_TYPE_NEWDECIMAL =>
          var x = buffer.readUnsignedByte() << 8 // precision
          x += buffer.readUnsignedByte() // decimals

          info.meta = x

        case MySQLConstants.MYSQL_TYPE_TIME2 =>
        case MySQLConstants.MYSQL_TYPE_DATETIME2 =>
        case MySQLConstants.MYSQL_TYPE_TIMESTAMP2 =>
          info.meta = buffer.readUnsignedByte()

        case _ =>
          info.meta = 0

      }
    }
    buffer.writerIndex(limit)
  }

}
