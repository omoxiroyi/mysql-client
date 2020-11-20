package com.fan.mysql.async.binlog.parse

import java.sql.Timestamp
import java.util
import java.util.{Calendar, TimeZone}

import com.fan.mysql.async.binlog.event.impl.{RowData, RowDataEvent, TableMapEvent}
import com.fan.mysql.async.binlog.event.{BinlogEvent, DefaultEvent, EventColumn, EventHeader}
import com.fan.mysql.async.binlog.{BinlogDumpContext, BinlogEventParser}
import com.fan.mysql.async.util.ChannelWrapper._
import com.fan.mysql.async.util.Log.Logging
import com.fan.mysql.async.util.{JsonConversion, LogBuffer, MySQLConstants}
import io.netty.buffer.ByteBuf

object RowDataEventParser {
  final val RW_V_EXTRAINFO_TAG = 0
  final val EXTRA_ROW_INFO_LEN_OFFSET = 0
  final val EXTRA_ROW_INFO_FORMAT_OFFSET = 1
  final val EXTRA_ROW_INFO_HDR_BYTES = 2
  final val EXTRA_ROW_INFO_MAX_PAYLOAD: Int = 255 - EXTRA_ROW_INFO_HDR_BYTES

  final val DATETIMEF_INT_OFS = 0x8000000000L
  final val TIMEF_INT_OFS = 0x800000L
  final val TIMEF_OFS = 0x800000000000L

  final val STMT_END_F = 1
  final val NO_FOREIGN_KEY_CHECKS_F: Int = 1 << 1
  final val RELAXED_UNIQUE_CHECKS_F: Int = 1 << 2
  final val COMPLETE_ROWS_F: Int = 1 << 3
}

class RowDataEventParser extends BinlogEventParser with Logging {

  import RowDataEventParser._

  private[this] var columnLen = 0
  private[this] var nullBits: util.BitSet = _
  private[this] var nullBitIndex = 0

  override def parse(nettyBuffer: ByteBuf,
                     header: EventHeader,
                     context: BinlogDumpContext): BinlogEvent = {

    val buffer = new LogBuffer(nettyBuffer.slice().toArray())

    val eventPos = buffer.position
    // read row event header
    val postHeaderLen = context.getFormatDescription.getPostHeaderLen(header.eventType - 1)
    buffer.position(eventPos)
    // read table id
    var tableId = 0L
    if (postHeaderLen == 6) tableId = buffer.getUint32
    else tableId = buffer.getUlong48
    // filter by table name
    val tableMap = context.getTableMapEvents.get(tableId)
    if (tableMap == null) return new DefaultEvent(header)
    val event = new RowDataEvent(header, tableId)
    // read flag
    val flags = buffer.getUint16
    event.setFlags(flags)
    // read extra data
    var headerLen = 0
    if (postHeaderLen == 10) {
      headerLen = buffer.getUint16
      headerLen -= 2
      val start = buffer.position
      val end = start + headerLen
      var i = start
      while ({
        i < end
      }) buffer.getUint8({
        i += 1
        i - 1
      }) match {
        case RW_V_EXTRAINFO_TAG =>
          buffer.position(i + EXTRA_ROW_INFO_LEN_OFFSET)
          val checkLen = buffer.getUint8
          val `val` = checkLen - EXTRA_ROW_INFO_HDR_BYTES
          assert(buffer.getUint8 == `val`)
          for (_ <- 0 until `val`) {
            assert(buffer.getUint8 == `val`)
          }

        case _ =>
          i = end

      }
    }
    // read row event body
    buffer.position(eventPos + postHeaderLen + headerLen)
    columnLen = buffer.getPackedLong.toInt
    val columns = buffer.getBitmap(columnLen)
    var changedColumns: util.BitSet = null
    if (header.eventType == MySQLConstants.UPDATE_ROWS_EVENT_V1
        || header.eventType == MySQLConstants.UPDATE_ROWS_EVENT)
      changedColumns = buffer.getBitmap(columnLen)
    nullBits = new util.BitSet(columnLen)
    // parse and convert to row event
    val timeZone = context.getTimeZone

    import scala.util.control.Breaks._

    breakable {
      while (nextOneRow(buffer, columns)) {
        val row = new RowData
        if (header.eventType == MySQLConstants.WRITE_ROWS_EVENT || header.eventType == MySQLConstants.WRITE_ROWS_EVENT_V1)
          parseRow(buffer, row, tableMap, columns, isAfter = true, timeZone)
        else if (header.eventType == MySQLConstants.DELETE_ROWS_EVENT || header.eventType == MySQLConstants.DELETE_ROWS_EVENT_V1)
          parseRow(buffer, row, tableMap, columns, isAfter = false, timeZone)
        else if (header.eventType == MySQLConstants.UPDATE_ROWS_EVENT || header.eventType == MySQLConstants.UPDATE_ROWS_EVENT_V1) {
          parseRow(buffer, row, tableMap, columns, isAfter = false, timeZone)
          if (!nextOneRow(buffer, changedColumns)) break
          parseRow(buffer, row, tableMap, changedColumns, isAfter = true, timeZone)
        }
        event.getRows.add(row)
      }
    }

    // end of statement check:
    if ((flags & STMT_END_F) != 0) context.getTableMapEvents.clear()
    event
  }

  private def parseRow(buffer: LogBuffer,
                       row: RowData,
                       tableMap: TableMapEvent,
                       colBit: util.BitSet,
                       isAfter: Boolean,
                       timeZone: String): Unit = {
    var currentColumn = 0
    var columnCount = 0
    // calculate real column count
    for (i <- 0 until columnLen) {
      if (colBit.get(i)) {
        columnCount += 1
      }
    }
    val rowData = new Array[EventColumn](columnCount)

    import scala.util.control.Breaks._

    // parse row value
    for (i <- 0 until columnLen) {
      breakable {
        if (!colBit.get(i)) break()
        val cInfo = tableMap.getColumnInfo(i)
        val column = new EventColumn
        if (nullBits.get({
              nullBitIndex += 1
              nullBitIndex - 1
            })) column.setNull(true)
        else {
          val value = fetchValue(buffer, cInfo.`type`, cInfo.meta, timeZone)
          column.setColumnValue(value)
        }
        rowData({
          currentColumn += 1
          currentColumn - 1
        }) = column
      }
    }
    // set event keys and columns
    if (isAfter) {
      row.setAfterColumns(rowData)
      row.setAfterBit(colBit)
    } else {
      row.setBeforeColumns(rowData)
      row.setBeforeBit(colBit)
    }
  }

  private def fetchValue(buffer: LogBuffer, `type`: Int, meta: Int, timeZone: String) = {
    var value: Serializable = null
    var len = 0
    var dataType = `type`
    if (dataType == MySQLConstants.MYSQL_TYPE_STRING) if (meta >= 256) {
      val byte0 = meta >> 8
      val byte1 = meta & 0xff
      if ((byte0 & 0x30) != 0x30) {
        /* a long CHAR() field: see #37426 */
        len = byte1 | (((byte0 & 0x30) ^ 0x30) << 4)
        dataType = byte0 | 0x30
      } else
        byte0 match {
          case MySQLConstants.MYSQL_TYPE_SET  =>
          case MySQLConstants.MYSQL_TYPE_ENUM =>
          case MySQLConstants.MYSQL_TYPE_STRING =>
            dataType = byte0
            len = byte1

          case _ =>
            throw new IllegalArgumentException(
              String.format("!! Don't know how to handle column type=%d meta=%d (%04X)",
                            dataType,
                            meta,
                            meta))
        }
    } else len = meta
    dataType match {
      case MySQLConstants.MYSQL_TYPE_TINY =>
        val num = new Array[Byte](1)
        buffer.fillBytes(num, 0, 1)
        value = num

      case MySQLConstants.MYSQL_TYPE_SHORT =>
        val num = new Array[Byte](2)
        buffer.fillBytes(num, 0, 2)
        value = num

      case MySQLConstants.MYSQL_TYPE_INT24 =>
        val num = new Array[Byte](3)
        buffer.fillBytes(num, 0, 3)
        value = num

      case MySQLConstants.MYSQL_TYPE_LONG =>
        val num = new Array[Byte](4)
        buffer.fillBytes(num, 0, 4)
        value = num

      case MySQLConstants.MYSQL_TYPE_LONGLONG =>
        val num = new Array[Byte](8)
        buffer.fillBytes(num, 0, 8)
        value = num

      case MySQLConstants.MYSQL_TYPE_DECIMAL =>
        logger.warn(
          "MYSQL_TYPE_DECIMAL : This enumeration value is " + "only used internally and cannot exist in a binlog!")
        value = null /* unknown format */

      case MySQLConstants.MYSQL_TYPE_NEWDECIMAL =>
        val precision = meta >> 8
        val decimals = meta & 0xff
        val number = buffer.getDecimal(precision, decimals)
        value = number.toPlainString

      case MySQLConstants.MYSQL_TYPE_FLOAT =>
        value = buffer.getFloat32

      case MySQLConstants.MYSQL_TYPE_DOUBLE =>
        value = buffer.getDouble64

      case MySQLConstants.MYSQL_TYPE_BIT =>
        /* Meta-data: bit_len, bytes_in_rec, 2 bytes */
        val nbits = ((meta >> 8) * 8) + (meta & 0xff)
        len = (nbits + 7) / 8
        if (nbits <= 1) len = 1
        val bit = new Array[Byte](len)
        buffer.fillBytes(bit, 0, len)
        value = bit

      case MySQLConstants.MYSQL_TYPE_TIMESTAMP =>
        val i32 = buffer.getUint32
        if (i32 == 0) value = "0000-00-00 00:00:00"
        else {
          val v = new Timestamp(i32 * 1000).toString
          value = v.substring(0, v.length - 2)
        }

      case MySQLConstants.MYSQL_TYPE_TIMESTAMP2 =>
        val tv_sec = buffer.getBeUint32
        var tv_usec = 0
        meta match {
          case 0 =>
            tv_usec = 0

          case 1 =>
          case 2 =>
            tv_usec = buffer.getInt8 * 10000

          case 3 =>
          case 4 =>
            tv_usec = buffer.getBeInt16 * 100

          case 5 =>
          case 6 =>
            tv_usec = buffer.getBeInt24

          case _ =>
            tv_usec = 0

        }
        var second: String = null
        if (tv_sec == 0) second = "0000-00-00 00:00:00"
        else {
          val cal = Calendar.getInstance(TimeZone.getTimeZone(timeZone))
          cal.setTimeInMillis(tv_sec * 1000)
          second = calendarToStr(cal)
        }
        if (meta >= 1) {
          var microSecond = usecondsToStr(tv_usec, meta)
          microSecond = microSecond.substring(0, meta)
          value = second + '.' + microSecond
        } else value = second

      case MySQLConstants.MYSQL_TYPE_DATETIME =>
        val i64 = buffer.getLong64
        if (i64 == 0) value = "0000-00-00 00:00:00"
        else {
          val d = (i64 / 1000000).toInt
          val t = (i64 % 1000000).toInt
          value = String.format("%04d-%02d-%02d %02d:%02d:%02d",
                                d / 10000,
                                (d % 10000) / 100,
                                d % 100,
                                t / 10000,
                                (t % 10000) / 100,
                                t % 100)
        }

      case MySQLConstants.MYSQL_TYPE_DATETIME2 =>
        val intpart = buffer.getBeUlong40 - DATETIMEF_INT_OFS
        var frac = 0
        meta match {
          case 0 =>
            frac = 0

          case 1 =>
          case 2 =>
            frac = buffer.getInt8 * 10000

          case 3 =>
          case 4 =>
            frac = buffer.getBeInt16 * 100

          case 5 =>
          case 6 =>
            frac = buffer.getBeInt24

          case _ =>
            frac = 0

        }
        var second: String = null
        if (intpart == 0) second = "0000-00-00 00:00:00"
        else { // 构造TimeStamp只处理到秒
          val ymd = intpart >> 17
          val ym = ymd >> 5
          val hms = intpart % (1 << 17)
          second = String.format("%04d-%02d-%02d %02d:%02d:%02d",
                                 (ym / 13).toInt,
                                 (ym % 13).toInt,
                                 (ymd % (1 << 5)).toInt,
                                 (hms >> 12).toInt,
                                 ((hms >> 6) % (1 << 6)).toInt,
                                 (hms % (1 << 6)).toInt)
        }
        if (meta >= 1) {
          var microSecond = usecondsToStr(frac, meta)
          microSecond = microSecond.substring(0, meta)
          value = second + '.' + microSecond
        } else value = second

      case MySQLConstants.MYSQL_TYPE_TIME =>
        val i32 = buffer.getInt24
        val u32 = Math.abs(i32)
        if (i32 == 0) value = "00:00:00"
        else
          value = String.format("%s%02d:%02d:%02d",
                                if (i32 >= 0) ""
                                else "-",
                                u32 / 10000,
                                (u32 % 10000) / 100,
                                u32 % 100)

      case MySQLConstants.MYSQL_TYPE_TIME2 =>
        var intpart = 0L
        var frac = 0
        var ltime = 0L
        meta match {
          case 0 =>
            intpart = buffer.getBeUint24 - TIMEF_INT_OFS
            ltime = intpart << 24

          case 1 =>
          case 2 =>
            intpart = buffer.getBeUint24 - TIMEF_INT_OFS
            frac = buffer.getUint8
            if (intpart < 0 && frac > 0) {
              intpart += 1
              frac -= 0x100
            }
            frac = frac * 10000
            ltime = intpart << 24

          case 3 =>
          case 4 =>
            intpart = buffer.getBeUint24 - TIMEF_INT_OFS
            frac = buffer.getBeUint16
            if (intpart < 0 && frac > 0) {
              intpart += 1
              frac -= 0x10000
            }
            frac = frac * 100
            ltime = intpart << 24

          case 5 =>
          case 6 =>
            intpart = buffer.getBeUlong48 - TIMEF_OFS
            ltime = intpart
            frac = (intpart % (1L << 24)).toInt

          case _ =>
            intpart = buffer.getBeUint24 - TIMEF_INT_OFS
            ltime = intpart << 24

        }
        var second: String = null
        if (intpart == 0) second = "00:00:00"
        else {
          val ultime = Math.abs(ltime)
          intpart = ultime >> 24
          second = String.format("%s%02d:%02d:%02d",
                                 if (ltime >= 0) ""
                                 else "-",
                                 ((intpart >> 12) % (1 << 10)).toInt,
                                 ((intpart >> 6) % (1 << 6)).toInt,
                                 (intpart % (1 << 6)).toInt)
        }
        if (meta >= 1) {
          var microSecond = usecondsToStr(Math.abs(frac), meta)
          microSecond = microSecond.substring(0, meta)
          value = second + '.' + microSecond
        } else value = second

      case MySQLConstants.MYSQL_TYPE_NEWDATE =>
        logger.warn(
          "MYSQL_TYPE_NEWDATE : This enumeration value is " + "only used internally and cannot exist in a binlog!")
        value = null

      case MySQLConstants.MYSQL_TYPE_DATE =>
        val i32 = buffer.getUint24
        if (i32 == 0) value = "0000-00-00"
        else value = String.format("%04d-%02d-%02d", i32 / (16 * 32), i32 / 32 % 16, i32 % 32)

      case MySQLConstants.MYSQL_TYPE_YEAR =>
        val i32 = buffer.getUint8
        if (i32 == 0) value = "0000"
        else value = String.valueOf((i32 + 1900).toShort)

      case MySQLConstants.MYSQL_TYPE_ENUM =>
        var int32: Array[Byte] = null
        len match {
          case 1 =>
            int32 = new Array[Byte](1)
            buffer.fillBytes(int32, 0, 1)

          case 2 =>
            int32 = new Array[Byte](2)
            buffer.fillBytes(int32, 0, 2)

          case _ =>
            throw new IllegalArgumentException("!! Unknown ENUM packlen = " + len)
        }
        value = int32

      case MySQLConstants.MYSQL_TYPE_SET =>
        val nbits = (meta & 0xFF) * 8
        len = (nbits + 7) / 8
        if (nbits <= 1) len = 1
        val set = new Array[Byte](len)
        buffer.fillBytes(set, 0, len)
        value = set

      case MySQLConstants.MYSQL_TYPE_TINY_BLOB =>
        logger.warn(
          "MYSQL_TYPE_TINY_BLOB : This enumeration value is " + "only used internally and cannot exist in a binlog!")

      case MySQLConstants.MYSQL_TYPE_MEDIUM_BLOB =>
        logger.warn(
          "MYSQL_TYPE_MEDIUM_BLOB : This enumeration value is " + "only used internally and cannot exist in a binlog!")

      case MySQLConstants.MYSQL_TYPE_LONG_BLOB =>
        logger.warn(
          "MYSQL_TYPE_LONG_BLOB : This enumeration value is " + "only used internally and cannot exist in a binlog!")

      case MySQLConstants.MYSQL_TYPE_BLOB =>
        var binary: Array[Byte] = null
        meta match {
          case 1 =>
            /* TINYBLOB/TINYTEXT */
            val len8 = buffer.getUint8
            binary = new Array[Byte](len8)
            buffer.fillBytes(binary, 0, len8)

          case 2 =>
            /* BLOB/TEXT */
            val len16 = buffer.getUint16
            binary = new Array[Byte](len16)
            buffer.fillBytes(binary, 0, len16)

          case 3 =>
            /* MEDIUMBLOB/MEDIUMTEXT */
            val len24 = buffer.getUint24
            binary = new Array[Byte](len24)
            buffer.fillBytes(binary, 0, len24)

          case 4 =>
            /* LONGBLOB/LONGTEXT */
            val len32 = buffer.getUint32.toInt
            binary = new Array[Byte](len32)
            buffer.fillBytes(binary, 0, len32)

          case _ =>
            throw new IllegalArgumentException("!! Unknown BLOB packlen = " + meta)
        }
        value = binary

      case MySQLConstants.MYSQL_TYPE_VARCHAR =>
      case MySQLConstants.MYSQL_TYPE_VAR_STRING =>
        len = meta
        if (len < 256) len = buffer.getUint8
        else len = buffer.getUint16
        val binary = new Array[Byte](len)
        buffer.fillBytes(binary, 0, len)
        value = binary

      case MySQLConstants.MYSQL_TYPE_STRING =>
        if (len < 256) len = buffer.getUint8
        else len = buffer.getUint16
        val binary = new Array[Byte](len)
        buffer.fillBytes(binary, 0, len)
        value = binary

      case MySQLConstants.MYSQL_TYPE_JSON =>
        len = buffer.getUint16
        buffer.forward(meta - 2)
        val position = buffer.position
        val jsonValue = JsonConversion.parse_value(buffer.getUint8, buffer, len - 1)
        val builder = new java.lang.StringBuilder
        jsonValue.toJsonString(builder)
        value = builder.toString
        buffer.position(position + len)

      case MySQLConstants.MYSQL_TYPE_GEOMETRY =>
        meta match {
          case 1 =>
            len = buffer.getUint8

          case 2 =>
            len = buffer.getUint16

          case 3 =>
            len = buffer.getUint24

          case 4 =>
            len = buffer.getUint32.toInt

          case _ =>
            throw new IllegalArgumentException("!! Unknown MYSQL_TYPE_GEOMETRY packlen = " + meta)
        }
        /* fill binary */
        val binary = new Array[Byte](len)
        buffer.fillBytes(binary, 0, len)
        value = binary

      case _ =>
        logger.error(
          String.format("!! Don't know how to handle column type=%d meta=%d (%04X)",
                        dataType,
                        meta,
                        meta))
        value = null
    }
    value
  }

  private def nextOneRow(buffer: LogBuffer, columns: util.BitSet) = {
    val hasOneRow = buffer.hasRemaining
    if (hasOneRow) {
      var column = 0
      for (i <- 0 until columnLen) {
        if (columns.get(i)) column += 1
      }
      nullBitIndex = 0
      nullBits.clear()
      buffer.fillBitmap(nullBits, column)
    }
    hasOneRow
  }

  private def calendarToStr(cal: Calendar) = {
    val year = cal.get(Calendar.YEAR)
    val month = cal.get(Calendar.MONTH) + 1
    val day = cal.get(Calendar.DAY_OF_MONTH)
    val hour = cal.get(Calendar.HOUR_OF_DAY)
    val minute = cal.get(Calendar.MINUTE)
    val second = cal.get(Calendar.SECOND)
    String.format("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second)
  }

  private def usecondsToStr(frac: Int, meta: Int) = {
    var sec = String.valueOf(frac)
    if (meta > 6) throw new IllegalArgumentException("unknow useconds meta : " + meta)
    if (sec.length < 6) {
      val result = new StringBuilder(6)
      var len = 6 - sec.length

      while ({
        len > 0
      }) {
        result.append('0')

        len -= 1
      }
      result.append(sec)
      sec = result.toString
    }
    sec.substring(0, meta)
  }

}
