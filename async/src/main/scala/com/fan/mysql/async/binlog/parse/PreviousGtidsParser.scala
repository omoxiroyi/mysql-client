package com.fan.mysql.async.binlog.parse

import java.math.BigInteger

import com.fan.mysql.async.binlog.event.impl.PreviousGtidsEvent
import com.fan.mysql.async.binlog.event.{BinlogEvent, EventHeader}
import com.fan.mysql.async.binlog.position.GtidSetPosition
import com.fan.mysql.async.binlog.{BinlogDumpContext, BinlogEventParser}
import com.fan.mysql.async.util.ByteUtil
import com.fan.mysql.async.util.ChannelWrapper._
import io.netty.buffer.ByteBuf

class PreviousGtidsParser extends BinlogEventParser {

  override def parse(buffer: ByteBuf, header: EventHeader, context: BinlogDumpContext): BinlogEvent = {
    val event = new PreviousGtidsEvent(header)
    val sb = new StringBuffer
    val sidNumberCount = buffer.readLong()
    var i = 0
    while ( {
      i < sidNumberCount
    }) {
      if (sb.length > 0) sb.append(",")
      val sourceId = buffer.readBytes(16).array()
      val uuidStr = parseServerId(sourceId)
      sb.append(uuidStr)
      val internalCount = buffer.readLong()
      var j = 0
      while ( {
        j < internalCount
      }) {
        val from = buffer.readUnsignedLong()
        val to = buffer.readUnsignedLong() - BigInteger.valueOf(1)
        sb.append(":").append(from).append("-").append(to)

        j += 1
      }

      i += 1
    }
    event.setGtidSet(sb.toString)
    event
  }

  def getBody(gtidSet: String): Array[Byte] = {
    val pos = new GtidSetPosition(gtidSet)
    // calculate body size
    var bodySize = 8
    val uuids = pos.getUUIDSets

    uuids.forEach { uuidSet =>
      bodySize += 16 + 8
      bodySize += 16 * uuidSet.getIntervals.size
    }
    // fill body
    val body = new Array[Byte](bodySize)
    var offset = 0
    ByteUtil.int8store(body, offset, uuids.size)
    offset += 8

    uuids.forEach { uuidSet =>
      val uuidByte = parseUuid(uuidSet.getUUID)
      System.arraycopy(uuidByte, 0, body, offset, 16)
      offset += 16
      ByteUtil.int8store(body, offset, uuidSet.getIntervals.size)
      offset += 8

      uuidSet.getIntervals.forEach { interval =>
        ByteUtil.int8store(body, offset, interval.getStart)
        offset += 8
        ByteUtil.int8store(body, offset, interval.getEnd + 1)
        offset += 8
      }
    }
    body
  }

  private def parseUuid(uuid: String) = {
    val source_id = new Array[Byte](16)
    var array_index = 0
    var str_index = 0

    import scala.util.control.Breaks._

    while (str_index < uuid.length) {
      breakable {
        val high_char = uuid.charAt({
          str_index += 1
          str_index - 1
        })
        if (high_char == '-') break() // continue
        val high = char2Byte(high_char)
        val low_char = uuid.charAt({
          str_index += 1
          str_index - 1
        })
        val low = char2Byte(low_char)
        source_id({
          array_index += 1
          array_index - 1
        }) = (high * 16 + low).toByte
      }
    }
    source_id
  }

  private def parseServerId(sourceId: Array[Byte]): String = {
    val sb = new StringBuilder
    if (sourceId == null || sourceId.length <= 0) return null
    for (i <- sourceId.indices) {
      val v = sourceId(i) & 0xff
      val hv = Integer.toHexString(v)
      if (hv.length < 2) sb.append(0)
      sb.append(hv)
      if (i == 3 | i == 5 | i == 7 | i == 9) sb.append("-")
    }
    sb.toString
  }

  private def char2Byte(c: Char) = {
    var b = 0
    c match {
      case '0' =>
        b = 0

      case '1' =>
        b = 1

      case '2' =>
        b = 2

      case '3' =>
        b = 3

      case '4' =>
        b = 4

      case '5' =>
        b = 5

      case '6' =>
        b = 6

      case '7' =>
        b = 7

      case '8' =>
        b = 8

      case '9' =>
        b = 9

      case 'a' =>
        b = 10

      case 'b' =>
        b = 11

      case 'c' =>
        b = 12

      case 'd' =>
        b = 13

      case 'e' =>
        b = 14

      case 'f' =>
        b = 15

      case _ =>

    }
    b
  }
}
