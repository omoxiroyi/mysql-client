package com.fan.mysql.async.encoder

import java.nio.charset.Charset

import com.fan.mysql.async.binlog.position.GtidSetPosition
import com.fan.mysql.async.message.client.{BinlogDumpGTIDMessage, ClientMessage}
import com.fan.mysql.async.util.ByteBufferUtils
import io.netty.buffer.ByteBuf

class BinlogDumpGTIDMessageEncoder(charset: Charset) extends MessageEncoder {
  override def encode(message: ClientMessage): ByteBuf = {
    val m = message.asInstanceOf[BinlogDumpGTIDMessage]

    val encodedBinlogName = m.binlogFileName.getBytes(charset)

    val buffer = ByteBufferUtils.packetBuffer()

    buffer.writeByte(ClientMessage.BinlogDumpGTID)

    // write 2 byte flag
    buffer.writeShort(0)
    // write 4 bytes server id of the slave
    buffer.writeInt(m.serverId.asInstanceOf[Int])
    // write 4 bytes of file name length
    val fileNameLen = m.binlogFileName.length
    buffer.writeInt(fileNameLen)
    // write n bytes of file name
    buffer.writeBytes(m.binlogFileName.getBytes)
    // write 8 bytes of file position
    buffer.writeLong(m.binlogPosition)
    // write data size
    var dataSize = 8

    val gtidSet = new GtidSetPosition(m.gtidSet)

    gtidSet.getUUIDSets.forEach { uuidSet =>
      dataSize += 16 + 8 + uuidSet.getIntervals.size * 16
    }

    buffer.writeInt(dataSize)

    // write UUID sets
    buffer.writeLong(gtidSet.getUUIDSets.size)

    gtidSet.getUUIDSets.forEach { uuidSet =>
      buffer.writeBytes(hexToByteArray(uuidSet.getUUID.replace("-", "")))
      val intervals = uuidSet.getIntervals
      buffer.writeLong(intervals.size)
      intervals.forEach { interval =>
        buffer.writeLong(interval.getStart)
        buffer.writeLong(interval.getEnd + 1)
      }
    }

    buffer
  }

  private def hexToByteArray(uuid: String) = {
    val b = new Array[Byte](uuid.length / 2)
    var i = 0
    var j = 0
    while (j < uuid.length) {
      b({
        i += 1
        i - 1
      }) = Integer.parseInt(uuid.charAt(j) + "" + uuid.charAt(j + 1), 16).toByte

      j += 2
    }
    b
  }
}
