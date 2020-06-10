package com.fan.mysql.packet.binlog

import java.util

import com.fan.mysql.packet.CommandPacket
import com.fan.mysql.position.GtidSetPosition
import com.fan.mysql.util.MySQLPacketBuffer

object BinlogDumpGtidPacket {
  val COM_BINLOG_DUMP_GTID: Byte = 0x1e
}

class BinlogDumpGtidPacket extends CommandPacket {
  private var binlogFileName: String = _
  private var binlogPosition: Long = 0L
  private var gtidSet: GtidSetPosition = _
  private var serverId: Long = -1

  def this(binlogFileName: String, binlogPosition: Long, gtidSet: String, serverId: Long) = {
    this()
    this.binlogFileName = binlogFileName
    this.binlogPosition = binlogPosition
    this.gtidSet = new GtidSetPosition(gtidSet)
    this.serverId = serverId
    this.commandType = BinlogDumpGtidPacket.COM_BINLOG_DUMP_GTID
  }

  override def write2Buffer(buffer: MySQLPacketBuffer): Unit = {
    super.write2Buffer(buffer)
    // write 2 byte flag
    buffer.writeUB2(0)
    // write 4 bytes server id of the slave
    buffer.writeUB4(serverId)
    // write 4 bytes of file name length
    val fileNameLen: Int = binlogFileName.length
    buffer.writeUB4(fileNameLen)
    // write n bytes of file name
    buffer.writeBytesNoNull(binlogFileName.getBytes)
    // write 8 bytes of file position
    buffer.writeLong(binlogPosition)
    // write data size
    var dataSize: Int = 8
    gtidSet.getUUIDSets.forEach { uuidSet =>
      dataSize += 16 + 8 + uuidSet.getIntervals.size * 16
    }
    buffer.writeUB4(dataSize)
    // write UUID sets
    buffer.writeLong(gtidSet.getUUIDSets.size)
    gtidSet.getUUIDSets.forEach { uuidSet =>
      buffer.writeBytesNoNull(hexToByteArray(uuidSet.getUUID.replace("-", "")))
      val intervals: util.Collection[GtidSetPosition.Interval] = uuidSet.getIntervals
      buffer.writeLong(intervals.size)

      intervals.forEach { interval =>
        buffer.writeLong(interval.getStart)
        buffer.writeLong(interval.getEnd + 1)
      }
    }
  }

  override def calcPacketSize: Int = {
    var size: Int = super.calcPacketSize
    size += 2 + 4 + 4
    size += binlogFileName.length
    size += 8 + 4
    var dataSize: Int = 8
    gtidSet.getUUIDSets.forEach { uuidSet =>
      dataSize += 16 + 8 + uuidSet.getIntervals.size * 16
    }
    size += dataSize
    size
  }

  override def getPacketInfo: String = "MYSQL Dump Packet"

  private def hexToByteArray(uuid: String): Array[Byte] = {
    val b: Array[Byte] = new Array[Byte](uuid.length / 2)
    var i: Int = 0
    var j: Int = 0
    while ( {
      j < uuid.length
    }) {
      b({
        i += 1
        i - 1
      }) = Integer.parseInt(uuid.charAt(j) + "" + uuid.charAt(j + 1), 16).toByte

      j += 2
    }
    b
  }

}
