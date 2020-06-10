package com.fan.mysql.packet.binlog

import com.fan.mysql.packet.CommandPacket
import com.fan.mysql.util.MySQLPacketBuffer

object BinlogDumpFilePacket {
  /** BINLOG_DUMP options */
  val BINLOG_DUMP_NON_BLOCK: Int = 1
  val BINLOG_SEND_ANNOTATE_ROWS_EVENT: Int = 2
}

class BinlogDumpFilePacket extends CommandPacket {
  var binlogPosition = 0L
  var slaveServerId: Long = -1
  var binlogFileName: String = _

  def this(binlogFileName: String, binlogPosition: Long, slaveServerId: Long) = {
    this()
    this.binlogFileName = binlogFileName
    this.binlogPosition = binlogPosition
    this.slaveServerId = slaveServerId
    this.commandType = CommandPacket.COM_BINLOG_DUMP
  }

  /**
   * <pre>
   * Bytes                        Name
   * -----                        ----
   * 1                            command
   * n                            arg
   * --------------------------------------------------------
   * Bytes                        Name
   * -----                        ----
   * 4                            binlog position to start at (little endian)
   * 2                            binlog flags (currently not used; always 0)
   * 4                            server_id of the slave (little endian)
   * n                            binlog file name (optional)
   *
   * </pre>
   */

  override def write2Buffer(buffer: MySQLPacketBuffer): Unit = {
    super.write2Buffer(buffer)
    // 1. write 4 bytes bin-log position to start at
    buffer.writeUB4(binlogPosition)
    // 2. write 2 bytes bin-log flags
    var binlog_flags: Int = 0
    binlog_flags |= BinlogDumpFilePacket.BINLOG_SEND_ANNOTATE_ROWS_EVENT
    buffer.write(binlog_flags.toByte)
    buffer.write(0x00.toByte)
    // 3. write 4 bytes server id of the slave
    buffer.writeUB4(this.slaveServerId)
    // 4. write bin-log file name if necessary
    if (this.binlogFileName != null) buffer.writeStringNoNull(this.binlogFileName)
  }

  override def calcPacketSize: Int = {
    var size = super.calcPacketSize
    size += 10
    size += this.binlogFileName.getBytes.length
    size
  }

  override def getPacketInfo = "MYSQL Dump Packet"
}
