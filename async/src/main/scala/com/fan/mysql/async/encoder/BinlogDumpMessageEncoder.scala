package com.fan.mysql.async.encoder

import java.nio.charset.Charset

import com.fan.mysql.async.message.client.{BinlogDumpMessage, ClientMessage}
import com.fan.mysql.async.util.ByteBufferUtils
import io.netty.buffer.ByteBuf

object BinlogDumpMessageEncoder {
  final val BINLOG_DUMP_NON_BLOCK = 1
  final val BINLOG_SEND_ANNOTATE_ROWS_EVENT = 2
}

class BinlogDumpMessageEncoder(charset: Charset) extends MessageEncoder {

  override def encode(message: ClientMessage): ByteBuf = {
    val m = message.asInstanceOf[BinlogDumpMessage]

    val encodedBinlogName = m.binlogFileName.getBytes(charset)

    val buffer = ByteBufferUtils.packetBuffer(4 + 2 + 4 + encodedBinlogName.length)

    buffer.writeByte(ClientMessage.BinlogDump)

    // 1. write 4 bytes bin-log position to start at
    buffer.writeInt(m.binlogPosition.asInstanceOf[Int])

    // 2. write 2 bytes bin-log flags
    var binlog_flags: Int = 0
    binlog_flags |= BinlogDumpMessageEncoder.BINLOG_SEND_ANNOTATE_ROWS_EVENT
    buffer.writeByte(binlog_flags)
    buffer.writeByte(0x00.toByte)

    // 3. write 4 bytes server id of the slave
    buffer.writeInt(m.slaveServerId.asInstanceOf[Int])

    // 4. write bin-log file name if necessary
    buffer.writeBytes(encodedBinlogName)

    buffer
  }
}
