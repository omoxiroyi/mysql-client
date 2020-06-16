package com.fan.mysql.async.binlog.parse

import com.fan.mysql.async.binlog.event.impl.RotateEvent
import com.fan.mysql.async.binlog.event.{BinlogEvent, EventHeader}
import com.fan.mysql.async.binlog.{BinlogDumpContext, BinlogEventParser}
import com.fan.mysql.async.util.ChannelWrapper._
import io.netty.buffer.ByteBuf

class RotateEventParser extends BinlogEventParser {

  override def parse(buffer: ByteBuf, header: EventHeader, context: BinlogDumpContext): BinlogEvent = {
    val position = buffer.readLong()
    val fileLen = buffer.writerIndex() - buffer.readerIndex()
    val file = buffer.readFixedASCIString(fileLen)
    val event = new RotateEvent(header)
    event.setBinlogFile(file)
    event.setBinlogPosition(position)
    context.setBinlogFileName(file)
    event
  }
}
