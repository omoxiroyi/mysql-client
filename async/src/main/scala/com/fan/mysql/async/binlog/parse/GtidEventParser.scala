package com.fan.mysql.async.binlog.parse

import com.fan.mysql.async.binlog.event.impl.GtidLogEvent
import com.fan.mysql.async.binlog.event.{BinlogEvent, EventHeader}
import com.fan.mysql.async.binlog.{BinlogDumpContext, BinlogEventParser}
import io.netty.buffer.ByteBuf

class GtidEventParser extends BinlogEventParser {
  override def parse(buffer: ByteBuf,
                     header: EventHeader,
                     context: BinlogDumpContext): BinlogEvent = {
    val event = new GtidLogEvent(header)

    buffer.readByte() // commit flag, always true

    event.setSourceId(buffer.readBytes(16).array())
    event.setTransactionId(buffer.readLong())

    if (buffer.isReadable) {
      event.setLogicalTimestamp(buffer.readByte())
      event.setLastCommitted(buffer.readLong())
      event.setSequenceNumber(buffer.readLong())
    }

    event
  }
}
