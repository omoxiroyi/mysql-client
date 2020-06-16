package com.fan.mysql.async.binlog.parse

import com.fan.mysql.async.binlog.event.impl.XidEvent
import com.fan.mysql.async.binlog.event.{BinlogEvent, EventHeader}
import com.fan.mysql.async.binlog.{BinlogDumpContext, BinlogEventParser}
import io.netty.buffer.ByteBuf

class XidEventParser extends BinlogEventParser {

  override def parse(buffer: ByteBuf, header: EventHeader, context: BinlogDumpContext): BinlogEvent = {
    val xid = buffer.readLong()
    val event = new XidEvent(header)
    event.setXid(xid)
    context.getTableMapEvents.clear()
    event
  }
}
