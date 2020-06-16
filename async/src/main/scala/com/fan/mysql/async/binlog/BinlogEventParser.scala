package com.fan.mysql.async.binlog

import com.fan.mysql.async.binlog.event.{BinlogEvent, EventHeader}
import io.netty.buffer.ByteBuf

trait BinlogEventParser {
  def parse(buffer: ByteBuf, header: EventHeader, context: BinlogDumpContext): BinlogEvent
}
