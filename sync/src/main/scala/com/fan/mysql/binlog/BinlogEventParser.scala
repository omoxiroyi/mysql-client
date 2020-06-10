package com.fan.mysql.binlog

import com.fan.mysql.dbsync.{BinlogContext, LogBuffer}
import com.fan.mysql.event.{BinlogEvent, EventHeader}

trait BinlogEventParser {
  def parse(buffer: LogBuffer, header: EventHeader, context: BinlogContext): BinlogEvent
}
