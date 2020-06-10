package com.fan.mysql.binlog

import com.fan.mysql.event.BinlogEvent

trait BinlogEventFilter {
  def accepts(event: BinlogEvent): Boolean
}
