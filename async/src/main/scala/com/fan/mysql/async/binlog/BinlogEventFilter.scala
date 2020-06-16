package com.fan.mysql.async.binlog

import com.fan.mysql.async.binlog.event.BinlogEvent

trait BinlogEventFilter {
  def accepts(event: BinlogEvent): Boolean
}
