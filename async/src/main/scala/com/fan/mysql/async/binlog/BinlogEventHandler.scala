package com.fan.mysql.async.binlog

import com.fan.mysql.async.binlog.event.BinlogEvent

trait BinlogEventHandler {
  def handle(event: BinlogEvent): Boolean
}
