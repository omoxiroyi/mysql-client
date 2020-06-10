package com.fan.mysql.binlog

import com.fan.mysql.event.BinlogEvent

trait BinlogEventHandler {
  def handle(event: BinlogEvent): Boolean
}
