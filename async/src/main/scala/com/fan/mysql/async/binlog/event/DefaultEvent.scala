package com.fan.mysql.async.binlog.event

class DefaultEvent(header: EventHeader) extends AbstractEvent(header) {
  override val originData: Array[Byte] = null
}
