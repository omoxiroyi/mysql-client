package com.fan.mysql.async.binlog.event.impl

import com.fan.mysql.async.binlog.event.{AbstractEvent, EventHeader}

class XidEvent(header: EventHeader) extends AbstractEvent(header) {

  private[this] var xid: Long = _

  def getXid: Long = xid

  def setXid(xid: Long): Unit = {
    this.xid = xid
  }
}
