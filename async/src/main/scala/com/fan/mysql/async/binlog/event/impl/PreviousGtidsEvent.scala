package com.fan.mysql.async.binlog.event.impl

import com.fan.mysql.async.binlog.event.{AbstractEvent, EventHeader}

class PreviousGtidsEvent(header: EventHeader) extends AbstractEvent(header) {
  private[this] var gtidSet: String = _

  def getGtidSet: String = gtidSet

  def setGtidSet(gtidSet: String): Unit = {
    this.gtidSet = gtidSet
  }
}
