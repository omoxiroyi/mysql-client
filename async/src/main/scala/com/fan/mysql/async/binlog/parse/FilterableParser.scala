package com.fan.mysql.async.binlog.parse

import com.fan.mysql.async.binlog.{BinlogEventFilter, BinlogEventParser}

abstract class FilterableParser extends BinlogEventParser {

  protected var filter: BinlogEventFilter = _

  def setFilter(filter: BinlogEventFilter): Unit = {
    this.filter = filter
  }
}
