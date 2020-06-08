package com.fan.mysql.driver

import java.util

import com.fan.mysql.packet.result.{FieldPacket, RowDataPacket}

/**
 * A query result consumer for eliminating the problem of using
 * too much memory when using resultSet directly
 */
trait MysqlConnQueryHandler {
  @throws[Exception]
  def onFieldPackets(fields: util.List[FieldPacket]): Unit

  @throws[Exception]
  def onRow(row: RowDataPacket): Unit

  @throws[Exception]
  def onRowEof(): Unit
}
