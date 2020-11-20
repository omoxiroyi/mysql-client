package com.fan.mysql.packet.result

import java.net.SocketAddress
import java.util

class ResultSetPacket {

  private[this] var sourceAddress: SocketAddress             = _
  private[this] var fieldDescriptors: util.List[FieldPacket] = new util.ArrayList[FieldPacket]
  private[this] val rows: util.List[util.List[String]]       = new util.ArrayList[util.List[String]]

  def setFieldDescriptors(fieldDescriptors: util.List[FieldPacket]): Unit = {
    this.fieldDescriptors = fieldDescriptors
  }

  def getFieldDescriptors: util.List[FieldPacket] = fieldDescriptors

  def addRow(row: util.List[String]): Unit = {
    this.rows.add(row)
  }

  def getRows: util.List[util.List[String]] = rows

  def setSourceAddress(sourceAddress: SocketAddress): Unit = {
    this.sourceAddress = sourceAddress
  }

  def getSourceAddress: SocketAddress = sourceAddress

  override def toString: String =
    "ResultSetPacket [fieldDescriptors=" + fieldDescriptors + ", fieldValues=" + rows + ", sourceAddress=" + sourceAddress + "]"

}
