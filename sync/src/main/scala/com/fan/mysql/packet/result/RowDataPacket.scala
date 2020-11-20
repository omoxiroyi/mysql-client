package com.fan.mysql.packet.result

import java.util

import com.fan.mysql.packet.MySQLPacket
import com.fan.mysql.util.MySQLPacketBuffer

/** <pre>
  * Bytes                   Name
  * -----                   ----
  * n (Length Coded String) (column value)
  * ...
  *
  * (column value): The data in the column, as a character string.
  * If a column is defined as non-character, the
  * server converts the value into a character
  * before sending it. Since the value is a Length
  * Coded String, a NULL can be represented with a
  * single byte containing 251(see the description
  * of Length Coded Strings in section "Elements" above).
  *
  * &#64;see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Row_Data_Packet
  * </pre>
  *
  * @author fan
  */
class RowDataPacket extends MySQLPacket {
  protected val NULL_MARK: Byte = 251.toByte

  var fieldCount: Int                     = _
  var fieldValues: util.List[Array[Byte]] = _
  var isNull                              = false

  def this(fieldCount: Int) {
    this()
    this.fieldCount = fieldCount
    this.fieldValues = new util.ArrayList[Array[Byte]](fieldCount)
  }

  def add(value: Array[Byte]): Unit = {
    fieldValues.add(value)
  }

  def addAll(values: util.List[Array[Byte]]): Unit = {
    fieldValues.addAll(values)
  }

  def getValue(index: Int): Array[Byte] = fieldValues.get(index)

  def setValue(index: Int, value: Array[Byte]): Unit = {
    fieldValues.set(index, value)
  }

  def getNull: RowDataPacket = {
    val ret = new RowDataPacket(0)
    ret.isNull = true
    ret
  }

  override def init(buffer: MySQLPacketBuffer, charset: String): Unit = {
    super.init(buffer, charset)
    for (_ <- 0 until this.fieldCount) {
      this.fieldValues.add(buffer.readBytesWithLength)
    }
  }

  override def write2Buffer(buffer: MySQLPacketBuffer): Unit = {
    for (i <- 0 until this.fieldCount) {
      val fv = fieldValues.get(i)
      if (fv == null) buffer.write(NULL_MARK)
      else if (fv.length == 0) buffer.write(0.toByte)
      else buffer.writeBytesWithLength(fv)
    }
  }

  override def calcPacketSize: Int = {
    var size = super.calcPacketSize
    for (i <- 0 until this.fieldCount) {
      val v = this.fieldValues.get(i)
      size += {
        if (v == null || v.length == 0) 1
        else MySQLPacketBuffer.getLength(v)
      }
    }
    size
  }

  override def getPacketInfo = "MySQL RowData Packet"
}
