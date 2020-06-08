package com.fan.mysql.packet.result

import com.fan.mysql.packet.MySQLPacket
import com.fan.mysql.util.MySQLPacketBuffer

/**
 * <pre>
 * lenenc_str catalog
 * lenenc_str schema
 * lenenc_str table
 * lenenc_str org_table
 * lenenc_str name
 * lenenc_str org_name
 * lenenc_int length of fixed-length fields [0c]
 * 2 character set
 * 4 column length
 * 1 type
 * 2 flags
 * 1 decimals
 * 2 filler [00] [00]
 * if command was COM_FIELD_LIST {
 * lenenc_int length of default-values
 * string[$len] default values
 * }
 * </pre>
 *
 * @see https://dev.mysql.com/doc/internals/en/com-query-response.html#column-type
 * @author fan
 */
class FieldPacket extends MySQLPacket {
  private val DEFAULT_CATALOG = "def".getBytes
  private val FILLER = new Array[Byte](2)

  var catalog: Array[Byte] = DEFAULT_CATALOG
  var db: String = _
  var table: String = _
  var orgTable: String = _
  var name: String = _
  var orgName: String = _
  var charsetIndex: Int = _
  var length: Long = _
  var `type`: Int = _
  var flags: Int = _
  var decimals: Byte = _
  var definition: Array[Byte] = _

  def copy: FieldPacket = {
    val field = new FieldPacket
    field.catalog = DEFAULT_CATALOG
    field.db = this.db
    field.table = this.table
    field.orgTable = this.orgTable
    field.name = this.name
    field.orgName = this.orgName
    field.charsetIndex = this.charsetIndex
    field.length = this.length
    field.`type` = this.`type`
    field.flags = this.flags
    field.decimals = this.decimals
    if (this.definition != null) {
      field.definition = new Array[Byte](this.definition.length)
      System.arraycopy(this.definition, 0, field.definition, 0, this.definition.length)
    }
    field
  }

  override def init(buffer: MySQLPacketBuffer, charset: String): Unit = {
    super.init(buffer, charset)
    this.catalog = buffer.readBytesWithLength
    this.db = buffer.readStringWithLength
    this.table = buffer.readStringWithLength
    this.orgTable = buffer.readStringWithLength
    this.name = buffer.readStringWithLength
    this.orgName = buffer.readStringWithLength
    buffer.move(1)
    this.charsetIndex = buffer.readUB2
    this.length = buffer.readUB4
    this.`type` = buffer.read & 0xff
    this.flags = buffer.readUB2
    this.decimals = buffer.read
    buffer.move(FILLER.length)
    if (buffer.hasRemaining) this.definition = buffer.readBytesWithLength
  }

  override def write2Buffer(buffer: MySQLPacketBuffer): Unit = {
    val nullVal: Byte = 0
    buffer.writeBytesWithLength(this.catalog, nullVal)
    buffer.writeStringWithLength(this.db, nullVal)
    buffer.writeStringWithLength(this.table, nullVal)
    buffer.writeStringWithLength(this.orgTable, nullVal)
    buffer.writeStringWithLength(this.name, nullVal)
    buffer.writeStringWithLength(this.orgName, nullVal)
    buffer.write(0x0C.toByte)
    buffer.writeUB2(this.charsetIndex)
    buffer.writeUB4(this.length)
    buffer.write((`type` & 0xff).toByte)
    buffer.writeUB2(this.flags)
    buffer.write(this.decimals)
    buffer.move(FILLER.length)
    if (this.definition != null) buffer.writeBytesWithLength(this.definition)
  }

  override def calcPacketSize: Int = {
    var size = super.calcPacketSize
    size += (if (catalog == null) 1
    else MySQLPacketBuffer.getLength(catalog))
    size += (if (db == null) 1
    else MySQLPacketBuffer.getLength(db.getBytes))
    size += (if (table == null) 1
    else MySQLPacketBuffer.getLength(table.getBytes))
    size += (if (orgTable == null) 1
    else MySQLPacketBuffer.getLength(orgTable.getBytes))
    size += (if (name == null) 1
    else MySQLPacketBuffer.getLength(name.getBytes))
    size += (if (orgName == null) 1
    else MySQLPacketBuffer.getLength(orgName.getBytes))
    size += 13 // 1+2+4+1+2+1+2

    if (definition != null) size += MySQLPacketBuffer.getLength(definition)
    size
  }

  override def getPacketInfo = "MySQL Field Packet"
}
