package com.fan.mysql.async.message.server

import com.fan.mysql.async.binary.decoder.BinaryDecoder
import com.fan.mysql.async.column.{ColumnDecoder, ColumnTypes}
import com.fan.mysql.async.general.ColumnData
import com.fan.mysql.async.util.CharsetMapper

case class ColumnDefinitionMessage(
    catalog: String,
    schema: String,
    table: String,
    originalTable: String,
    name: String,
    originalName: String,
    characterSet: Int,
    columnLength: Long,
    columnType: Int,
    flags: Short,
    decimals: Byte,
    binaryDecoder: BinaryDecoder,
    textDecoder: ColumnDecoder
) extends ServerMessage(ServerMessage.ColumnDefinition)
    with ColumnData {

  def dataType: Int = this.columnType

  def dataTypeSize: Long = this.columnLength

  override def toString: String = {
    val columnTypeName = ColumnTypes.Mapping.getOrElse(columnType, columnType)
    val charsetName = CharsetMapper.DefaultCharsetsById.getOrElse(characterSet, characterSet)

    s"${this.getClass.getSimpleName}(name=$name,columnType=$columnTypeName,table=$table,charset=$charsetName,decimals=$decimals})"
  }
}
