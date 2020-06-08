

package com.fan.mysql.async.codec

import java.nio.charset.Charset

import com.fan.mysql.async.binary.decoder.{ByteDecoder, TimeDecoder, _}
import com.fan.mysql.async.column.{ByteDecoder => TextByteDecoder, TimeDecoder => TextTimeDecoder, _}
import com.fan.mysql.async.util.CharsetMapper

import scala.annotation.switch


class DecoderRegistry(charset: Charset) {

  private final val bigDecimalDecoder = new BigDecimalDecoder(charset)
  private final val stringDecoder = new StringDecoder(charset)

  def binaryDecoderFor(columnType: Int, charsetCode: Int): BinaryDecoder = {

    (columnType: @switch) match {
      case ColumnTypes.FIELD_TYPE_VARCHAR |
           ColumnTypes.FIELD_TYPE_ENUM => this.stringDecoder
      case ColumnTypes.FIELD_TYPE_BLOB |
           ColumnTypes.FIELD_TYPE_LONG_BLOB |
           ColumnTypes.FIELD_TYPE_MEDIUM_BLOB |
           ColumnTypes.FIELD_TYPE_TINY_BLOB |
           ColumnTypes.FIELD_TYPE_VAR_STRING |
           ColumnTypes.FIELD_TYPE_STRING =>
        if (charsetCode == CharsetMapper.Binary) {
          ByteArrayDecoder
        } else {
          this.stringDecoder
        }
      case ColumnTypes.FIELD_TYPE_BIT => ByteArrayDecoder
      case ColumnTypes.FIELD_TYPE_LONGLONG => LongDecoder
      case ColumnTypes.FIELD_TYPE_LONG | ColumnTypes.FIELD_TYPE_INT24 => IntegerDecoder
      case ColumnTypes.FIELD_TYPE_YEAR | ColumnTypes.FIELD_TYPE_SHORT => ShortDecoder
      case ColumnTypes.FIELD_TYPE_TINY => ByteDecoder
      case ColumnTypes.FIELD_TYPE_DOUBLE => DoubleDecoder
      case ColumnTypes.FIELD_TYPE_FLOAT => FloatDecoder
      case ColumnTypes.FIELD_TYPE_NUMERIC |
           ColumnTypes.FIELD_TYPE_DECIMAL |
           ColumnTypes.FIELD_TYPE_NEW_DECIMAL => this.bigDecimalDecoder
      case ColumnTypes.FIELD_TYPE_DATETIME | ColumnTypes.FIELD_TYPE_TIMESTAMP => TimestampDecoder
      case ColumnTypes.FIELD_TYPE_DATE => DateDecoder
      case ColumnTypes.FIELD_TYPE_TIME => TimeDecoder
      case ColumnTypes.FIELD_TYPE_NULL => NullDecoder
    }
  }

  def textDecoderFor(columnType: Int, charsetCode: Int): ColumnDecoder = {
    (columnType: @switch) match {
      case ColumnTypes.FIELD_TYPE_DATE => DateEncoderDecoder
      case ColumnTypes.FIELD_TYPE_DATETIME |
           ColumnTypes.FIELD_TYPE_TIMESTAMP => LocalDateTimeEncoderDecoder
      case ColumnTypes.FIELD_TYPE_DECIMAL |
           ColumnTypes.FIELD_TYPE_NEW_DECIMAL |
           ColumnTypes.FIELD_TYPE_NUMERIC => BigDecimalEncoderDecoder
      case ColumnTypes.FIELD_TYPE_DOUBLE => DoubleEncoderDecoder
      case ColumnTypes.FIELD_TYPE_FLOAT => FloatEncoderDecoder
      case ColumnTypes.FIELD_TYPE_INT24 => IntegerEncoderDecoder
      case ColumnTypes.FIELD_TYPE_LONG => IntegerEncoderDecoder
      case ColumnTypes.FIELD_TYPE_LONGLONG => LongEncoderDecoder
      case ColumnTypes.FIELD_TYPE_NEWDATE => DateEncoderDecoder
      case ColumnTypes.FIELD_TYPE_SHORT => ShortEncoderDecoder
      case ColumnTypes.FIELD_TYPE_TIME => TextTimeDecoder
      case ColumnTypes.FIELD_TYPE_TINY => TextByteDecoder
      case ColumnTypes.FIELD_TYPE_VARCHAR |
           ColumnTypes.FIELD_TYPE_ENUM => StringEncoderDecoder
      case ColumnTypes.FIELD_TYPE_YEAR => ShortEncoderDecoder
      case ColumnTypes.FIELD_TYPE_BIT => ByteArrayColumnDecoder
      case ColumnTypes.FIELD_TYPE_BLOB |
           ColumnTypes.FIELD_TYPE_VAR_STRING |
           ColumnTypes.FIELD_TYPE_STRING =>
        if (charsetCode == CharsetMapper.Binary) {
          ByteArrayColumnDecoder
        } else {
          StringEncoderDecoder
        }
      case _ => StringEncoderDecoder
    }

  }

}
