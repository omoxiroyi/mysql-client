
package com.fan.mysql.async.encoder

import com.fan.mysql.async.binary.BinaryRowEncoder
import com.fan.mysql.async.column.ColumnTypes
import com.fan.mysql.async.message.client.{ClientMessage, PreparedStatementExecuteMessage}
import com.fan.mysql.async.util.ByteBufferUtils
import io.netty.buffer.{ByteBuf, Unpooled}


class PreparedStatementExecuteEncoder(rowEncoder: BinaryRowEncoder) extends MessageEncoder {

  def encode(message: ClientMessage): ByteBuf = {
    val m = message.asInstanceOf[PreparedStatementExecuteMessage]

    val buffer = ByteBufferUtils.packetBuffer()
    buffer.writeByte(m.kind)
    buffer.writeBytes(m.statementId)
    buffer.writeByte(0x00) // no cursor
    buffer.writeInt(1)

    if (m.parameters.isEmpty) {
      buffer
    } else {
      Unpooled.wrappedBuffer(buffer, encodeValues(m.values, m.valuesToInclude))
    }

  }

  private[encoder] def encodeValues(values: Seq[Any], valuesToInclude: Set[Int]): ByteBuf = {
    val nullBitsCount = (values.size + 7) / 8
    val nullBits = new Array[Byte](nullBitsCount)
    val bitMapBuffer = ByteBufferUtils.mysqlBuffer(1 + nullBitsCount)
    val parameterTypesBuffer = ByteBufferUtils.mysqlBuffer(values.size * 2)
    val parameterValuesBuffer = ByteBufferUtils.mysqlBuffer()

    var index = 0

    while (index < values.length) {
      val value = values(index)
      if (value == null || value == None) {
        nullBits(index / 8) = (nullBits(index / 8) | (1 << (index & 7))).asInstanceOf[Byte]
        parameterTypesBuffer.writeShort(ColumnTypes.FIELD_TYPE_NULL)
      } else {
        value match {
          case Some(v) => encodeValue(parameterTypesBuffer, parameterValuesBuffer, v, valuesToInclude(index))
          case _ => encodeValue(parameterTypesBuffer, parameterValuesBuffer, value, valuesToInclude(index))
        }
      }
      index += 1
    }

    bitMapBuffer.writeBytes(nullBits)
    if (values.nonEmpty) {
      bitMapBuffer.writeByte(1)
    } else {
      bitMapBuffer.writeByte(0)
    }

    Unpooled.wrappedBuffer(bitMapBuffer, parameterTypesBuffer, parameterValuesBuffer)
  }

  private def encodeValue(parameterTypesBuffer: ByteBuf, parameterValuesBuffer: ByteBuf, value: Any, includeValue: Boolean): Unit = {
    val encoder = rowEncoder.encoderFor(value)
    parameterTypesBuffer.writeShort(encoder.encodesTo)
    if (includeValue)
      encoder.encode(value, parameterValuesBuffer)
  }

}