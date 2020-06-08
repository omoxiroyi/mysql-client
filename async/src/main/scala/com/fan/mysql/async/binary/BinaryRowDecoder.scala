

package com.fan.mysql.async.binary

import _root_.io.netty.buffer.ByteBuf
import com.fan.mysql.async.exceptions.BufferNotFullyConsumedException
import com.fan.mysql.async.message.server.ColumnDefinitionMessage
import com.fan.mysql.async.util.Log

import scala.collection.mutable.ArrayBuffer

object BinaryRowDecoder {
  final val log = Log.get[BinaryRowDecoder]
  final val BitMapOffset = 9
}

class BinaryRowDecoder {

  //import BinaryRowDecoder._

  def decode(buffer: ByteBuf, columns: Seq[ColumnDefinitionMessage]): Array[Any] = {

    //log.debug("columns are {} - {}", buffer.readableBytes(), columns)
    //log.debug( "decoding row\n{}", MySQLHelper.dumpAsHex(buffer))
    //PrintUtils.printArray("bitmap", buffer)

    val nullCount = (columns.size + 9) / 8

    val nullBitMask = new Array[Byte](nullCount)
    buffer.readBytes(nullBitMask)

    var nullMaskPos = 0
    var bit = 4

    val row = new ArrayBuffer[Any](columns.size)

    var index = 0

    while (index < columns.size) {

      if ((nullBitMask(nullMaskPos) & bit) != 0) {
        row += null
      } else {

        val column = columns(index)

        //log.debug(s"${decoder.getClass.getSimpleName} - ${buffer.readableBytes()}")
        //log.debug("Column value [{}] - {}", value, column.name)

        row += column.binaryDecoder.decode(buffer)
      }

      bit <<= 1

      if ((bit & 255) == 0) {
        bit = 1
        nullMaskPos += 1
      }

      index += 1
    }

    //log.debug("values are {}", row)

    if (buffer.readableBytes() != 0) {
      throw new BufferNotFullyConsumedException(buffer)
    }

    row.toArray
  }

}