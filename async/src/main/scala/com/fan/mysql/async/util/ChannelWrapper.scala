

package com.fan.mysql.async.util

import java.math.BigInteger
import java.nio.charset.Charset
import java.util

import com.fan.mysql.async.exceptions.UnknownLengthException
import io.netty.buffer.ByteBuf
import io.netty.util.{ByteProcessor, CharsetUtil}

import scala.language.implicitConversions

object ChannelWrapper {
  implicit def bufferToWrapper(buffer: ByteBuf): ChannelWrapper = new ChannelWrapper(buffer)

  final val MySQL_NULL = 0xfb
  final val log = Log.get[ChannelWrapper]

  final val BIGINT_MAX_VALUE: BigInt = BigInt("18446744073709551615")
}

class ChannelWrapper(val buffer: ByteBuf) extends AnyVal {

  import ChannelWrapper._

  def readFixedASCIString(length: Int): String =
    this.readFixedString(length, CharsetUtil.ISO_8859_1)

  def readLengthASCIString(): String =
    this.readLengthEncodedString(CharsetUtil.ISO_8859_1)

  def readFixedString(length: Int, charset: Charset): String = {
    val bytes = new Array[Byte](length)
    buffer.readBytes(bytes)
    new String(bytes, charset)
  }

  def readCString(charset: Charset): String = ByteBufferUtils.readCString(buffer, charset)

  def readUntilEOF(charset: Charset): String = ByteBufferUtils.readUntilEOF(buffer, charset)

  def readLengthEncodedString(charset: Charset): String = {
    val length = readBinaryLength
    readFixedString(length.asInstanceOf[Int], charset)
  }

  def readBinaryLength: Long = {
    val firstByte = buffer.readUnsignedByte()

    if (firstByte <= 250) {
      firstByte
    } else {
      firstByte match {
        case MySQL_NULL => -1
        case 252 => buffer.readUnsignedShort()
        case 253 => readLongInt
        case 254 => buffer.readLong()
        case _ => throw new UnknownLengthException(firstByte)
      }
    }

  }

  def readLongInt: Int = {
    val first = buffer.readByte()
    val second = buffer.readByte()
    val third = buffer.readByte()

    (first & 0xff) | ((second & 0xff) << 8) | ((third & 0xff) << 16)
  }

  def readUnsignedLong40LE(): Long = {
    if (buffer.readableBytes() < 6)
      throw new IllegalArgumentException(s"limit exceed: ${buffer.readerIndex() + 6}")

    ((buffer.readByte() & 0xff).asInstanceOf[Long] << 32) |
      ((buffer.readByte() & 0xff).asInstanceOf[Long] << 24) | ((buffer.readByte() & 0xff).asInstanceOf[Long] << 16) |
      ((buffer.readByte() & 0xff).asInstanceOf[Long] << 8) | (buffer.readByte() & 0xff).asInstanceOf[Long]
  }

  def readUnsignedLong48(): Long = {
    if (buffer.readableBytes() < 6)
      throw new IllegalArgumentException(s"limit exceed: ${buffer.readerIndex() + 6}")

    (buffer.readByte() & 0xff).asInstanceOf[Long] | ((buffer.readByte() & 0xff).asInstanceOf[Long] << 8) |
      ((buffer.readByte() & 0xff).asInstanceOf[Long] << 16) | ((buffer.readByte() & 0xff).asInstanceOf[Long] << 24) |
      ((buffer.readByte() & 0xff).asInstanceOf[Long] << 32) | ((buffer.readByte() & 0xff).asInstanceOf[Long] << 40)
  }

  def readUnsignedLong48LE(): Long = {
    if (buffer.readableBytes() < 6)
      throw new IllegalArgumentException(s"limit exceed: ${buffer.readerIndex() + 6}")

    ((buffer.readByte() & 0xff).asInstanceOf[Long] << 40) | ((buffer.readByte() & 0xff).asInstanceOf[Long] << 32) |
      ((buffer.readByte() & 0xff).asInstanceOf[Long] << 24) | ((buffer.readByte() & 0xff).asInstanceOf[Long] << 16) |
      ((buffer.readByte() & 0xff).asInstanceOf[Long] << 8) | (buffer.readByte() & 0xff).asInstanceOf[Long]
  }

  def readUnsignedLong(): BigInt = {
    val long64 = buffer.readLong()

    if (long64 >= 0)
      BigInt(long64)
    else
      BIGINT_MAX_VALUE + BigInteger.valueOf(1 + long64)
  }

  def readBitmap(len: Int): util.BitSet = {
    val bitmap = new util.BitSet(len)
    this.fillBitmap(bitmap, len, buffer)
    bitmap
  }

  def writeLength(length: Long): Unit = {
    if (length < 251) {
      buffer.writeByte(length.asInstanceOf[Byte])
    } else if (length < 65536L) {
      buffer.writeByte(252)
      buffer.writeShort(length.asInstanceOf[Int])
    } else if (length < 16777216L) {
      buffer.writeByte(253)
      writeLongInt(length.asInstanceOf[Int])
    } else {
      buffer.writeByte(254)
      buffer.writeLong(length)
    }
  }

  def writeLongInt(i: Int): Unit = {
    buffer.writeByte(i & 0xff)
    buffer.writeByte(i >>> 8)
    buffer.writeByte(i >>> 16)
  }

  def writeLengthEncodedString(value: String, charset: Charset): Unit = {
    val bytes = value.getBytes(charset)
    writeLength(bytes.length)
    buffer.writeBytes(bytes)
  }

  def writePacketLength(sequence: Int = 0): Unit = {
    ByteBufferUtils.writePacketLength(buffer, sequence)
  }

  def mysqlReadInt(): Int = {
    val first = buffer.readByte()
    val last = buffer.readByte()

    (first & 0xff) | ((last & 0xff) << 8)
  }

  def forward(len: Int): Unit = {
    if (buffer.readableBytes() < len)
      throw new IllegalArgumentException(s"limit exceed: ${buffer.readerIndex() + len}")

    buffer.readerIndex(buffer.readerIndex() + len)
  }

  def fillBitmap(bitmap: util.BitSet, len: Int, buffer: ByteBuf): Unit = {
    if (buffer.readerIndex() + ((len + 7) / 8) > buffer.writerIndex())
      throw new IllegalArgumentException(s"limit exceed: ${buffer.readerIndex() + ((len + 7) / 8)}")

    var bit = 0

    val begin = buffer.writerIndex()

    buffer.forEachByte(buffer.readerIndex(), len / 8, new ByteProcessor {
      override def process(flag: Byte): Boolean = {
        if (flag == 0) {
          bit += 8
          return true
        }

        if ((flag & 0x01) != 0)
          bitmap.set(bit)
        if ((flag & 0x02) != 0)
          bitmap.set(bit + 1)
        if ((flag & 0x04) != 0)
          bitmap.set(bit + 2)
        if ((flag & 0x08) != 0)
          bitmap.set(bit + 3)
        if ((flag & 0x10) != 0)
          bitmap.set(bit + 4)
        if ((flag & 0x20) != 0)
          bitmap.set(bit + 5)
        if ((flag & 0x40) != 0)
          bitmap.set(bit + 6)
        if ((flag & 0x80) != 0)
          bitmap.set(bit + 7)

        bit += 8
        true
      }
    })

    buffer.writerIndex(begin + len / 8)
  }

  def fillBytes(dest: Array[Byte], destPos: Int, len: Int): Unit = {
    if (buffer.readerIndex() + len > buffer.writerIndex())
      throw new IllegalArgumentException(s"limit exceed: ${buffer.readerIndex() + len}")

    System.arraycopy(buffer.readBytes(len).array(), 0, dest, destPos, len)
  }

  def toArray(): Array[Byte] = {
    val arr = new Array[Byte](buffer.readableBytes())
    buffer.readBytes(arr)
    arr
  }
}
