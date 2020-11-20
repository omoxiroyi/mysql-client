package com.fan.mysql.util

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.SocketChannel

import com.fan.mysql.packet.HeaderPacket

object PacketManager {
  final val HEADER_LENGTH = 4

  @throws[IOException]
  def readNextPacket(ch: SocketChannel): Array[Byte] = {
    val header = readHeader(ch, HEADER_LENGTH)
    val body   = readBytes(ch, header.packetBodyLength)
    mergeBytes(header.toBytes, body)
  }

  @throws[IOException]
  def readHeader(ch: SocketChannel, len: Int): HeaderPacket =
    HeaderPacket.fromBytes(readBytes(ch, len))

  @throws[IOException]
  def readBytesAsBuffer(ch: SocketChannel, len: Int): ByteBuffer = {
    val buffer = ByteBuffer.allocate(len)
    while (buffer.hasRemaining) {
      val readNum = ch.read(buffer)
      if (readNum == -1) {
        throw new IOException("Unexpected End Stream")
      }
    }
    buffer
  }

  @throws[IOException]
  def readBytes(ch: SocketChannel, len: Int): Array[Byte] = readBytesAsBuffer(ch, len).array()

  def mergeBytes(b1: Array[Byte], b2: Array[Byte]): Array[Byte] = {
    if (b1 == null) b2
    else if (b2 == null) b1
    else {
      val b3 = new Array[Byte](b1.length + b2.length)
      System.arraycopy(b1, 0, b3, 0, b1.length)
      System.arraycopy(b2, 0, b3, b1.length, b2.length)
      b3
    }
  }

  /** Since We r using blocking IO, so we will just write once and assert the
    * length to simplify the read operation.<br>
    * If the block write doesn't work as we expected, we will change this
    * implementation as per the result.
    */
  @throws[IOException]
  def write(ch: SocketChannel, src: Array[ByteBuffer]): Unit = {
    ch.write(src)
  }

  @throws[IOException]
  def write(ch: SocketChannel, data: Array[Byte]): Unit = {
    write(ch, Array[ByteBuffer](ByteBuffer.wrap(data)))
  }
}
