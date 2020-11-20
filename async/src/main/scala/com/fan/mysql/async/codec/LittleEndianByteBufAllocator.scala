/*
 * Copyright 2013 Norman Maurer
 *
 * Norman Maurer, licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.fan.mysql.async.codec

import java.nio.ByteOrder

import io.netty.buffer.{ByteBuf, ByteBufAllocator, CompositeByteBuf, UnpooledByteBufAllocator}

object LittleEndianByteBufAllocator {
  val INSTANCE: LittleEndianByteBufAllocator = new LittleEndianByteBufAllocator
}

/** Allocates ByteBuf which have LITTLE_ENDIAN order.
  */
class LittleEndianByteBufAllocator extends ByteBufAllocator {
  private val allocator = new UnpooledByteBufAllocator(false)

  def isDirectBufferPooled: Boolean = false

  def buffer(): ByteBuf = littleEndian(allocator.buffer())

  def buffer(initialCapacity: Int): ByteBuf = littleEndian(allocator.buffer(initialCapacity))

  def buffer(initialCapacity: Int, maxCapacity: Int): ByteBuf =
    littleEndian(allocator.buffer(initialCapacity, maxCapacity))

  def ioBuffer(): ByteBuf = littleEndian(allocator.ioBuffer())

  def ioBuffer(initialCapacity: Int): ByteBuf = littleEndian(allocator.ioBuffer(initialCapacity))

  def ioBuffer(initialCapacity: Int, maxCapacity: Int): ByteBuf =
    littleEndian(allocator.ioBuffer(initialCapacity, maxCapacity))

  def heapBuffer(): ByteBuf = littleEndian(allocator.heapBuffer())

  def heapBuffer(initialCapacity: Int): ByteBuf =
    littleEndian(allocator.heapBuffer(initialCapacity))

  def heapBuffer(initialCapacity: Int, maxCapacity: Int): ByteBuf =
    littleEndian(allocator.heapBuffer(initialCapacity, maxCapacity))

  def directBuffer(): ByteBuf = littleEndian(allocator.directBuffer())

  def directBuffer(initialCapacity: Int): ByteBuf =
    littleEndian(allocator.directBuffer(initialCapacity))

  def directBuffer(initialCapacity: Int, maxCapacity: Int): ByteBuf =
    littleEndian(allocator.directBuffer(initialCapacity, maxCapacity))

  def compositeBuffer(): CompositeByteBuf = allocator.compositeBuffer()

  def compositeBuffer(maxNumComponents: Int): CompositeByteBuf =
    allocator.compositeBuffer(maxNumComponents)

  def compositeHeapBuffer(): CompositeByteBuf = allocator.compositeHeapBuffer()

  def compositeHeapBuffer(maxNumComponents: Int): CompositeByteBuf =
    allocator.compositeHeapBuffer(maxNumComponents)

  def compositeDirectBuffer(): CompositeByteBuf = allocator.compositeDirectBuffer()

  def compositeDirectBuffer(maxNumComponents: Int): CompositeByteBuf =
    allocator.compositeDirectBuffer(maxNumComponents)

  def calculateNewCapacity(minNewCapacity: Int, maxCapacity: Int): Int =
    allocator.calculateNewCapacity(minNewCapacity, maxCapacity)

  //noinspection ScalaDeprecation
  private def littleEndian(b: ByteBuf) = b.order(ByteOrder.LITTLE_ENDIAN)

}
