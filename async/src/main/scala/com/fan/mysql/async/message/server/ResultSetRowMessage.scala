

package com.fan.mysql.async.message.server

import io.netty.buffer.ByteBuf

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class ResultSetRowMessage
  extends ServerMessage(ServerMessage.Row)
    with mutable.Buffer[ByteBuf] {

  private val buffer = new ArrayBuffer[ByteBuf]()

  def length: Int = buffer.length

  def apply(idx: Int): ByteBuf = buffer(idx)

  def update(n: Int, newelem: ByteBuf) {
    buffer.update(n, newelem)
  }

  override def addOne(elem: ByteBuf): this.type = {
    this.buffer += elem
    this
  }

  def clear() {
    this.buffer.clear()
  }

  override def prepend(elem: ByteBuf): this.type = {
    this.buffer.+=:(elem)
    this
  }

  def insertAll(n: Int, elems: Iterable[ByteBuf]) {
    this.buffer.insertAll(n, elems)
  }

  def remove(n: Int): ByteBuf = {
    this.buffer.remove(n)
  }

  override def iterator: Iterator[ByteBuf] = this.buffer.iterator

  override def insert(idx: Int, elem: ByteBuf): Unit = {
    this.buffer.insert(idx, elem)
  }

  override def insertAll(idx: Int, elems: IterableOnce[ByteBuf]): Unit = {
    this.buffer.insertAll(idx, elems)
  }

  override def remove(idx: Int, count: Int): Unit = {
    this.buffer.remove(idx, count)
  }

  override def patchInPlace(from: Int, patch: IterableOnce[ByteBuf], replaced: Int): ResultSetRowMessage.this.type = {
    this.buffer.patchInPlace(from, patch, replaced)
    this
  }
}