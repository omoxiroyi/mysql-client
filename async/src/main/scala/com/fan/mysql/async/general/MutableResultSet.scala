package com.fan.mysql.async.general

import com.fan.mysql.async.db.{ResultSet, RowData}
import com.fan.mysql.async.util.Log
import org.slf4j.Logger

import scala.collection.mutable.ArrayBuffer

object MutableResultSet {
  val log: Logger = Log.get[MutableResultSet[Nothing]]
}

class MutableResultSet[T <: ColumnData](val columnTypes: IndexedSeq[T]) extends ResultSet {

  private val rows = new ArrayBuffer[RowData]()
  private val columnMapping: Map[String, Int] =
    this.columnTypes.indices.map(index => (this.columnTypes(index).name, index)).toMap

  val columnNames: IndexedSeq[String] = this.columnTypes.map(c => c.name)

  val types: IndexedSeq[Int] = this.columnTypes.map(c => c.dataType)

  override def length: Int = this.rows.length

  override def apply(idx: Int): RowData = this.rows(idx)

  def addRow(row: Array[Any]): Unit = {
    this.rows += new ArrayRowData(this.rows.size, this.columnMapping, row)
  }

}
