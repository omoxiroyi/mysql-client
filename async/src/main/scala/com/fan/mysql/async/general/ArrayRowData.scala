

package com.fan.mysql.async.general

import com.fan.mysql.async.db.RowData

class ArrayRowData(row: Int, val mapping: Map[String, Int], val columns: Array[Any]) extends RowData {

  /**
   *
   * Returns a column value by it's position in the originating query.
   *
   * @param columnNumber
   * @return
   */
  def apply(columnNumber: Int): Any = columns(columnNumber)

  /**
   *
   * Returns a column value by it's name in the originating query.
   *
   * @param columnName
   * @return
   */
  def apply(columnName: String): Any = columns(mapping(columnName))

  /**
   *
   * Number of this row in the query results. Counts start at 0.
   *
   * @return
   */
  def rowNumber: Int = row

  def length: Int = columns.length
}
