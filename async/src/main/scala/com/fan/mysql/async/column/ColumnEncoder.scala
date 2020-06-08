

package com.fan.mysql.async.column

trait ColumnEncoder {

  def encode(value: Any): String = value.toString

}
