package com.fan.mysql.async.column

trait ColumnEncoderRegistry {

  def encode(value: Any): String

  def kindOf(value: Any): Int

}
