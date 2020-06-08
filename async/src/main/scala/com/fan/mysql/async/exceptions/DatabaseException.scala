

package com.fan.mysql.async.exceptions

class DatabaseException(message: String, cause: Throwable) extends RuntimeException(message) {

  def this(message: String) = this(message, null)

}