

package com.fan.mysql.async.exceptions

class UnknownLengthException(length: Int)
  extends DatabaseException("Can't handle the length %d".format(length))