package com.fan.mysql.async.exceptions

class NegativeMessageSizeException(code: Byte, size: Int)
    extends DatabaseException("Message of type %d had negative size %s".format(code, size))
