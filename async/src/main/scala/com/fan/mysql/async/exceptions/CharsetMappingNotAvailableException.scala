package com.fan.mysql.async.exceptions

import java.nio.charset.Charset

class CharsetMappingNotAvailableException(charset: Charset)
    extends DatabaseException(
      "There is no MySQL charset mapping name for the Java Charset %s".format(charset.name()))
