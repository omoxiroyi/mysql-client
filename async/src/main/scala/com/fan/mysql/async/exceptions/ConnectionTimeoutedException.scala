package com.fan.mysql.async.exceptions

import com.fan.mysql.async.db.Connection

class ConnectionTimeoutedException(val connection: Connection)
    extends DatabaseException(
      "The connection %s has a timeouted query and is being closed".format(connection))
