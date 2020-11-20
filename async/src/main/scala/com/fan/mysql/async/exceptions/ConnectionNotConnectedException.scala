package com.fan.mysql.async.exceptions

import com.fan.mysql.async.db.Connection

class ConnectionNotConnectedException(val connection: Connection)
    extends DatabaseException(
      "The connection %s is not connected to the database".format(connection))
