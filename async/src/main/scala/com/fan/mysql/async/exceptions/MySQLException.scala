

package com.fan.mysql.async.exceptions

import com.fan.mysql.async.message.server.ErrorMessage


class MySQLException(val errorMessage: ErrorMessage)
  extends DatabaseException("Error %d - %s - %s".format(errorMessage.errorCode, errorMessage.sqlState, errorMessage.errorMessage))
