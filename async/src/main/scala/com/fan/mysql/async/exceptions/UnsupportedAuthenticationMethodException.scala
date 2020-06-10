

package com.fan.mysql.async.exceptions

class UnsupportedAuthenticationMethodException(val authenticationType: String)
  extends DatabaseException("Unknown authentication method -> '%s'".format(authenticationType)) {

  def this(authType: Int) = {
    this(authType.toString)
  }

}