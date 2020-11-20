package com.fan.mysql.async.encoder.auth

import java.nio.charset.Charset

object AuthenticationMethod {

  final val Native: String = "mysql_native_password"
  final val Old: String = "mysql_old_password"

  final val Availables = Map(
    Native -> MySQLNativePasswordAuthentication,
    Old -> OldPasswordAuthentication
  )
}

trait AuthenticationMethod {

  def generateAuthentication(charset: Charset,
                             password: Option[String],
                             seed: Array[Byte]): Array[Byte]

}
