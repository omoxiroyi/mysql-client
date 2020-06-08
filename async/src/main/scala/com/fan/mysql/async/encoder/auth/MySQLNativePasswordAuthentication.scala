

package com.fan.mysql.async.encoder.auth

import java.nio.charset.Charset
import java.security.MessageDigest

object MySQLNativePasswordAuthentication extends AuthenticationMethod {

  final val EmptyArray = Array.empty[Byte]

  def generateAuthentication(charset: Charset, password: Option[String], seed: Array[Byte]): Array[Byte] = {

    if (password.isDefined) {
      scramble411(charset, password.get, seed)
    } else {
      EmptyArray
    }

  }

  private def scramble411(charset: Charset, password: String, seed: Array[Byte]): Array[Byte] = {

    val messageDigest = MessageDigest.getInstance("SHA-1")
    val initialDigest = messageDigest.digest(password.getBytes(charset))

    messageDigest.reset()

    val finalDigest = messageDigest.digest(initialDigest)

    messageDigest.reset()

    messageDigest.update(seed)
    messageDigest.update(finalDigest)

    val result = messageDigest.digest()
    var counter = 0

    while (counter < result.length) {
      result(counter) = (result(counter) ^ initialDigest(counter)).asInstanceOf[Byte]
      counter += 1
    }

    result
  }

}
