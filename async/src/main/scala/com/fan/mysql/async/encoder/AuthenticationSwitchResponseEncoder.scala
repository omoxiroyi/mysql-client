package com.fan.mysql.async.encoder

import java.nio.charset.Charset

import com.fan.mysql.async.encoder.auth.AuthenticationMethod
import com.fan.mysql.async.exceptions.UnsupportedAuthenticationMethodException
import com.fan.mysql.async.message.client.{AuthenticationSwitchResponse, ClientMessage}
import com.fan.mysql.async.util.ByteBufferUtils
import io.netty.buffer.ByteBuf

class AuthenticationSwitchResponseEncoder(charset: Charset) extends MessageEncoder {

  def encode(message: ClientMessage): ByteBuf = {
    val switch = message.asInstanceOf[AuthenticationSwitchResponse]

    val method = switch.request.method
    val authenticator = AuthenticationMethod.Availables.getOrElse(
      method, {
        throw new UnsupportedAuthenticationMethodException(method)
      })

    val buffer = ByteBufferUtils.packetBuffer()

    val bytes = authenticator.generateAuthentication(charset, switch.password, switch.request.seed.getBytes(charset))
    buffer.writeBytes(bytes)

    buffer
  }

}
