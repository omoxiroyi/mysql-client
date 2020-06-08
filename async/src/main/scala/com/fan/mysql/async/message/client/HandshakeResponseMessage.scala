

package com.fan.mysql.async.message.client

import java.nio.charset.Charset

case class HandshakeResponseMessage(
                                     username: String,
                                     charset: Charset,
                                     seed: Array[Byte],
                                     authenticationMethod: String,
                                     password: Option[String] = None,
                                     database: Option[String] = None
                                   )
  extends ClientMessage(ClientMessage.ClientProtocolVersion)