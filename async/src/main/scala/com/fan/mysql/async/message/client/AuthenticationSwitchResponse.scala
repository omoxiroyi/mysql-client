package com.fan.mysql.async.message.client

import com.fan.mysql.async.message.server.AuthenticationSwitchRequest

case class AuthenticationSwitchResponse(password: Option[String],
                                        request: AuthenticationSwitchRequest)
    extends ClientMessage(ClientMessage.AuthSwitchResponse)
