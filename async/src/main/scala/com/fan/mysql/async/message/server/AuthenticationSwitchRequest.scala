package com.fan.mysql.async.message.server

case class AuthenticationSwitchRequest(
    method: String,
    seed: String
) extends ServerMessage(ServerMessage.EOF)
