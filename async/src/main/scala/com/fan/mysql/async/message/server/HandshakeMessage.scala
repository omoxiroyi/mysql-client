package com.fan.mysql.async.message.server

case class HandshakeMessage(
    serverVersion: String,
    connectionId: Long,
    seed: Array[Byte],
    serverCapabilities: Int,
    characterSet: Int,
    statusFlags: Int,
    authenticationMethod: String
) extends ServerMessage(ServerMessage.ServerProtocolVersion)
