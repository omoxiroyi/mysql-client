package com.fan.mysql.async.message.server

case class ErrorMessage(errorCode: Int, sqlState: String, errorMessage: String)
    extends ServerMessage(ServerMessage.Error)
