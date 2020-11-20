package com.fan.mysql.async.message.server

case class ParamProcessingFinishedMessage(eofMessage: EOFMessage)
    extends ServerMessage(ServerMessage.ParamProcessingFinished)
