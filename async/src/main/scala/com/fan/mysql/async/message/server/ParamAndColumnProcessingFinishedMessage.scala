package com.fan.mysql.async.message.server

case class ParamAndColumnProcessingFinishedMessage(eofMessage: EOFMessage)
    extends ServerMessage(ServerMessage.ParamAndColumnProcessingFinished)
