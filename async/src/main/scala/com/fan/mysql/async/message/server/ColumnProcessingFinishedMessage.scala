package com.fan.mysql.async.message.server

case class ColumnProcessingFinishedMessage(eofMessage: EOFMessage)
    extends ServerMessage(ServerMessage.ColumnDefinitionFinished)
