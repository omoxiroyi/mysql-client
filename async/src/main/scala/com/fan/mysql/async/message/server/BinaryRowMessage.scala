package com.fan.mysql.async.message.server

import io.netty.buffer.ByteBuf

case class BinaryRowMessage(buffer: ByteBuf) extends ServerMessage(ServerMessage.BinaryRow)
