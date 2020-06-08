

package com.fan.mysql.async.message.server

case class EOFMessage(warningCount: Int, flags: Int)
  extends ServerMessage(ServerMessage.EOF)