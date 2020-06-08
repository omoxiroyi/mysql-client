
package com.fan.mysql.async.message.server

case class OkMessage(
                      affectedRows: Long,
                      lastInsertId: Long,
                      statusFlags: Int,
                      warnings: Int,
                      message: String)
  extends ServerMessage(ServerMessage.Ok)