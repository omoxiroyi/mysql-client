

package com.fan.mysql.async.message.client

case class PreparedStatementPrepareMessage(statement: String)
  extends ClientMessage(ClientMessage.PreparedStatementPrepare)