package com.fan.mysql.async.message.client

case class QueryMessage(query: String) extends ClientMessage(ClientMessage.Query)
