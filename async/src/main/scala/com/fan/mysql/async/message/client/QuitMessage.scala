package com.fan.mysql.async.message.client

object QuitMessage {
  val Instance = new QuitMessage();
}

class QuitMessage extends ClientMessage(ClientMessage.Quit)
