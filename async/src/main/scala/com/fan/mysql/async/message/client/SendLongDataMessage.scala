package com.fan.mysql.async.message.client

import io.netty.buffer.ByteBuf

case class SendLongDataMessage(statementId: Array[Byte], value: ByteBuf, paramId: Int)
