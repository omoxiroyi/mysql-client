package com.fan.mysql.async.message.client

case class BinlogDumpMessage(
                              binlogFileName: String,
                              binlogPosition: Long,
                              slaveServerId: Long
                            ) extends ClientMessage(ClientMessage.BinlogDump)
