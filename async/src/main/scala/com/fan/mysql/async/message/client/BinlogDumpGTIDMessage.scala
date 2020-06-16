package com.fan.mysql.async.message.client

case class BinlogDumpGTIDMessage(
                                  binlogFileName: String,
                                  binlogPosition: Long,
                                  gtidSet: String,
                                  serverId: Long
                                ) extends ClientMessage(ClientMessage.BinlogDumpGTID)
