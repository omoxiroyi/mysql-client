package com.fan.mysql.async.message.client

import com.fan.mysql.async.db.KindedMessage

object ClientMessage {

  final val Quit = 0x01 // COM_QUIT
  final val Query = 0x03 // COM_QUERY
  final val ClientProtocolVersion = 0x09 // COM_STATISTICS
  final val BinlogDump = 0x12 // COM_BINLOG_DUMP
  final val PreparedStatementPrepare = 0x16 // COM_STMT_PREPARE
  final val PreparedStatementExecute = 0x17 // COM_STMT_EXECUTE
  final val PreparedStatementSendLongData = 0x18 // COM_STMT_SEND_LONG_DATA
  final val BinlogDumpGTID = 0x1e // COM_BINLOG_DUMP_GTID
  final val AuthSwitchResponse = 0xfe // AuthSwitchRequest

}

class ClientMessage(val kind: Int) extends KindedMessage
