package com.fan.mysql.packet

import com.fan.mysql.util.MySQLPacketBuffer

/**
 * From client to server whenever the client wants the server to do something.
 *
 * <pre>
 * Bytes          Name
 * -----         ----
 * 1             command
 * n             arg
 *
 * command:      The most common final value is 03 COM_QUERY, because
 * INSERT UPDATE DELETE SELECT etc. have this code.
 * The possible final values at time of writing (taken
 * from /include/mysql_com.h for enum_server_command) are:
 *
 * #      Name                Associated client function
 *                -      ----                --------------------------  
 * 0x00   COM_SLEEP           (none, this is an internal thread state)
 * 0x01   COM_QUIT            mysql_close
 * 0x02   COM_INIT_DB         mysql_select_db 
 * 0x03   COM_QUERY           mysql_real_query
 * 0x04   COM_FIELD_LIST      mysql_list_fields
 * 0x05   COM_CREATE_DB       mysql_create_db (deprecated)
 * 0x06   COM_DROP_DB         mysql_drop_db (deprecated)
 * 0x07   COM_REFRESH         mysql_refresh
 * 0x08   COM_SHUTDOWN        mysql_shutdown
 * 0x09   COM_STATISTICS      mysql_stat
 * 0x0a   COM_PROCESS_INFO    mysql_list_processes
 * 0x0b   COM_CONNECT         (none, this is an internal thread state)
 * 0x0c   COM_PROCESS_KILL    mysql_kill
 * 0x0d   COM_DEBUG           mysql_dump_debug_info
 * 0x0e   COM_PING            mysql_ping
 * 0x0f   COM_TIME            (none, this is an internal thread state)
 * 0x10   COM_DELAYED_INSERT  (none, this is an internal thread state)
 * 0x11   COM_CHANGE_USER     mysql_change_user
 * 0x12   COM_BINLOG_DUMP     (used by slave server / mysqlbinlog)
 * 0x13   COM_TABLE_DUMP      (used by slave server to get master table)
 * 0x14   COM_CONNECT_OUT     (used by slave to log connection to master)
 * 0x15   COM_REGISTER_SLAVE  (used by slave to register to master)
 * 0x16   COM_STMT_PREPARE    mysql_stmt_prepare
 * 0x17   COM_STMT_EXECUTE    mysql_stmt_execute
 * 0x18   COM_STMT_SEND_LONG_DATA mysql_stmt_send_long_data
 * 0x19   COM_STMT_CLOSE      mysql_stmt_close
 * 0x1a   COM_STMT_RESET      mysql_stmt_reset
 * 0x1b   COM_SET_OPTION      mysql_set_server_option
 * 0x1c   COM_STMT_FETCH      mysql_stmt_fetch
 *
 * arg:          The text of the command is just the way the user typed it, there is no processing
 * by the client (except remofinal val of the final ';').
 * This field is not a null-terminated string; however,
 * the size can be calculated from the packet size,
 * and the MySQL client appends '\0' when receiving.
 * </pre>
 *
 * @author fan
 */
abstract class CommandPacket extends MySQLPacket {
  var commandType: Byte = 0

  override def init(buffer: MySQLPacketBuffer, charset: String): Unit = {
    super.init(buffer, charset)
    this.commandType = buffer.read
  }

  override def write2Buffer(buffer: MySQLPacketBuffer): Unit = {
    buffer.write(this.commandType)
  }

  override def calcPacketSize: Int = {
    var packetSize = super.calcPacketSize
    packetSize += 1
    packetSize
  }
}

object CommandPacket {
  final val COM_SLEEP: Byte = 0x00 // (none, this is an internal

  // thread state)
  final val COM_QUIT: Byte = 0x01 // mysql_close

  final val COM_INIT_DB: Byte = 0x02 // mysql_select_db

  final val COM_QUERY: Byte = 0x03 // mysql_real_query

  final val COM_FIELD_LIST: Byte = 0x04 // mysql_list_fields

  final val COM_CREATE_DB: Byte = 0x05 // mysql_create_db

  // (deprecated)
  final val COM_DROP_DB: Byte = 0x06 // mysql_drop_db (deprecated)

  final val COM_REFRESH: Byte = 0x07 // mysql_refresh

  final val COM_SHUTDOWN: Byte = 0x08 // mysql_shutdown

  final val COM_STATISTICS: Byte = 0x09 // mysql_stat

  final val COM_PROCESS_INFO: Byte = 0x0a // mysql_list_processes

  final val COM_CONNECT: Byte = 0x0b
  final val COM_PROCESS_KILL: Byte = 0x0c // mysql_kill

  final val COM_DEBUG: Byte = 0x0d // mysql_dump_debug_info

  final val COM_PING: Byte = 0x0e // mysql_ping

  final val COM_TIME: Byte = 0x0f
  final val COM_DELAYED_INSERT: Byte = 0x10 // (none, this is an

  // internal thread
  // state)
  final val COM_CHANGE_USER: Byte = 0x11 // mysql_change_user

  final val COM_BINLOG_DUMP: Byte = 0x12 // (used by slave server /

  // mysqlbinlog)
  final val COM_TABLE_DUMP: Byte = 0x13 // (used by slave server to

  // get master table)
  final val COM_CONNECT_OUT: Byte = 0x14 // (used by slave to log

  // connection to master)
  final val COM_REGISTER_SLAVE: Byte = 0x15 // (used by slave to

  // register to master)
  final val COM_STMT_PREPARE: Byte = 0x16 // mysql_stmt_prepare

  final val COM_STMT_EXECUTE: Byte = 0x17 // mysql_stmt_execute

  final val COM_STMT_SEND_LONG_DATA: Byte = 0x18 // mysql_stmt_send_long_data

  final val COM_STMT_CLOSE: Byte = 0x19 // mysql_stmt_close

  final val COM_STMT_RESET: Byte = 0x1a // mysql_stmt_reset

  final val COM_SET_OPTION: Byte = 0x1b // mysql_set_server_option

  final val COM_STMT_FETCH: Byte = 0x1c // mysql_stmt_fetch

  final val COM_RESET_CONNECTION: Byte = 0x1f // Resets the session

  // state
  final val COM_EOF: Byte = 0xfe.toByte //

  final val COM_CLOSE: Byte = 0xf1.toByte // definition by

  // connection is closed
  final val COM_DAEMON = 0x1d
}
