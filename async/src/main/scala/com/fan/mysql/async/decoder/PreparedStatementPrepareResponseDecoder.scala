

package com.fan.mysql.async.decoder

import com.fan.mysql.async.message.server.{PreparedStatementPrepareResponse, ServerMessage}
import com.fan.mysql.async.util.Log
import io.netty.buffer.ByteBuf

class PreparedStatementPrepareResponseDecoder extends MessageDecoder {

  final val log = Log.get[PreparedStatementPrepareResponseDecoder]

  def decode(buffer: ByteBuf): ServerMessage = {

    //val dump = MySQLHelper.dumpAsHex(buffer)
    //log.debug("prepared statement response dump is \n{}", dump)

    val statementId = Array[Byte](buffer.readByte(), buffer.readByte(), buffer.readByte(), buffer.readByte())
    val columnsCount = buffer.readUnsignedShort()
    val paramsCount = buffer.readUnsignedShort()

    // filler
    buffer.readByte()

    val warningCount = buffer.readShort()

    PreparedStatementPrepareResponse(
      statementId = statementId,
      warningCount = warningCount,
      columnsCount = columnsCount,
      paramsCount = paramsCount
    )
  }

}
