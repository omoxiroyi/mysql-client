package com.fan.mysql.async.message.server

case class PreparedStatementPrepareResponse(statementId: Array[Byte],
                                            warningCount: Short,
                                            paramsCount: Int,
                                            columnsCount: Int)
    extends ServerMessage(ServerMessage.PreparedStatementPrepareResponse)
