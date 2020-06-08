

package com.fan.mysql.async.message.client

import com.fan.mysql.async.message.server.ColumnDefinitionMessage


case class PreparedStatementExecuteMessage(
                                            statementId: Array[Byte],
                                            values: Seq[Any],
                                            valuesToInclude: Set[Int],
                                            parameters: Seq[ColumnDefinitionMessage])
  extends ClientMessage(ClientMessage.PreparedStatementExecute)