package com.fan.mysql.async.codec

case class PreparedStatement(statement: String, values: Seq[Any])
