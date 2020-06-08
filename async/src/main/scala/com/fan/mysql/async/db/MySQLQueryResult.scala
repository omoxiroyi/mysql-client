

package com.fan.mysql.async.db


class MySQLQueryResult(
                        rowsAffected: Long,
                        message: String,
                        val lastInsertId: Long,
                        val statusFlags: Int,
                        val warnings: Int,
                        rows: Option[ResultSet] = None) extends QueryResult(rowsAffected, message, rows)
