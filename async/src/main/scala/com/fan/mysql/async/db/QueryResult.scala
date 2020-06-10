

package com.fan.mysql.async.db

/**
 *
 * This is the result of the execution of a statement, contains basic information as the number or rows
 * affected by the statement and the rows returned if there were any.
 *
 */
class QueryResult(val rowsAffected: Long, val statusMessage: String, val rows: Option[ResultSet] = None) {

  override def toString: String = {
    "QueryResult{rows -> %s,status -> %s}".format(this.rowsAffected, this.statusMessage)
  }

}
