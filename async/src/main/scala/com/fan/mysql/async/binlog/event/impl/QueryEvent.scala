package com.fan.mysql.async.binlog.event.impl

import com.fan.mysql.async.binlog.event.{AbstractEvent, EventHeader}
import com.fan.mysql.async.binlog.parse.QueryEventParser.StatusVariable

class QueryEvent(header: EventHeader) extends AbstractEvent(header) {
  private[this] var threadId = 0L
  private[this] var execTime = 0L
  private[this] var databaseLength = 0
  private[this] var errorCode = 0
  private[this] var statusVariablesLength = 0
  private[this] var statusVariables: StatusVariable = _
  private[this] var databaseName: String = _
  private[this] var query: String = _

  def getThreadId: Long = threadId

  def setThreadId(threadId: Long): Unit = {
    this.threadId = threadId
  }

  def getExecTime: Long = execTime

  def setExecTime(execTime: Long): Unit = {
    this.execTime = execTime
  }

  def getDatabaseLength: Int = databaseLength

  def setDatabaseLength(databaseLength: Int): Unit = {
    this.databaseLength = databaseLength
  }

  def getErrorCode: Int = errorCode

  def setErrorCode(errorCode: Int): Unit = {
    this.errorCode = errorCode
  }

  def getStatusVariablesLength: Int = statusVariablesLength

  def setStatusVariablesLength(statusVariablesLength: Int): Unit = {
    this.statusVariablesLength = statusVariablesLength
  }

  def getStatusVariables: StatusVariable = statusVariables

  def setStatusVariables(statusVariables: StatusVariable): Unit = {
    this.statusVariables = statusVariables
  }

  def getDatabaseName: String = databaseName

  def setDatabaseName(databaseName: String): Unit = {
    this.databaseName = databaseName
  }

  def getQuery: String = query

  def setQuery(query: String): Unit = {
    this.query = query
  }

}
