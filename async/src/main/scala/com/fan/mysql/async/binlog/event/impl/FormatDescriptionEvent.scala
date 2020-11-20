package com.fan.mysql.async.binlog.event.impl

import com.fan.mysql.async.binlog.event.{AbstractEvent, EventHeader}

class FormatDescriptionEvent(header: EventHeader) extends AbstractEvent(header) {
  private[this] var binlogVersion: Int = _
  private[this] var serverVersion: String = _
  private[this] var createTime: Long = _
  private[this] var commonHeaderLen: Int = _
  private[this] var postHeaderLen: Array[Short] = _

  def this(binlogChecksum: Int) = {
    this(EventHeader(checksumAlg = binlogChecksum))
  }

  def getBinlogVersion: Int = binlogVersion

  def setBinlogVersion(binlogVersion: Int): Unit = {
    this.binlogVersion = binlogVersion
  }

  def getServerVersion: String = serverVersion

  def setServerVersion(serverVersion: String): Unit = {
    this.serverVersion = serverVersion
  }

  def getCreateTime: Long = createTime

  def setCreateTime(createTime: Long): Unit = {
    this.createTime = createTime
  }

  def getCommonHeaderLen: Int = commonHeaderLen

  def setCommonHeaderLen(commonHeaderLen: Int): Unit = {
    this.commonHeaderLen = commonHeaderLen
  }

  def getPostHeaderLen: Array[Short] = postHeaderLen

  def setPostHeaderLen(postHeaderLen: Array[Short]): Unit = {
    this.postHeaderLen = postHeaderLen
  }

}
