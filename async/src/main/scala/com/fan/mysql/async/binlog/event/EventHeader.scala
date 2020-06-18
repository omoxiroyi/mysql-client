package com.fan.mysql.async.binlog.event

import org.apache.commons.lang3.builder.{ToStringBuilder, ToStringStyle}

case class EventHeader(
                        timestamp: Long = 0,
                        eventType: Int = 0,
                        serverId: Long = 0,
                        eventLength: Long = 0,
                        nextPosition: Long = 0,
                        flags: Int = 0,
                        binlogFileName: String = null,
                        timestampOfReceipt: Long = 0,
                        checksumAlg: Int
                      ) {
  override def toString: String =
    ToStringBuilder.reflectionToString(this, ToStringStyle.DEFAULT_STYLE)
}
