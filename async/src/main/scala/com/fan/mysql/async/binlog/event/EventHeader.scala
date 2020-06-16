package com.fan.mysql.async.binlog.event

case class EventHeader(
                        timestamp: Long = null,
                        eventType: Int = null,
                        serverId: Long = null,
                        eventLength: Long = null,
                        nextPosition: Long = null,
                        flags: Int = null,
                        binlogFileName: String = null,
                        timestampOfReceipt: Long = null,
                        checksumAlg: Int = null
                      )
