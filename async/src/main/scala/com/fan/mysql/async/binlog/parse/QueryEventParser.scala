package com.fan.mysql.async.binlog.parse

import java.nio.charset.Charset

import com.fan.mysql.async.binlog.event.impl.QueryEvent
import com.fan.mysql.async.binlog.event.{BinlogEvent, EventHeader}
import com.fan.mysql.async.binlog.{BinlogDumpContext, BinlogEventParser}
import com.fan.mysql.async.util.ChannelWrapper._
import com.fan.mysql.async.util.Log.Logging
import com.fan.mysql.async.util.MySQLCharsetUtil
import io.netty.buffer.ByteBuf
import org.apache.commons.lang3.builder.{ToStringBuilder, ToStringStyle}

object QueryEventParser {

  /**
    * The maximum number of updated databases that a status of Query-log-event can
    * carry. It can redefined within a range [1.. OVER_MAX_DBS_IN_EVENT_MTS].
    */
  final val MAX_DBS_IN_EVENT_MTS = 16

  /**
    * When the actual number of databases exceeds MAX_DBS_IN_EVENT_MTS the final value of
    * OVER_MAX_DBS_IN_EVENT_MTS is is put into the mts_accessed_dbs status.
    */
  final val OVER_MAX_DBS_IN_EVENT_MTS = 254

  final val SYSTEM_CHARSET_MBMAXLEN = 3
  final val NAME_CHAR_LEN = 64
  /* Field/table name length */
  final val NAME_LEN: Int = NAME_CHAR_LEN * SYSTEM_CHARSET_MBMAXLEN

  // Status variable type
  final val Q_FLAGS2_CODE = 0
  final val Q_SQL_MODE_CODE = 1
  final val Q_CATALOG_CODE = 2
  final val Q_AUTO_INCREMENT = 3
  final val Q_CHARSET_CODE = 4
  final val Q_TIME_ZONE_CODE = 5
  final val Q_CATALOG_NZ_CODE = 6
  final val Q_LC_TIME_NAMES_CODE = 7
  final val Q_CHARSET_DATABASE_CODE = 8
  final val Q_TABLE_MAP_FOR_UPDATE_CODE = 9
  final val Q_MASTER_DATA_WRITTEN_CODE = 10
  final val Q_INVOKER = 11
  final val Q_UPDATED_DB_NAMES = 12
  final val Q_MICROSECONDS = 13
  final val Q_COMMIT_TS = 14
  final val Q_COMMIT_TS2 = 15
  final val Q_EXPLICIT_DEFAULTS_FOR_TIMESTAMP = 16
  final val Q_DDL_LOGGED_WITH_XID = 17
  final val Q_DEFAULT_COLLATION_FOR_UTF8MB4 = 18
  final val Q_SQL_REQUIRE_PRIMARY_KEY = 19

  /**
    * FROM MariaDB 5.5.34
    */
  final val Q_HRNOW = 128

  class StatusVariable {
    var flags2 = 0L
    var sql_mode = 0L
    var catalog: String = _
    var autoIncrementIncrement = 0
    var autoIncrementOffset = 0
    var clientCharset = 0
    var clientCollation = 0
    var serverCollation = 0
    var timezone: String = _
    var user: String = _
    var host: String = _
    var microseconds = 0
    var lcTimeNames = 0
    var charsetDatabase = 0
    var tableMapForUpdate: BigInt = _
    var masterDataWritten = 0L
    var updatedDbNames: Array[String] = _
    var explicitDefaultsForTimestamp = 0
    var ddlXid = 0L

    def getFlags2: Long = flags2

    def getSql_mode: Long = sql_mode

    def getCatalog: String = catalog

    def getAutoIncrementIncrement: Int = autoIncrementIncrement

    def getAutoIncrementOffset: Int = autoIncrementOffset

    def getClientCharset: Int = clientCharset

    def getClientCollation: Int = clientCollation

    def getServerCollation: Int = serverCollation

    def getTimezone: String = timezone

    def getUser: String = user

    def getHost: String = host

    def getMicroseconds: Int = microseconds

    def getLcTimeNames: Int = lcTimeNames

    def getCharsetDatabase: Int = charsetDatabase

    def getTableMapForUpdate: BigInt = tableMapForUpdate

    def getMasterDataWritten: Long = masterDataWritten

    def getUpdatedDbNames: Array[String] = updatedDbNames

    def getExplicitDefaultsForTimestamp: Int = explicitDefaultsForTimestamp

    def getDdlXid: Long = ddlXid

    override def toString: String =
      ToStringBuilder.reflectionToString(this, ToStringStyle.DEFAULT_STYLE)
  }

}

class QueryEventParser extends BinlogEventParser with Logging {

  import QueryEventParser._

  private[this] var serverCharset = "UTF-8"

  override def parse(buffer: ByteBuf,
                     header: EventHeader,
                     context: BinlogDumpContext): BinlogEvent = {
    val event = new QueryEvent(header)
    event.setThreadId(buffer.readUnsignedInt())
    event.setExecTime(buffer.readUnsignedInt())
    val dbLen = buffer.readUnsignedByte()
    event.setDatabaseLength(dbLen)
    event.setErrorCode(buffer.readUnsignedShort())
    val statusVariablesLen = buffer.readUnsignedShort()
    event.setStatusVariablesLength(statusVariablesLen)
    event.setStatusVariables(parseStatusVariables(buffer, statusVariablesLen))
    event.setDatabaseName(buffer.readFixedASCIString(dbLen + 1))
    val queryLen = buffer.writerIndex() - buffer.readerIndex()
    event.setQuery(buffer.readFixedString(queryLen, Charset.forName(serverCharset)))
    event
  }

  protected def parseStatusVariables(buffer: ByteBuf, statusVarsLen: Int): StatusVariable = {
    var code = -1
    val end = buffer.readerIndex() + statusVarsLen
    val `var` = new StatusVariable

    import scala.util.control.Breaks._

    breakable {
      while (buffer.readerIndex() < end) {

        code = buffer.readUnsignedByte()

        code match {
          case Q_FLAGS2_CODE =>
            `var`.flags2 = buffer.readUnsignedInt()

          case Q_SQL_MODE_CODE =>
            `var`.sql_mode = buffer.readLong()

          case Q_CATALOG_NZ_CODE =>
            `var`.catalog = buffer.readLengthASCIString()

          case Q_AUTO_INCREMENT =>
            `var`.autoIncrementIncrement = buffer.readUnsignedShort()
            `var`.autoIncrementOffset = buffer.readUnsignedShort()

          case Q_CHARSET_CODE =>
            // Charset: 6 byte character set flag.
            // 1-2 = character set client
            // 3-4 = collation client
            // 5-6 = collation server
            `var`.clientCharset = buffer.readUnsignedShort()
            `var`.clientCollation = buffer.readUnsignedShort()
            `var`.serverCollation = buffer.readUnsignedShort()

          case Q_TIME_ZONE_CODE =>
            `var`.timezone = buffer.readLengthASCIString()

          case Q_CATALOG_CODE =>
            /* for 5.0.x where 0<=x<=3 masters */
            val len = buffer.readUnsignedByte()
            `var`.catalog = buffer.readFixedASCIString(len + 1)

          case Q_LC_TIME_NAMES_CODE =>
            `var`.lcTimeNames = buffer.readUnsignedShort()

          case Q_CHARSET_DATABASE_CODE =>
            `var`.charsetDatabase = buffer.readUnsignedShort()

          case Q_TABLE_MAP_FOR_UPDATE_CODE =>
            `var`.tableMapForUpdate = buffer.readUnsignedLong()

          case Q_MASTER_DATA_WRITTEN_CODE =>
            `var`.masterDataWritten = buffer.readUnsignedInt()

          case Q_INVOKER =>
            `var`.user = buffer.readLengthASCIString()
            `var`.host = buffer.readLengthASCIString()

          case Q_UPDATED_DB_NAMES =>
            var mtsAccessedDbs = buffer.readUnsignedByte()

            /**
              * Notice, the following check is positive also in case of the master's
              * MAX_DBS_IN_EVENT_MTS > the slave's one and the event contains e.g the
              * master's MAX_DBS_IN_EVENT_MTS db:s.
              */
            if (mtsAccessedDbs > MAX_DBS_IN_EVENT_MTS) {
              mtsAccessedDbs = OVER_MAX_DBS_IN_EVENT_MTS.asInstanceOf[Short]
              break

            }
            `var`.updatedDbNames = new Array[String](mtsAccessedDbs)
            var i = 0
            while ({
              i < mtsAccessedDbs && buffer.readerIndex() < end
            }) {
              `var`.updatedDbNames(i) = buffer.readCString(Charset.defaultCharset)

              i += 1
            }

          case Q_MICROSECONDS =>
            `var`.microseconds = buffer.readUnsignedMedium()

          case Q_COMMIT_TS =>
            logger.debug("commit ts status var")

          case Q_COMMIT_TS2 =>
            logger.debug("commit ts2 status var")

          case Q_EXPLICIT_DEFAULTS_FOR_TIMESTAMP =>
            `var`.explicitDefaultsForTimestamp = buffer.readByte()

          case Q_DDL_LOGGED_WITH_XID =>
            `var`.ddlXid = buffer.readLong()

          case Q_DEFAULT_COLLATION_FOR_UTF8MB4 =>
            buffer.forward(2)

          case Q_SQL_REQUIRE_PRIMARY_KEY =>
            buffer.forward(1)

          case Q_HRNOW =>
            buffer.forward(3)

          case _ =>
            /*
             * That's why you must write status vars in growing order of code
             */
            logger.warn(
              "Query_log_event has unknown status vars (first has code: " + code + "), skipping the rest of them")
            break()
        }
      }
    }

    if (`var`.serverCollation > 0) {
      val mysqlCharset = MySQLCharsetUtil.getCharsetFromIndex(`var`.serverCollation)
      this.serverCharset = MySQLCharsetUtil.getJavaCharsetFromMysql(mysqlCharset)
    }

    `var`
  }

}
