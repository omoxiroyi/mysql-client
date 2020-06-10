package com.fan.mysql.parser;


import com.fan.mysql.binlog.BinlogEventParser;
import com.fan.mysql.dbsync.BinlogContext;
import com.fan.mysql.dbsync.LogBuffer;
import com.fan.mysql.event.BinlogEvent;
import com.fan.mysql.event.EventHeader;
import com.fan.mysql.event.impl.QueryEvent;
import com.fan.mysql.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;

@SuppressWarnings("unused")
public class QueryEventParser implements BinlogEventParser {

    private static final Logger logger = LoggerFactory.getLogger(QueryEventParser.class);

    /**
     * The maximum number of updated databases that a status of Query-log-event can
     * carry. It can redefined within a range [1.. OVER_MAX_DBS_IN_EVENT_MTS].
     */
    public static final int MAX_DBS_IN_EVENT_MTS = 16;
    /**
     * When the actual number of databases exceeds MAX_DBS_IN_EVENT_MTS the value of
     * OVER_MAX_DBS_IN_EVENT_MTS is is put into the mts_accessed_dbs status.
     */
    public static final int OVER_MAX_DBS_IN_EVENT_MTS = 254;

    public static final int SYSTEM_CHARSET_MBMAXLEN = 3;
    public static final int NAME_CHAR_LEN = 64;
    /* Field/table name length */
    public static final int NAME_LEN = (NAME_CHAR_LEN * SYSTEM_CHARSET_MBMAXLEN);

    // Status variable type
    public static final int Q_FLAGS2_CODE = 0;
    public static final int Q_SQL_MODE_CODE = 1;
    public static final int Q_CATALOG_CODE = 2;
    public static final int Q_AUTO_INCREMENT = 3;
    public static final int Q_CHARSET_CODE = 4;
    public static final int Q_TIME_ZONE_CODE = 5;
    public static final int Q_CATALOG_NZ_CODE = 6;
    public static final int Q_LC_TIME_NAMES_CODE = 7;
    public static final int Q_CHARSET_DATABASE_CODE = 8;
    public static final int Q_TABLE_MAP_FOR_UPDATE_CODE = 9;
    public static final int Q_MASTER_DATA_WRITTEN_CODE = 10;
    public static final int Q_INVOKER = 11;
    public static final int Q_UPDATED_DB_NAMES = 12;
    public static final int Q_MICROSECONDS = 13;
    public static final int Q_COMMIT_TS = 14;
    public static final int Q_COMMIT_TS2 = 15;
    public static final int Q_EXPLICIT_DEFAULTS_FOR_TIMESTAMP = 16;
    public static final int Q_DDL_LOGGED_WITH_XID = 17;
    public static final int Q_DEFAULT_COLLATION_FOR_UTF8MB4 = 18;
    public static final int Q_SQL_REQUIRE_PRIMARY_KEY = 19;
    /**
     * FROM MariaDB 5.5.34
     */
    public static final int Q_HRNOW = 128;

    private String serverCharset = "UTF-8";

    @Override
    public BinlogEvent parse(LogBuffer buffer, EventHeader header, BinlogContext context) {
        QueryEvent event = new QueryEvent(header);
        event.setThreadId(buffer.getUint32());
        event.setExecTime(buffer.getUint32());
        int dbLen = buffer.getUint8();
        event.setDatabaseLength(dbLen);
        event.setErrorCode(buffer.getUint16());
        int statusVariablesLen = buffer.getUint16();
        event.setStatusVariablesLength(statusVariablesLen);
        event.setStatusVariables(parseStatusVariables(buffer, statusVariablesLen));
        event.setDatabaseName(buffer.getFixString(dbLen + 1));
        int queryLen = buffer.limit() - buffer.position();
        event.setQuery(buffer.getFixString(queryLen, serverCharset));
        return event;
    }

    protected StatusVariable parseStatusVariables(LogBuffer buffer, int statusVarsLen) {
        int code;
        int end = buffer.position() + statusVarsLen;
        StatusVariable var = new StatusVariable();
        while (buffer.position() < end) {
            switch (code = buffer.getUint8()) {
                case Q_FLAGS2_CODE:
                    var.flags2 = buffer.getUint32();
                    break;
                case Q_SQL_MODE_CODE:
                    var.sql_mode = buffer.getLong64();
                    break;
                case Q_CATALOG_NZ_CODE:
                    var.catalog = buffer.getString();
                    break;
                case Q_AUTO_INCREMENT:
                    var.autoIncrementIncrement = buffer.getUint16();
                    var.autoIncrementOffset = buffer.getUint16();
                    break;
                case Q_CHARSET_CODE:
                    // Charset: 6 byte character set flag.
                    // 1-2 = character set client
                    // 3-4 = collation client
                    // 5-6 = collation server
                    var.clientCharset = buffer.getUint16();
                    var.clientCollation = buffer.getUint16();
                    var.serverCollation = buffer.getUint16();
                    break;
                case Q_TIME_ZONE_CODE:
                    var.timezone = buffer.getString();
                    break;
                case Q_CATALOG_CODE: /* for 5.0.x where 0<=x<=3 masters */
                    final int len = buffer.getUint8();
                    var.catalog = buffer.getFixString(len + 1);
                    break;
                case Q_LC_TIME_NAMES_CODE:
                    var.lcTimeNames = buffer.getUint16();
                    break;
                case Q_CHARSET_DATABASE_CODE:
                    var.charsetDatabase = buffer.getUint16();
                    break;
                case Q_TABLE_MAP_FOR_UPDATE_CODE:
                    var.tableMapForUpdate = buffer.getUlong64();
                    break;
                case Q_MASTER_DATA_WRITTEN_CODE:
                    var.masterDataWritten = buffer.getUint32();
                    break;
                case Q_INVOKER:
                    var.user = buffer.getString();
                    var.host = buffer.getString();
                    break;
                case Q_UPDATED_DB_NAMES:
                    int mtsAccessedDbs = buffer.getUint8();
                    if (mtsAccessedDbs > MAX_DBS_IN_EVENT_MTS) {
						break;
                    }
                    var.updatedDbNames = new String[mtsAccessedDbs];
                    for (int i = 0; i < mtsAccessedDbs && buffer.position() < end; i++) {
                        var.updatedDbNames[i] = buffer.getZeroTerminatedString();
                        // int length = end - buffer.position();
                        // var.updatedDbNames[i] = buffer.getFixString(length <
                        // NAME_LEN ? length : NAME_LEN);
                    }
                    break;
                case Q_MICROSECONDS:
                    var.microseconds = buffer.getUint24();
                    break;
                case Q_COMMIT_TS:
                    logger.debug("commit ts status var");
                    break;
                case Q_COMMIT_TS2:
                    logger.debug("commit ts2 status var");
                    break;
                case Q_EXPLICIT_DEFAULTS_FOR_TIMESTAMP:
                    var.explicitDefaultsForTimestamp = buffer.getInt8();
                    break;
                case Q_DDL_LOGGED_WITH_XID:
                    var.ddlXid = buffer.getLong64();
                    break;
                case Q_DEFAULT_COLLATION_FOR_UTF8MB4:
                    buffer.forward(2);
                    break;
                case Q_SQL_REQUIRE_PRIMARY_KEY:
                    buffer.forward(1);
                    break;
                case Q_HRNOW:
                    buffer.forward(3);
                    break;
                default:
                    /*
                     * That's why you must write status vars in growing order of code
                     */
                    logger.warn("Query_log_event has unknown status vars (first has code: " + code
                            + "), skipping the rest of them");
                    break; // Break loop
            }
        }
        if (var.serverCollation > 0) {
            String mysqlCharset = CharsetUtil.getCharsetFromIndex(var.serverCollation);
            this.serverCharset = CharsetUtil.getJavaCharsetFromMysql(mysqlCharset);
        }
        return var;
    }

    @SuppressWarnings("unused")
    public static class StatusVariable {

        private long flags2;
        private long sql_mode;
        private String catalog;
        private int autoIncrementIncrement;
        private int autoIncrementOffset;
        private int clientCharset;
        private int clientCollation;
        private int serverCollation;
        private String timezone;
        private String user;
        private String host;
        private int microseconds;
        private int lcTimeNames;
        private int charsetDatabase;
        private BigInteger tableMapForUpdate;
        private long masterDataWritten;
        private String[] updatedDbNames;
        private int explicitDefaultsForTimestamp;
        private long ddlXid;

        public long getFlags2() {
            return flags2;
        }

        public long getSql_mode() {
            return sql_mode;
        }

        public String getCatalog() {
            return catalog;
        }

        public int getAutoIncrementIncrement() {
            return autoIncrementIncrement;
        }

        public int getAutoIncrementOffset() {
            return autoIncrementOffset;
        }

        public int getClientCharset() {
            return clientCharset;
        }

        public int getClientCollation() {
            return clientCollation;
        }

        public int getServerCollation() {
            return serverCollation;
        }

        public String getTimezone() {
            return timezone;
        }

        public String getUser() {
            return user;
        }

        public String getHost() {
            return host;
        }

        public int getMicroseconds() {
            return microseconds;
        }

        public int getLcTimeNames() {
            return lcTimeNames;
        }

        public int getCharsetDatabase() {
            return charsetDatabase;
        }

        public BigInteger getTableMapForUpdate() {
            return tableMapForUpdate;
        }

        public long getMasterDataWritten() {
            return masterDataWritten;
        }

        public String[] getUpdatedDbNames() {
            return updatedDbNames;
        }

        public int getExplicitDefaultsForTimestamp() {
            return explicitDefaultsForTimestamp;
        }

        public long getDdlXid() {
            return ddlXid;
        }

    }

}
