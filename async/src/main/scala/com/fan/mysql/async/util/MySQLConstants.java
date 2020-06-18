package com.fan.mysql.async.util;

@SuppressWarnings({"unused", "SpellCheckingInspection"})
public final class MySQLConstants {

    /* mysql value types */
    public static final int MYSQL_TYPE_DECIMAL = 0;
    public static final int MYSQL_TYPE_TINY = 1;
    public static final int MYSQL_TYPE_SHORT = 2;
    public static final int MYSQL_TYPE_LONG = 3;
    public static final int MYSQL_TYPE_FLOAT = 4;
    public static final int MYSQL_TYPE_DOUBLE = 5;
    public static final int MYSQL_TYPE_NULL = 6;
    public static final int MYSQL_TYPE_TIMESTAMP = 7;
    public static final int MYSQL_TYPE_LONGLONG = 8;
    public static final int MYSQL_TYPE_INT24 = 9;
    public static final int MYSQL_TYPE_DATE = 10;
    public static final int MYSQL_TYPE_TIME = 11;
    public static final int MYSQL_TYPE_DATETIME = 12;
    public static final int MYSQL_TYPE_YEAR = 13;
    public static final int MYSQL_TYPE_NEWDATE = 14;
    public static final int MYSQL_TYPE_VARCHAR = 15;
    public static final int MYSQL_TYPE_BIT = 16;
    public static final int MYSQL_TYPE_TIMESTAMP2 = 17;
    public static final int MYSQL_TYPE_DATETIME2 = 18;
    public static final int MYSQL_TYPE_TIME2 = 19;

    public static final int MYSQL_TYPE_JSON = 245;
    public static final int MYSQL_TYPE_NEWDECIMAL = 246;
    public static final int MYSQL_TYPE_ENUM = 247;
    public static final int MYSQL_TYPE_SET = 248;
    public static final int MYSQL_TYPE_TINY_BLOB = 249;
    public static final int MYSQL_TYPE_MEDIUM_BLOB = 250;
    public static final int MYSQL_TYPE_LONG_BLOB = 251;
    public static final int MYSQL_TYPE_BLOB = 252;
    public static final int MYSQL_TYPE_VAR_STRING = 253;
    public static final int MYSQL_TYPE_STRING = 254;
    public static final int MYSQL_TYPE_GEOMETRY = 255;

    /* log events types */
    public static final int UNKNOWN_EVENT = 0;
    public static final int START_EVENT_V3 = 1;
    public static final int QUERY_EVENT = 2;
    public static final int STOP_EVENT = 3;
    public static final int ROTATE_EVENT = 4;
    public static final int INTVAR_EVENT = 5;
    public static final int LOAD_EVENT = 6;
    public static final int SLAVE_EVENT = 7;
    public static final int CREATE_FILE_EVENT = 8;
    public static final int APPEND_BLOCK_EVENT = 9;
    public static final int EXEC_LOAD_EVENT = 10;
    public static final int DELETE_FILE_EVENT = 11;
    public static final int NEW_LOAD_EVENT = 12;
    public static final int RAND_EVENT = 13;
    public static final int USER_VAR_EVENT = 14;
    public static final int FORMAT_DESCRIPTION_EVENT = 15;
    public static final int XID_EVENT = 16;
    public static final int BEGIN_LOAD_QUERY_EVENT = 17;
    public static final int EXECUTE_LOAD_QUERY_EVENT = 18;
    public static final int TABLE_MAP_EVENT = 19;
    public static final int PRE_GA_WRITE_ROWS_EVENT = 20;
    public static final int PRE_GA_UPDATE_ROWS_EVENT = 21;
    public static final int PRE_GA_DELETE_ROWS_EVENT = 22;
    public static final int WRITE_ROWS_EVENT_V1 = 23;
    public static final int UPDATE_ROWS_EVENT_V1 = 24;
    public static final int DELETE_ROWS_EVENT_V1 = 25;
    public static final int INCIDENT_EVENT = 26;
    public static final int HEARTBEAT_LOG_EVENT = 27;
    public static final int IGNORABLE_LOG_EVENT = 28;
    public static final int ROWS_QUERY_LOG_EVENT = 29;
    public static final int WRITE_ROWS_EVENT = 30;
    public static final int UPDATE_ROWS_EVENT = 31;
    public static final int DELETE_ROWS_EVENT = 32;
    public static final int GTID_LOG_EVENT = 33;
    public static final int ANONYMOUS_GTID_LOG_EVENT = 34;
    public static final int PREVIOUS_GTIDS_LOG_EVENT = 35;
    public static final int MYSQL_EVENTS_END = 36;
    public static final int VIEW_CHANGE_EVENT = 37;
    public static final int XA_PREPARE_LOG_EVENT = 38;
    // public static final int MARIA_EVENTS_BEGIN = 160;
    // public static final int ANNOTATE_ROWS_EVENT = 160;
    // public static final int BINLOG_CHECKPOINT_EVENT = 161;
    // public static final int GTID_EVENT = 162;
    // public static final int GTID_LIST_EVENT = 163;
    // public static final int ENUM_END_EVENT = 164;

    /* checksum related */
    public static final int BINLOG_CHECKSUM_ALG_OFF = 0;
    public static final int BINLOG_CHECKSUM_ALG_CRC32 = 1;
    public static final int BINLOG_CHECKSUM_ALG_ENUM_END = 2;
    public static final int BINLOG_CHECKSUM_ALG_UNDEF = 255;

    public static final int CHECKSUM_CRC32_SIGNATURE_LEN = 4;
    public static final int BINLOG_CHECKSUM_ALG_DESC_LEN = 1;
    public static final int BINLOG_CHECKSUM_LEN = CHECKSUM_CRC32_SIGNATURE_LEN;

}
