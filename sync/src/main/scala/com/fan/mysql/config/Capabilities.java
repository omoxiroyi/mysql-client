package com.fan.mysql.config;

/**
 * @author fan
 */
@SuppressWarnings("ALL")
public interface Capabilities {

    /* latin1 charset */
    public static final String CODE_PAGE_1252 = "Cp1252";
    /* new more secure passwords */
    public static final int CLIENT_LONG_PASSWORD = 1;
    /* Found instead of affected rows */
    public static final int CLIENT_FOUND_ROWS = 2;
    /* Get all column flags */
    public static final int CLIENT_LONG_FLAG = 4;
    /* One can specify db on connect */
    public static final int CLIENT_CONNECT_WITH_DB = 8;
    /* Don't allow database.table.column */
    public static final int CLIENT_NO_SCHEMA = 16;
    /* Can use compression protocol */
    public static final int CLIENT_COMPRESS = 32;
    /* Odbc client */
    public static final int CLIENT_ODBC = 64;
    /* Can use LOAD DATA LOCAL */
    public static final int CLIENT_LOCAL_FILES = 128;
    /* Ignore spaces before '(' */
    public static final int CLIENT_IGNORE_SPACE = 256;
    /* New 4.1 protocol */
    public static final int CLIENT_PROTOCOL_41 = 512;
    /* This is an interactive client */
    public static final int CLIENT_INTERACTIVE = 1024;
    /* Switch to SSL after handshake */
    public static final int CLIENT_SSL = 2048;
    /* IGNORE sigpipes */
    public static final int CLIENT_IGNORE_SIGPIPE = 4096;
    /* Client knows about transactions */
    public static final int CLIENT_TRANSACTIONS = 8192;
    /* Old flag for 4.1 protocol */
    public static final int CLIENT_RESERVED = 16384;
    /* New 4.1 authentication */
    public static final int CLIENT_SECURE_CONNECTION = 32768;
    /* Enable/disable multi-stmt support */
    public static final int CLIENT_MULTI_STATEMENTS = 65536;
    /* Enable/disable multi-results */
    public static final int CLIENT_MULTI_RESULTS = 131072;
    /*
     * Sends extra data in Initial Handshake Packet and supports the pluggable
     * authentication protocol
     */
    public static final int CLIENT_PLUGIN_AUTH = 0x00080000;
    /* Permits connection attributes */
    public static final int CLIENT_CONNECT_ATTRS = 0x00100000;
    /* Understands length-encoded integer for auth response data */
    public static final int CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000;
}
