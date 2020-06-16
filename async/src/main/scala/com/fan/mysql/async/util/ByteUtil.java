package com.fan.mysql.async.util;

@SuppressWarnings("unused")
public class ByteUtil {

    public static byte[] long2byte(long l, int len) {
        byte[] value = new byte[len];
        for (int i = 0; i < len; i++) {
            value[i] = (byte) ((l >> i * 8) & 0xff);
        }
        return value;
    }

    public static void int1store(byte[] ptr, int index, long tmp) {
        ptr[index] = (byte) (tmp & 0xff);
    }

    public static void int2store(byte[] ptr, int index, long tmp) {
        ptr[index] = (byte) (tmp & 0xff);
        ptr[index + 1] = (byte) ((tmp >> 8) & 0xff);
    }

    public static void int3store(byte[] ptr, int index, long tmp) {
        ptr[index] = (byte) (tmp & 0xff);
        ptr[index + 1] = (byte) ((tmp >> 8) & 0xff);
        ptr[index + 2] = (byte) ((tmp >> 16) & 0xff);
    }

    public static void int4store(byte[] ptr, int index, long tmp) {
        ptr[index] = (byte) (tmp & 0xff);
        ptr[index + 1] = (byte) ((tmp >> 8) & 0xff);
        ptr[index + 2] = (byte) ((tmp >> 16) & 0xff);
        ptr[index + 3] = (byte) ((tmp >> 24) & 0xff);
    }

    public static void int8store(byte[] ptr, int index, long tmp) {
        ptr[index] = (byte) (tmp & 0xff);
        ptr[index + 1] = (byte) ((tmp >> 8) & 0xff);
        ptr[index + 2] = (byte) ((tmp >> 16) & 0xff);
        ptr[index + 3] = (byte) ((tmp >> 24) & 0xff);
        ptr[index + 4] = (byte) ((tmp >> 32) & 0xff);
        ptr[index + 5] = (byte) ((tmp >> 40) & 0xff);
        ptr[index + 6] = (byte) ((tmp >> 48) & 0xff);
        ptr[index + 7] = (byte) ((tmp >> 56) & 0xff);
    }

}
