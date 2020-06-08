package com.fan.mysql.util;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

/**
 * @author fan
 */
@SuppressWarnings("unused")
public class MySQLPacketBuffer {

    private static final Logger logger = LoggerFactory.getLogger(MySQLPacketBuffer.class);

    private static final long NULL_LENGTH = -1;
    private static final int MAX_PACKET_LENGTH = 256 * 256 * 256 - 1;

    private final int length;
    private int position;
    private final byte[] buffer;
    private String charset;

    public MySQLPacketBuffer(byte[] data) {
        this.buffer = data;
        this.length = data.length;
        this.position = 4;
    }

    public MySQLPacketBuffer(int size) {
        this.buffer = new byte[size];
        this.length = size;
        this.position = 4;
    }

    public void init(String charset) {
        this.charset = CharsetUtil.getJavaCharsetFromMysql(charset);
    }

    public void move(int i) {
        this.position += i;
    }

    public boolean hasRemaining() {
        return length > position;
    }

    public byte read() {
        return this.buffer[position++];
    }

    public byte read(int i) {
        return this.buffer[i];
    }

    public int readUB2() {
        final byte[] b = this.buffer;
        int i = b[position++] & 0xff;
        i |= (b[position++] & 0xff) << 8;
        return i;
    }

    public int readUB3() {
        final byte[] b = this.buffer;
        int i = b[position++] & 0xff;
        i |= (b[position++] & 0xff) << 8;
        i |= (b[position++] & 0xff) << 16;
        return i;
    }

    public long readUB4() {
        final byte[] b = this.buffer;
        long l = b[position++] & 0xff;
        l |= (long) (b[position++] & 0xff) << 8;
        l |= (long) (b[position++] & 0xff) << 16;
        l |= (long) (b[position++] & 0xff) << 24;
        return l;
    }

    public int readInt() {
        final byte[] b = this.buffer;
        int i = b[position++] & 0xff;
        i |= (b[position++] & 0xff) << 8;
        i |= (b[position++] & 0xff) << 16;
        i |= (b[position++] & 0xff) << 24;
        return i;
    }

    public float readFloat() {
        return Float.intBitsToFloat(readInt());
    }

    public long readLong() {
        final byte[] b = this.buffer;
        long l = b[position++] & 0xff;
        l |= (long) (b[position++] & 0xff) << 8;
        l |= (long) (b[position++] & 0xff) << 16;
        l |= (long) (b[position++] & 0xff) << 24;
        l |= (long) (b[position++] & 0xff) << 32;
        l |= (long) (b[position++] & 0xff) << 40;
        l |= (long) (b[position++] & 0xff) << 48;
        l |= (long) (b[position++] & 0xff) << 56;
        return l;
    }

    public double readDouble() {
        return Double.longBitsToDouble(readLong());
    }

    /**
     * If the value is < 251, it is stored as a 1-byte integer. If the value is
     * ≥ 251 and < (2^16), it is stored as 0xfc + 2-byte integer. If the value
     * is ≥ (2^16) and < (2^24), it is stored as 0xfd + 3-byte integer. If the
     * value is ≥ (2^24) and < (2^64) it is stored as 0xfe + 8-byte integer.
     *
     * @return length
     */
    public long readLength() {
        int length = this.buffer[position++] & 0xff;
        switch (length) {
            case 251:
                return NULL_LENGTH;
            case 252:
                return readUB2();
            case 253:
                return readUB3();
            case 254:
                return readLong();
            default:
                return length;
        }
    }

    public byte[] readBytes(int length) {
        byte[] ab = new byte[length];
        System.arraycopy(this.buffer, position, ab, 0, length);
        position += length;
        return ab;
    }

    public byte[] readBytes() {
        if (position >= length) {
            return new byte[0];
        }
        byte[] ab = new byte[length - position];
        System.arraycopy(this.buffer, position, ab, 0, ab.length);
        position = length;
        return ab;
    }

    public byte[] readBytesWithLength() {
        int length = (int) readLength();
        if (length == 0)
            return new byte[0];

        if (length == NULL_LENGTH)
            return null;

        byte[] ab = new byte[length];
        System.arraycopy(this.buffer, position, ab, 0, ab.length);
        position += length;
        return ab;
    }

    public byte[] readBytesWithNull() {
        if (position >= length) {
            return new byte[0];
        }
        int offset = -1;
        for (int i = position; i < length; i++) {
            if (this.buffer[i] == 0) {
                offset = i;
                break;
            }
        }
        byte[] ab;
        if (offset == -1) {
            ab = new byte[length - position];
            System.arraycopy(this.buffer, position, ab, 0, ab.length);
            position = length;
        } else {
            ab = new byte[offset - position];
            System.arraycopy(this.buffer, position, ab, 0, ab.length);
            position = offset + 1;
        }
        return ab;
    }

    public String readSting() {
        if (position >= length) {
            return null;
        }
        String s;
        if (this.charset != null) {
            try {
                s = new String(buffer, position, length - position, charset);
            } catch (UnsupportedEncodingException e) {
                logger.warn("original charset is :" + this.charset);
                s = new String(buffer, position, length - position);
            }
        } else {
            s = new String(buffer, position, length - position);
        }
        position = length;
        return s;
    }

    public String readStringWithNull() {
        final byte[] b = this.buffer;
        if (position >= length) {
            return null;
        }
        int offset = -1;
        for (int i = position; i < length; i++) {
            if (b[i] == 0) {
                offset = i;
                break;
            }
        }
        if (offset == -1) {
            String s;
            if (this.charset == null) {
                s = new String(b, position, length - position);
            } else {
                try {
                    s = new String(b, position, length - position, this.charset);
                } catch (UnsupportedEncodingException e) {
                    logger.warn("original charset is :" + this.charset);
                    s = new String(buffer, position, offset - position);
                }
            }
            position = length;
            return s;
        }
        if (offset > position) {
            String s;
            if (this.charset == null) {
                s = new String(b, position, offset - position);
            } else {
                try {
                    s = new String(b, position, offset - position, charset);
                } catch (UnsupportedEncodingException e) {
                    logger.warn("original charset is :" + this.charset);
                    s = new String(buffer, position, offset - position);
                }
            }
            position = offset + 1;
            return s;
        } else {
            position++;
            return null;
        }
    }

    public String readLengthString(int len) {
        String s;
        if (this.charset == null) {
            s = new String(buffer, position, len);
        } else {
            try {
                s = new String(buffer, position, len, charset);
            } catch (UnsupportedEncodingException e) {
                logger.warn("original charset is :" + this.charset);
                s = new String(buffer, position, len);
            }
        }
        this.position += len;
        return s;
    }

    public String readStringWithLength() {
        int len = (int) readLength();
        if (len <= 0)
            return null;
        return readLengthString(len);
    }

    public void write(byte b) {
        this.buffer[position++] = b;
    }

    public void writeUB2(int i) {
        byte[] b = this.buffer;
        b[this.position++] = (byte) (i & 0xff);
        b[this.position++] = (byte) (i >>> 8);
    }

    public void writeUB3(int i) {
        byte[] b = this.buffer;
        b[this.position++] = (byte) (i & 0xff);
        b[this.position++] = (byte) (i >>> 8);
        b[this.position++] = (byte) (i >>> 16);
    }

    public void writeUB4(long l) {
        byte[] b = this.buffer;
        b[this.position++] = (byte) (l & 0xff);
        b[this.position++] = (byte) (l >>> 8);
        b[this.position++] = (byte) (l >>> 16);
        b[this.position++] = (byte) (l >>> 24);
    }

    public void writeInt(int i) {
        byte[] b = this.buffer;
        b[this.position++] = (byte) (i & 0xff);
        b[this.position++] = (byte) (i >>> 8);
        b[this.position++] = (byte) (i >>> 16);
        b[this.position++] = (byte) (i >>> 24);
    }

    public void writeFloat(float f) {
        writeInt(Float.floatToIntBits(f));
    }

    public void writeLong(long l) {
        byte[] b = this.buffer;
        b[this.position++] = (byte) (l & 0xff);
        b[this.position++] = (byte) (l >>> 8);
        b[this.position++] = (byte) (l >>> 16);
        b[this.position++] = (byte) (l >>> 24);
        b[this.position++] = (byte) (l >>> 32);
        b[this.position++] = (byte) (l >>> 40);
        b[this.position++] = (byte) (l >>> 48);
        b[this.position++] = (byte) (l >>> 56);
    }

    public void writeDouble(double d) {
        writeLong(Double.doubleToLongBits(d));
    }

    public void writeLength(long len) {
        if (len < 251) {
            write((byte) len);
        } else if (len < 0x10000L) {
            write((byte) 252);
            writeUB2((int) len);
        } else if (len < 0x1000000L) {
            write((byte) 253);
            writeUB3((int) len);
        } else {
            write((byte) 254);
            writeLong(len);
        }
    }

    public void writeBytesNoNull(byte[] b) {
        int len = b.length;
        System.arraycopy(b, 0, this.buffer, this.position, len);
        this.position += len;
    }

    public void writeBytesWithLength(byte[] bytes, byte nullValue) {
        if (bytes == null) {
            write(nullValue);
        } else {
            writeBytesWithLength(bytes);
        }
    }

    public void writeBytesWithLength(byte[] bytes) {
        if (bytes == null) {
            this.buffer[position++] = 0;
        } else {
            int len = bytes.length;
            writeLength(len);
            System.arraycopy(bytes, 0, this.buffer, this.position, len);
            this.position += len;
        }
    }

    public void writeStringWithNull(String s) {
        if (s != null) {
            writeStringNoNull(s);
        }
        this.buffer[position++] = (byte) 0;
    }

    public void writeStringNoNull(String s) {
        int len = 0;
        try {
            len = (this.charset == null ? s.getBytes().length : s.getBytes(this.charset).length);
            System.arraycopy(this.charset == null ? s.getBytes() : s.getBytes(this.charset), 0, this.buffer,
                    this.position, len);
        } catch (UnsupportedEncodingException e) {
            logger.warn("original charset is :" + this.charset);
        }
        this.position += len;
    }

    public void writeStringWithLength(String s, byte nullValue) {
        if (s == null) {
            write(nullValue);
        } else {
            writeStringWithLength(s);
        }
    }

    public void writeStringWithLength(String s) {
        int len = 0;
        try {
            len = (this.charset == null ? s.getBytes().length : s.getBytes(this.charset).length);
        } catch (UnsupportedEncodingException e) {
            logger.warn("original charset is :" + this.charset);
        }
        writeLength(len);
        writeStringNoNull(s);
    }

    // buffer flip() invoked by write
    public ByteBuffer toByteBuffer() {
        int position = this.position;
        ByteBuffer byteBuffer = ByteBuffer.allocate(position);
        byteBuffer.put(this.buffer, 0, position);
        return byteBuffer;
    }

    public ByteBuffer multiToByteBuffer() {
        int position = this.position;
        if (position - 4 > MAX_PACKET_LENGTH) {
            return multiPacketBuffer();
        } else {
            return toByteBuffer();
        }
    }

    private ByteBuffer multiPacketBuffer() {
        int totalLen = this.position;
        totalLen += 4 * ((this.position - 4) / MAX_PACKET_LENGTH);
        ByteBuffer byteBuffer = ByteBuffer.allocate(totalLen);
        int len = this.position - 4;
        int index = 4;
        int packetId = buffer[3];
        do {
            int tempLen = Math.min(len, MAX_PACKET_LENGTH);
            byte[] temp = new byte[tempLen + 4];
            // skip header
            System.arraycopy(this.buffer, index, temp, 4, tempLen);
            setPacketLength(temp, tempLen, packetId);
            byteBuffer.put(temp, 0, temp.length);
            packetId++;
            index += MAX_PACKET_LENGTH;
            len -= MAX_PACKET_LENGTH;
        } while (len >= 0);
        return byteBuffer;
    }

    private void setPacketLength(byte[] data, int len, int packetId) {
        data[0] = (byte) (len & 0xff);
        data[1] = (byte) (len >>> 8);
        data[2] = (byte) (len >>> 16);
        data[3] = (byte) (packetId & 0xff);
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public String getCharset() {
        return this.charset;
    }

    public static int getLength(long length) {
        if (length < 251) {
            return 1;
        } else if (length < 0x10000L) {
            return 3;
        } else if (length < 0x1000000L) {
            return 4;
        } else {
            return 9;
        }
    }

    public static int getLength(byte[] src) {
        int length = src.length;
        if (length < 251) {
            return 1 + length;
        } else if (length < 0x10000L) {
            return 3 + length;
        } else if (length < 0x1000000L) {
            return 4 + length;
        } else {
            return 9 + length;
        }
    }

}
