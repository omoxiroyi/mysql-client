package com.dingcloud.dts.binlog.mysql.dbsync;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.BitSet;

public class LogBuffer {

	public static LogBuffer EOF = new LogBuffer();

	protected byte[] buffer;

	protected int limit;
	protected int position;

	protected LogBuffer() {
	}

	public LogBuffer(byte[] buffer, final int origin, final int limit) {
		if (origin + limit > buffer.length)
			throw new IllegalArgumentException("capacity excceed: " + (origin + limit));

		this.buffer = buffer;
		this.position = origin;
		this.limit = limit;
	}

	/**
	 * Return n bytes in this buffer.
	 */
	public final LogBuffer duplicate(final int pos, final int len) {
		if (pos + len > limit)
			throw new IllegalArgumentException("limit excceed: " + (pos + len));

		// XXX: Do momery copy avoid buffer modified.
		final int off = pos;
		byte[] buf = Arrays.copyOfRange(buffer, off, off + len);
		return new LogBuffer(buf, 0, len);
	}

	public final int position() {
		return position;
	}

	public final void position(final int newPosition) {
		if (newPosition > limit || newPosition < 0)
			throw new IllegalArgumentException("limit excceed: " + newPosition);

		this.position = newPosition;
	}

	/**
	 * Forwards this buffer's position.
	 * 
	 * @param len
	 *            The forward distance
	 * @return This buffer
	 */
	public final LogBuffer forward(final int len) {
		if (position + len > limit)
			throw new IllegalArgumentException("limit excceed: " + (position + len));

		this.position += len;
		return this;
	}

	/**
	 * Rewinds this buffer. The position is set to zero.
	 * 
	 * @return This buffer
	 */
	public final void rewind() {
		position = 0;
	}

	/**
	 * Returns this buffer's limit.
	 * </p>
	 * 
	 * @return The limit of this buffer
	 */
	public final int limit() {
		return limit;
	}

	public final void limit(int newLimit) {
		if (newLimit > buffer.length || newLimit < 0)
			throw new IllegalArgumentException("capacity excceed: " + newLimit);

		limit = newLimit;
	}

	/**
	 * Returns the number of elements between the current position and the limit.
	 * </p>
	 * 
	 * @return The number of elements remaining in this buffer
	 */
	public final int remaining() {
		return limit - position;
	}

	/**
	 * Tells whether there are any elements between the current position and the
	 * limit.
	 * </p>
	 * 
	 * @return <tt>true</tt> if, and only if, there is at least one element
	 *         remaining in this buffer
	 */
	public final boolean hasRemaining() {
		return position < limit;
	}

	/**
	 * Return 8-bit signed int from buffer.
	 */
	public final int getInt8(final int pos) {
		if (pos >= limit || pos < 0)
			throw new IllegalArgumentException("limit excceed: " + pos);

		return buffer[pos];
	}

	/**
	 * Return next 8-bit signed int from buffer.
	 */
	public final int getInt8() {
		if (position >= limit)
			throw new IllegalArgumentException("limit excceed: " + position);

		return buffer[position++];
	}

	/**
	 * Return 8-bit unsigned int from buffer.
	 */
	public final int getUint8(final int pos) {
		if (pos >= limit || pos < 0)
			throw new IllegalArgumentException("limit excceed: " + pos);

		return 0xff & buffer[pos];
	}

	/**
	 * Return next 8-bit unsigned int from buffer.
	 */
	public final int getUint8() {
		if (position >= limit)
			throw new IllegalArgumentException("limit excceed: " + position);

		return 0xff & buffer[position++];
	}

	/**
	 * Return 16-bit signed int from buffer. (little-endian)
	 * 
	 * @see mysql-5.1.60/include/my_global.h - sint2korr
	 */
	public final int getInt16(final int pos) {
		if (pos + 1 >= limit || pos < 0)
			throw new IllegalArgumentException("limit excceed: " + pos);

		byte[] buf = buffer;
		return (0xff & buf[pos]) | ((buf[pos + 1]) << 8);
	}

	/**
	 * Return next 16-bit signed int from buffer. (little-endian)
	 * 
	 * @see mysql-5.1.60/include/my_global.h - sint2korr
	 */
	public final int getInt16() {
		if (position + 1 >= limit)
			throw new IllegalArgumentException("limit excceed: " + (position + 1));

		byte[] buf = buffer;
		return (0xff & buf[position++]) | ((buf[position++]) << 8);
	}

	/**
	 * Return 16-bit unsigned int from buffer. (little-endian)
	 * 
	 * @see mysql-5.1.60/include/my_global.h - uint2korr
	 */
	public final int getUint16(final int pos) {
		if (pos + 1 >= limit || pos < 0)
			throw new IllegalArgumentException("limit excceed: " + pos);

		byte[] buf = buffer;
		return (0xff & buf[pos]) | ((0xff & buf[pos + 1]) << 8);
	}

	/**
	 * Return next 16-bit unsigned int from buffer. (little-endian)
	 * 
	 * @see mysql-5.1.60/include/my_global.h - uint2korr
	 */
	public final int getUint16() {
		if (position + 1 >= limit)
			throw new IllegalArgumentException("limit excceed: " + (position - 1));

		byte[] buf = buffer;
		return (0xff & buf[position++]) | ((0xff & buf[position++]) << 8);
	}

	/**
	 * Return 16-bit signed int from buffer. (big-endian)
	 * 
	 * @see mysql-5.6.10/include/myisampack.h - mi_sint2korr
	 */
	public final int getBeInt16(final int pos) {
		if (pos + 1 >= limit || pos < 0)
			throw new IllegalArgumentException("limit excceed: " + (pos < 0 ? pos : (pos + 1)));

		byte[] buf = buffer;
		return (0xff & buf[pos + 1]) | ((buf[pos]) << 8);
	}

	/**
	 * Return next 16-bit signed int from buffer. (big-endian)
	 * 
	 * @see mysql-5.1.60/include/my_global.h - mi_sint2korr
	 */
	public final int getBeInt16() {
		if (position + 1 >= limit)
			throw new IllegalArgumentException("limit excceed: " + (position + 1));

		byte[] buf = buffer;
		return (buf[position++] << 8) | (0xff & buf[position++]);
	}

	/**
	 * Return next 16-bit unsigned int from buffer. (big-endian)
	 * 
	 * @see mysql-5.6.10/include/myisampack.h - mi_usint2korr
	 */
	public final int getBeUint16() {
		if (position + 1 >= limit)
			throw new IllegalArgumentException("limit excceed: " + (position + 1));

		byte[] buf = buffer;
		return ((0xff & buf[position++]) << 8) | (0xff & buf[position++]);
	}

	/**
	 * Return next 24-bit signed int from buffer. (little-endian)
	 * 
	 * @see mysql-5.1.60/include/my_global.h - sint3korr
	 */
	public final int getInt24() {
		if (position + 2 >= limit)
			throw new IllegalArgumentException("limit excceed: " + (position + 2));

		byte[] buf = buffer;
		return (0xff & buf[position++]) | ((0xff & buf[position++]) << 8) | ((buf[position++]) << 16);
	}

	/**
	 * Return next 24-bit signed int from buffer. (big-endian)
	 * 
	 * @see mysql-5.6.10/include/myisampack.h - mi_usint3korr
	 */
	public final int getBeInt24() {
		if (position + 2 >= limit)
			throw new IllegalArgumentException("limit excceed: " + (position + 2));

		byte[] buf = buffer;
		return ((buf[position++]) << 16) | ((0xff & buf[position++]) << 8) | (0xff & buf[position++]);
	}

	/**
	 * Return 24-bit unsigned int from buffer. (little-endian)
	 * 
	 * @see mysql-5.1.60/include/my_global.h - uint3korr
	 */
	public final int getUint24(final int pos) {
		if (pos + 2 >= limit || pos < 0)
			throw new IllegalArgumentException("limit excceed: " + pos);

		byte[] buf = buffer;
		return (0xff & buf[pos]) | ((0xff & buf[pos + 1]) << 8) | ((0xff & buf[pos + 2]) << 16);
	}

	/**
	 * Return next 24-bit unsigned int from buffer. (little-endian)
	 * 
	 * @see mysql-5.1.60/include/my_global.h - uint3korr
	 */
	public final int getUint24() {
		if (position + 2 >= limit)
			throw new IllegalArgumentException("limit excceed: " + (position + 2));

		byte[] buf = buffer;
		return (0xff & buf[position++]) | ((0xff & buf[position++]) << 8) | ((0xff & buf[position++]) << 16);
	}

	/**
	 * Return next 24-bit unsigned int from buffer. (big-endian)
	 * 
	 * @see mysql-5.6.10/include/myisampack.h - mi_usint3korr
	 */
	public final int getBeUint24() {
		if (position + 2 >= limit)
			throw new IllegalArgumentException("limit excceed: " + (position + 2));

		byte[] buf = buffer;
		return ((0xff & buf[position++]) << 16) | ((0xff & buf[position++]) << 8) | (0xff & buf[position++]);
	}

	/**
	 * Return 32-bit signed int from buffer. (little-endian)
	 * 
	 * @see mysql-5.1.60/include/my_global.h - sint4korr
	 */
	public final int getInt32(final int pos) {
		if (pos + 3 >= limit || pos < 0)
			throw new IllegalArgumentException("limit excceed: " + pos);

		byte[] buf = buffer;
		return (0xff & buf[pos]) | ((0xff & buf[pos + 1]) << 8) | ((0xff & buf[pos + 2]) << 16)
				| ((buf[pos + 3]) << 24);
	}

	/**
	 * Return next 32-bit signed int from buffer. (little-endian)
	 * 
	 * @see mysql-5.1.60/include/my_global.h - sint4korr
	 */
	public final int getInt32() {
		if (position + 3 >= limit)
			throw new IllegalArgumentException("limit excceed: " + (position + 3));

		byte[] buf = buffer;
		return (0xff & buf[position++]) | ((0xff & buf[position++]) << 8) | ((0xff & buf[position++]) << 16)
				| ((buf[position++]) << 24);
	}

	/**
	 * Return 32-bit unsigned int from buffer. (little-endian)
	 * 
	 * @see mysql-5.1.60/include/my_global.h - uint4korr
	 */
	public final long getUint32(final int pos) {
		if (pos + 3 >= limit || pos < 0)
			throw new IllegalArgumentException("limit excceed: " + pos);

		byte[] buf = buffer;
		return ((long) (0xff & buf[pos])) | ((long) (0xff & buf[pos + 1]) << 8) | ((long) (0xff & buf[pos + 2]) << 16)
				| ((long) (0xff & buf[pos + 3]) << 24);
	}

	/**
	 * Return next 32-bit unsigned int from buffer. (little-endian)
	 * 
	 * @see mysql-5.1.60/include/my_global.h - uint4korr
	 */
	public final long getUint32() {
		if (position + 3 >= limit)
			throw new IllegalArgumentException("limit excceed: " + (position + 3));

		byte[] buf = buffer;
		return ((long) (0xff & buf[position++])) | ((long) (0xff & buf[position++]) << 8)
				| ((long) (0xff & buf[position++]) << 16) | ((long) (0xff & buf[position++]) << 24);
	}

	/**
	 * Return next 32-bit unsigned int from buffer. (big-endian)
	 * 
	 * @see mysql-5.6.10/include/myisampack.h - mi_uint4korr
	 */
	public final long getBeUint32() {
		if (position + 3 >= limit)
			throw new IllegalArgumentException("limit excceed: " + (position + 3));

		byte[] buf = buffer;
		return ((long) (0xff & buf[position++]) << 24) | ((long) (0xff & buf[position++]) << 16)
				| ((long) (0xff & buf[position++]) << 8) | ((long) (0xff & buf[position++]));
	}

	/**
	 * Return next 40-bit unsigned int from buffer. (big-endian)
	 * 
	 * @see mysql-5.6.10/include/myisampack.h - mi_uint5korr
	 */
	public final long getBeUlong40() {
		if (position + 4 >= limit)
			throw new IllegalArgumentException("limit excceed: " + (position + 4));

		byte[] buf = buffer;
		return ((long) (0xff & buf[position++]) << 32) | ((long) (0xff & buf[position++]) << 24)
				| ((long) (0xff & buf[position++]) << 16) | ((long) (0xff & buf[position++]) << 8)
				| ((long) (0xff & buf[position++]));
	}

	/**
	 * Return next 48-bit unsigned long from buffer. (little-endian)
	 * 
	 * @see mysql-5.1.60/include/my_global.h - uint6korr
	 */
	public final long getUlong48() {
		if (position + 5 >= limit)
			throw new IllegalArgumentException("limit excceed: " + (position + 5));

		byte[] buf = buffer;
		return ((long) (0xff & buf[position++])) | ((long) (0xff & buf[position++]) << 8)
				| ((long) (0xff & buf[position++]) << 16) | ((long) (0xff & buf[position++]) << 24)
				| ((long) (0xff & buf[position++]) << 32) | ((long) (0xff & buf[position++]) << 40);
	}

	/**
	 * Return next 48-bit unsigned long from buffer. (big-endian)
	 * 
	 * @see mysql-5.6.10/include/myisampack.h - mi_uint6korr
	 */
	public final long getBeUlong48() {
		if (position + 5 >= limit)
			throw new IllegalArgumentException("limit excceed: " + (position + 5));

		byte[] buf = buffer;
		return ((long) (0xff & buf[position++]) << 40) | ((long) (0xff & buf[position++]) << 32)
				| ((long) (0xff & buf[position++]) << 24) | ((long) (0xff & buf[position++]) << 16)
				| ((long) (0xff & buf[position++]) << 8) | ((long) (0xff & buf[position++]));
	}

	/**
	 * Return 64-bit signed long from buffer. (little-endian)
	 * 
	 * @see mysql-5.1.60/include/my_global.h - sint8korr
	 */
	public final long getLong64(final int pos) {
		if (pos + 7 >= limit || pos < 0)
			throw new IllegalArgumentException("limit excceed: " + pos);

		byte[] buf = buffer;
		return ((long) (0xff & buf[pos])) | ((long) (0xff & buf[pos + 1]) << 8) | ((long) (0xff & buf[pos + 2]) << 16)
				| ((long) (0xff & buf[pos + 3]) << 24) | ((long) (0xff & buf[pos + 4]) << 32)
				| ((long) (0xff & buf[pos + 5]) << 40) | ((long) (0xff & buf[pos + 6]) << 48)
				| ((long) (buf[pos + 7]) << 56);
	}

	/**
	 * Return 64-bit signed long from buffer. (big-endian)
	 * 
	 * @see mysql-5.6.10/include/myisampack.h - mi_sint8korr
	 */
	public final long getBeLong64(final int pos) {
		if (pos + 7 >= limit || pos < 0)
			throw new IllegalArgumentException("limit excceed: " + pos);

		byte[] buf = buffer;
		return ((long) (0xff & buf[pos + 7])) | ((long) (0xff & buf[pos + 6]) << 8)
				| ((long) (0xff & buf[pos + 5]) << 16) | ((long) (0xff & buf[pos + 4]) << 24)
				| ((long) (0xff & buf[pos + 3]) << 32) | ((long) (0xff & buf[pos + 2]) << 40)
				| ((long) (0xff & buf[pos + 1]) << 48) | ((long) (buf[pos]) << 56);
	}

	/**
	 * Return next 64-bit signed long from buffer. (little-endian)
	 * 
	 * @see mysql-5.1.60/include/my_global.h - sint8korr
	 */
	public final long getLong64() {
		if (position + 7 >= limit)
			throw new IllegalArgumentException("limit excceed: " + (position + 7));

		byte[] buf = buffer;
		return ((long) (0xff & buf[position++])) | ((long) (0xff & buf[position++]) << 8)
				| ((long) (0xff & buf[position++]) << 16) | ((long) (0xff & buf[position++]) << 24)
				| ((long) (0xff & buf[position++]) << 32) | ((long) (0xff & buf[position++]) << 40)
				| ((long) (0xff & buf[position++]) << 48) | ((long) (buf[position++]) << 56);
	}

	/**
	 * Return next 64-bit signed long from buffer. (big-endian)
	 * 
	 * @see mysql-5.6.10/include/myisampack.h - mi_sint8korr
	 */
	public final long getBeLong64() {
		if (position + 7 >= limit)
			throw new IllegalArgumentException("limit excceed: " + (position + 7));

		byte[] buf = buffer;
		return ((long) (buf[position++]) << 56) | ((long) (0xff & buf[position++]) << 48)
				| ((long) (0xff & buf[position++]) << 40) | ((long) (0xff & buf[position++]) << 32)
				| ((long) (0xff & buf[position++]) << 24) | ((long) (0xff & buf[position++]) << 16)
				| ((long) (0xff & buf[position++]) << 8) | ((long) (0xff & buf[position++]));
	}

	/* The max ulonglong - 0x ff ff ff ff ff ff ff ff */
	public static final BigInteger BIGINT_MAX_VALUE = new BigInteger("18446744073709551615");

	/**
	 * Return 64-bit unsigned long from buffer. (little-endian)
	 * 
	 * @see mysql-5.1.60/include/my_global.h - uint8korr
	 */
	public final BigInteger getUlong64(final int pos) {
		final long long64 = getLong64(pos);

		return (long64 >= 0) ? BigInteger.valueOf(long64) : BIGINT_MAX_VALUE.add(BigInteger.valueOf(1 + long64));
	}

	/**
	 * Return 64-bit unsigned long from buffer. (big-endian)
	 * 
	 * @see mysql-5.6.10/include/myisampack.h - mi_uint8korr
	 */
	public final BigInteger getBeUlong64(final int pos) {
		final long long64 = getBeLong64(pos);

		return (long64 >= 0) ? BigInteger.valueOf(long64) : BIGINT_MAX_VALUE.add(BigInteger.valueOf(1 + long64));
	}

	/**
	 * Return next 64-bit unsigned long from buffer. (little-endian)
	 * 
	 * @see mysql-5.1.60/include/my_global.h - uint8korr
	 */
	public final BigInteger getUlong64() {
		final long long64 = getLong64();

		return (long64 >= 0) ? BigInteger.valueOf(long64) : BIGINT_MAX_VALUE.add(BigInteger.valueOf(1 + long64));
	}

	/**
	 * Return next 64-bit unsigned long from buffer. (big-endian)
	 * 
	 * @see mysql-5.6.10/include/myisampack.h - mi_uint8korr
	 */
	public final BigInteger getBeUlong64() {
		final long long64 = getBeLong64();

		return (long64 >= 0) ? BigInteger.valueOf(long64) : BIGINT_MAX_VALUE.add(BigInteger.valueOf(1 + long64));
	}

	/**
	 * Return 32-bit float from buffer. (little-endian)
	 * 
	 * @see mysql-5.1.60/include/my_global.h - float4get
	 */
	public final float getFloat32(final int pos) {
		return Float.intBitsToFloat(getInt32(pos));
	}

	/**
	 * Return next 32-bit float from buffer. (little-endian)
	 * 
	 * @see mysql-5.1.60/include/my_global.h - float4get
	 */
	public final float getFloat32() {
		return Float.intBitsToFloat(getInt32());
	}

	/**
	 * Return 64-bit double from buffer. (little-endian)
	 * 
	 * @see mysql-5.1.60/include/my_global.h - float8get
	 */
	public final double getDouble64(final int pos) {
		return Double.longBitsToDouble(getLong64(pos));
	}

	/**
	 * Return next 64-bit double from buffer. (little-endian)
	 * 
	 * @see mysql-5.1.60/include/my_global.h - float8get
	 */
	public final double getDouble64() {
		return Double.longBitsToDouble(getLong64());
	}

	public static final long NULL_LENGTH = ((long) ~0);

	/**
	 * Return packed number from buffer. (little-endian) A Packed Integer has the
	 * capacity of storing up to 8-byte integers, while small integers still can use
	 * 1, 3, or 4 bytes. The value of the first byte determines how to read the
	 * number, according to the following table.
	 * <ul>
	 * <li>0-250 The first byte is the number (in the range 0-250). No additional
	 * bytes are used.</li>
	 * <li>252 Two more bytes are used. The number is in the range 251-0xffff.</li>
	 * <li>253 Three more bytes are used. The number is in the range
	 * 0xffff-0xffffff.</li>
	 * <li>254 Eight more bytes are used. The number is in the range
	 * 0xffffff-0xffffffffffffffff.</li>
	 * </ul>
	 * That representation allows a first byte value of 251 to represent the SQL
	 * NULL value.
	 */
	public final long getPackedLong(final int pos) {
		final int lead = getUint8(pos);
		if (lead < 251)
			return lead;

		switch (lead) {
		case 251:
			return NULL_LENGTH;
		case 252:
			return getUint16(pos + 1);
		case 253:
			return getUint24(pos + 1);
		default: /* Must be 254 when here */
			return getUint32(pos + 1);
		}
	}

	/**
	 * Return next packed number from buffer. (little-endian)
	 * 
	 * @see LogBuffer#getPackedLong(int)
	 */
	public final long getPackedLong() {
		final int lead = getUint8();
		if (lead < 251)
			return lead;

		switch (lead) {
		case 251:
			return NULL_LENGTH;
		case 252:
			return getUint16();
		case 253:
			return getUint24();
		default: /* Must be 254 when here */
			final long value = getUint32();
			position += 4; /* ignore other */
			return value;
		}
	}

	/* default ANSI charset */
	public static final String ISO_8859_1 = "ISO-8859-1";

	/**
	 * Return fix length string from buffer.
	 */
	public final String getFixString(final int pos, final int len) {
		return getFixString(pos, len, ISO_8859_1);
	}

	/**
	 * Return next fix length string from buffer.
	 */
	public final String getFixString(final int len) {
		return getFixString(len, ISO_8859_1);
	}

	/**
	 * Return fix length string from buffer.
	 */
	public final String getFixString(final int pos, final int len, String charsetName) {
		if (pos + len > limit || pos < 0)
			throw new IllegalArgumentException("limit excceed: " + (pos < 0 ? pos : (pos + len)));

		final int from = pos;
		final int end = from + len;
		byte[] buf = buffer;
		int found = from;
		for (; (found < end) && buf[found] != '\0'; found++)
			/* empty loop */;

		try {
			return new String(buf, from, found - from, charsetName);
		} catch (UnsupportedEncodingException e) {
			throw new IllegalArgumentException("Unsupported encoding: " + charsetName, e);
		}
	}

	/**
	 * Return next fix length string from buffer.
	 */
	public final String getFixString(final int len, String charsetName) {
		if (position + len > limit)
			throw new IllegalArgumentException("limit excceed: " + (position + len));

		final int from = position;
		final int end = from + len;
		byte[] buf = buffer;
		int found = from;
		for (; (found < end) && buf[found] != '\0'; found++)
			/* empty loop */;

		try {
			String string = new String(buf, from, found - from, charsetName);
			position += len;
			return string;
		} catch (UnsupportedEncodingException e) {
			throw new IllegalArgumentException("Unsupported encoding: " + charsetName, e);
		}
	}

	/**
	 * Return dynamic length string from buffer.
	 */
	public final String getString(final int pos) {
		return getString(pos, ISO_8859_1);
	}

	/**
	 * Return next dynamic length string from buffer.
	 */
	public final String getString() {
		return getString(ISO_8859_1);
	}

	/**
	 * Return dynamic length string from buffer.
	 */
	public final String getString(final int pos, String charsetName) {
		if (pos >= limit || pos < 0)
			throw new IllegalArgumentException("limit excceed: " + pos);

		byte[] buf = buffer;
		final int len = (0xff & buf[pos]);
		if (pos + len + 1 > limit)
			throw new IllegalArgumentException("limit excceed: " + (pos + len + 1));

		try {
			return new String(buf, pos + 1, len, charsetName);
		} catch (UnsupportedEncodingException e) {
			throw new IllegalArgumentException("Unsupported encoding: " + charsetName, e);
		}
	}

	/**
	 * Return next dynamic length string from buffer.
	 */
	public final String getString(String charsetName) {
		if (position >= limit)
			throw new IllegalArgumentException("limit excceed: " + position);

		byte[] buf = buffer;
		final int len = (0xff & buf[position]);
		if (position + len + 1 > limit)
			throw new IllegalArgumentException("limit excceed: " + (position + len + 1));

		try {
			String string = new String(buf, position + 1, len, charsetName);
			position += len + 1;
			return string;
		} catch (UnsupportedEncodingException e) {
			throw new IllegalArgumentException("Unsupported encoding: " + charsetName, e);
		}
	}

	public String getZeroTerminatedString() {
		int originPos = position;
		byte[] buf = buffer;
		while (buf[position] != 0) {
			position++;
		}
		return new String(buf, originPos, position++ - originPos);
	}

	/**
	 * Return 16-bit signed int from buffer. (big-endian)
	 * 
	 * @see mysql-5.1.60/include/myisampack.h - mi_sint2korr
	 */
	private static final int getInt16BE(byte[] buffer, final int pos) {
		return ((buffer[pos]) << 8) | (0xff & buffer[pos + 1]);
	}

	/**
	 * Return 24-bit signed int from buffer. (big-endian)
	 * 
	 * @see mysql-5.1.60/include/myisampack.h - mi_sint3korr
	 */
	private static final int getInt24BE(byte[] buffer, final int pos) {
		return (buffer[pos] << 16) | ((0xff & buffer[pos + 1]) << 8) | (0xff & buffer[pos + 2]);
	}

	/**
	 * Return 32-bit signed int from buffer. (big-endian)
	 * 
	 * @see mysql-5.1.60/include/myisampack.h - mi_sint4korr
	 */
	private static final int getInt32BE(byte[] buffer, final int pos) {
		return (buffer[pos] << 24) | ((0xff & buffer[pos + 1]) << 16) | ((0xff & buffer[pos + 2]) << 8)
				| (0xff & buffer[pos + 3]);
	}

	/* decimal representation */
	public static final int DIG_PER_DEC1 = 9;
	public static final int DIG_BASE = 1000000000;
	public static final int DIG_MAX = DIG_BASE - 1;
	public static final int dig2bytes[] = { 0, 1, 1, 2, 2, 3, 3, 4, 4, 4 };
	public static final int powers10[] = { 1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000 };

	public static final int DIG_PER_INT32 = 9;
	public static final int SIZE_OF_INT32 = 4;

	/**
	 * Return next big decimal from buffer.
	 * 
	 * @see mysql-5.1.60/strings/decimal.c - bin2decimal()
	 */
	public final BigDecimal getDecimal(final int precision, final int scale) {
		final int intg = precision - scale;
		final int frac = scale;
		final int intg0 = intg / DIG_PER_INT32;
		final int frac0 = frac / DIG_PER_INT32;
		final int intg0x = intg - intg0 * DIG_PER_INT32;
		final int frac0x = frac - frac0 * DIG_PER_INT32;

		final int binSize = intg0 * SIZE_OF_INT32 + dig2bytes[intg0x] + frac0 * SIZE_OF_INT32 + dig2bytes[frac0x];
		if (position + binSize > limit) {
			throw new IllegalArgumentException("limit excceed: " + (position + binSize));
		}

		BigDecimal decimal = getDecimal0(position, intg, frac, // NL
				intg0, frac0, intg0x, frac0x);
		position += binSize;
		return decimal;
	}

	/**
	 * Return big decimal from buffer.
	 * 
	 * <pre>
	 * Decimal representation in binlog seems to be as follows:
	 * 
	 * 1st bit - sign such that set == +, unset == -
	 * every 4 bytes represent 9 digits in big-endian order, so that
	 * if you print the values of these quads as big-endian integers one after
	 * another, you get the whole number string representation in decimal. What
	 * remains is to put a sign and a decimal dot.
	 * 
	 * 80 00 00 05 1b 38 b0 60 00 means:
	 * 
	 *   0x80 - positive 
	 *   0x00000005 - 5
	 *   0x1b38b060 - 456700000
	 *   0x00       - 0
	 * 
	 * 54567000000 / 10^{10} = 5.4567
	 * </pre>
	 * 
	 * @see mysql-5.1.60/strings/decimal.c - bin2decimal()
	 * @see mysql-5.1.60/strings/decimal.c - decimal2string()
	 */
	private final BigDecimal getDecimal0(final int begin, final int intg, final int frac, final int intg0,
			final int frac0, final int intg0x, final int frac0x) {
		final int mask = ((buffer[begin] & 0x80) == 0x80) ? 0 : -1;
		int from = begin;

		/* max string length */
		final int len = ((mask != 0) ? 1 : 0) + ((intg != 0) ? intg : 1) // NL
				+ ((frac != 0) ? 1 : 0) + frac;
		char[] buf = new char[len];
		int pos = 0;

		if (mask != 0) /* decimal sign */
			buf[pos++] = ('-');

		final byte[] d_copy = buffer;
		d_copy[begin] ^= 0x80; /* clear sign */
		int mark = pos;

		if (intg0x != 0) {
			final int i = dig2bytes[intg0x];
			int x = 0;
			switch (i) {
			case 1:
				x = d_copy[from] /* one byte */;
				break;
			case 2:
				x = getInt16BE(d_copy, from);
				break;
			case 3:
				x = getInt24BE(d_copy, from);
				break;
			case 4:
				x = getInt32BE(d_copy, from);
				break;
			}
			from += i;
			x ^= mask;
			if (x < 0 || x >= powers10[intg0x + 1]) {
				throw new IllegalArgumentException("bad format, x exceed: " + x + ", " + powers10[intg0x + 1]);
			}
			if (x != 0 /* !digit || x != 0 */) {
				for (int j = intg0x; j > 0; j--) {
					final int divisor = powers10[j - 1];
					final int y = x / divisor;
					if (mark < pos || y != 0) {
						buf[pos++] = ((char) ('0' + y));
					}
					x -= y * divisor;
				}
			}
		}

		for (final int stop = from + intg0 * SIZE_OF_INT32; from < stop; from += SIZE_OF_INT32) {
			int x = getInt32BE(d_copy, from);
			x ^= mask;
			if (x < 0 || x > DIG_MAX) {
				throw new IllegalArgumentException("bad format, x exceed: " + x + ", " + DIG_MAX);
			}
			if (x != 0) {
				if (mark < pos) {
					for (int i = DIG_PER_DEC1; i > 0; i--) {
						final int divisor = powers10[i - 1];
						final int y = x / divisor;
						buf[pos++] = ((char) ('0' + y));
						x -= y * divisor;
					}
				} else {
					for (int i = DIG_PER_DEC1; i > 0; i--) {
						final int divisor = powers10[i - 1];
						final int y = x / divisor;
						if (mark < pos || y != 0) {
							buf[pos++] = ((char) ('0' + y));
						}
						x -= y * divisor;
					}
				}
			} else if (mark < pos) {
				for (int i = DIG_PER_DEC1; i > 0; i--)
					buf[pos++] = ('0');
			}
		}

		if (mark == pos)
			/*
			 * fix 0.0 problem, only '.' may cause BigDecimal parsing exception.
			 */
			buf[pos++] = ('0');

		if (frac > 0) {
			buf[pos++] = ('.');
			mark = pos;

			for (final int stop = from + frac0 * SIZE_OF_INT32; from < stop; from += SIZE_OF_INT32) {
				int x = getInt32BE(d_copy, from);
				x ^= mask;
				if (x < 0 || x > DIG_MAX) {
					throw new IllegalArgumentException("bad format, x exceed: " + x + ", " + DIG_MAX);
				}
				if (x != 0) {
					for (int i = DIG_PER_DEC1; i > 0; i--) {
						final int divisor = powers10[i - 1];
						final int y = x / divisor;
						buf[pos++] = ((char) ('0' + y));
						x -= y * divisor;
					}
				} else {
					for (int i = DIG_PER_DEC1; i > 0; i--)
						buf[pos++] = ('0');
				}
			}

			if (frac0x != 0) {
				final int i = dig2bytes[frac0x];
				int x = 0;
				switch (i) {
				case 1:
					x = d_copy[from] /* one byte */;
					break;
				case 2:
					x = getInt16BE(d_copy, from);
					break;
				case 3:
					x = getInt24BE(d_copy, from);
					break;
				case 4:
					x = getInt32BE(d_copy, from);
					break;
				}
				x ^= mask;
				if (x != 0) {
					final int dig = DIG_PER_DEC1 - frac0x;
					x *= powers10[dig];
					if (x < 0 || x > DIG_MAX) {
						throw new IllegalArgumentException("bad format, x exceed: " + x + ", " + DIG_MAX);
					}
					for (int j = DIG_PER_DEC1; j > dig; j--) {
						final int divisor = powers10[j - 1];
						final int y = x / divisor;
						buf[pos++] = ((char) ('0' + y));
						x -= y * divisor;
					}
				}
			}

			if (mark == pos)
				/* make number more friendly */
				buf[pos++] = ('0');
		}

		d_copy[begin] ^= 0x80; /* restore sign */
		String decimal = String.valueOf(buf, 0, pos);
		return new BigDecimal(decimal);
	}

	/**
	 * Fill MY_BITMAP structure from buffer.
	 * 
	 * @param len
	 *            The length of MY_BITMAP in bits.
	 */
	public final void fillBitmap(BitSet bitmap, final int pos, final int len) {
		if (pos + ((len + 7) / 8) > limit || pos < 0)
			throw new IllegalArgumentException("limit excceed: " + (pos + (len + 7) / 8));

		fillBitmap0(bitmap, pos, len);
	}

	/**
	 * Fill next MY_BITMAP structure from buffer.
	 * 
	 * @param len
	 *            The length of MY_BITMAP in bits.
	 */
	public final void fillBitmap(BitSet bitmap, final int len) {
		if (position + ((len + 7) / 8) > limit)
			throw new IllegalArgumentException("limit excceed: " + (position + ((len + 7) / 8)));

		position = fillBitmap0(bitmap, position, len);
	}

	/**
	 * Fill MY_BITMAP structure from buffer.
	 * 
	 * @param len
	 *            The length of MY_BITMAP in bits.
	 */
	private final int fillBitmap0(BitSet bitmap, int pos, final int len) {
		final byte[] buf = buffer;

		for (int bit = 0; bit < len; bit += 8) {
			int flag = ((int) buf[pos++]) & 0xff;
			if (flag == 0)
				continue;
			if ((flag & 0x01) != 0)
				bitmap.set(bit);
			if ((flag & 0x02) != 0)
				bitmap.set(bit + 1);
			if ((flag & 0x04) != 0)
				bitmap.set(bit + 2);
			if ((flag & 0x08) != 0)
				bitmap.set(bit + 3);
			if ((flag & 0x10) != 0)
				bitmap.set(bit + 4);
			if ((flag & 0x20) != 0)
				bitmap.set(bit + 5);
			if ((flag & 0x40) != 0)
				bitmap.set(bit + 6);
			if ((flag & 0x80) != 0)
				bitmap.set(bit + 7);
		}
		return pos;
	}

	/**
	 * Return MY_BITMAP structure from buffer.
	 * 
	 * @param len
	 *            The length of MY_BITMAP in bits.
	 */
	public final BitSet getBitmap(final int pos, final int len) {
		BitSet bitmap = new BitSet(len);
		fillBitmap(bitmap, pos, len);
		return bitmap;
	}

	/**
	 * Return next MY_BITMAP structure from buffer.
	 * 
	 * @param len
	 *            The length of MY_BITMAP in bits.
	 */
	public final BitSet getBitmap(final int len) {
		BitSet bitmap = new BitSet(len);
		fillBitmap(bitmap, len);
		return bitmap;
	}

	/**
	 * Fill n bytes in this buffer.
	 */
	public final void fillBytes(final int pos, byte[] dest, final int destPos, final int len) {
		if (pos + len > limit || pos < 0)
			throw new IllegalArgumentException("limit excceed: " + (pos + len));

		System.arraycopy(buffer, pos, dest, destPos, len);
	}

	/**
	 * Fill next n bytes in this buffer.
	 */
	public final void fillBytes(byte[] dest, final int destPos, final int len) {
		if (position + len > limit)
			throw new IllegalArgumentException("limit excceed: " + (position + len));

		System.arraycopy(buffer, position, dest, destPos, len);
		position += len;
	}

	/**
	 * Return n-byte data from buffer.
	 */
	public final byte[] getData(final int pos, final int len) {
		byte[] buf = new byte[len];
		fillBytes(pos, buf, 0, len);
		return buf;
	}

	/**
	 * Return next n-byte data from buffer.
	 */
	public final byte[] getData(final int len) {
		byte[] buf = new byte[len];
		fillBytes(buf, 0, len);
		return buf;
	}

	/**
	 * Return all remaining data from buffer.
	 */
	public final byte[] getData() {
		return getData(0, limit);
	}

}
