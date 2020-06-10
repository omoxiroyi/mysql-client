package com.dingcloud.dts.binlog.fetcher;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dingcloud.dts.binlog.mysql.dbsync.LogBuffer;
import com.dingcloud.dts.binlog.vio.Vio;

public class ConnectionFetcher extends LogBuffer {

	private static final Logger logger = LoggerFactory.getLogger(ConnectionFetcher.class);

	/** Command to dump binlog */
	public static final byte COM_BINLOG_DUMP = 18;

	/** Packet header sizes */
	public static final int NET_HEADER_SIZE = 4;
	public static final int SQLSTATE_LENGTH = 5;

	/** Packet offsets */
	public static final int PACKET_LEN_OFFSET = 0;
	public static final int PACKET_SEQ_OFFSET = 3;

	/** Maximum packet length */
	public static final int MAX_PACKET_LENGTH = (256 * 256 * 256 - 1);

	/** Default event buffer */
	public static final int DEFAULT_EVENT_PACKET_LENGTH = 1024 * 1024;

	private Vio vio;

	public ConnectionFetcher(SocketChannel channel) {
		this.vio = new Vio(channel);
		this.buffer = new byte[DEFAULT_EVENT_PACKET_LENGTH];
		this.position = 0;
		this.limit = 0;
	}

	public void fetch() throws IOException {
		try {
			// reset position
			position = 0;
			// fetching packet header from input.
			fetch0(0, NET_HEADER_SIZE);
			// Fetching the first packet(may a multi-packet).
			int netlen = getUint24(PACKET_LEN_OFFSET);
			int netnum = getUint8(PACKET_SEQ_OFFSET);
			fetch0(0, netlen);
			// Detecting error code.
			final int mark = getUint8(0);
			if (mark != 0) {
				// error from master
				if (mark == 255) {
					// Indicates an error
					position = 1;
					final int errno = getInt16();
					String sqlstate = forward(1).getFixString(SQLSTATE_LENGTH);
					String errmsg = getFixString(limit - position);
					throw new IOException("Received error packet:" + " errno = " + errno + ",sqlstate = " + sqlstate
							+ " errmsg = " + errmsg);
				} else if (mark == 254) {
					// Indicates end of stream.
					throw new IOException(
							"Received EOF packet from server, apparent master disconnected. It's may be duplicate slaveId , check instance config");
				} else {
					// Should not happen.
					throw new IOException("Unexpected response " + mark + " while fetching binlog: packet #" + netnum
							+ ", len = " + netlen);
				}
			}
			// The first packet is a multi-packet, concatenate the packets.
			while (netlen == MAX_PACKET_LENGTH) {
				int off = limit;
				fetch0(off, NET_HEADER_SIZE);
				netlen = getUint24(off + PACKET_LEN_OFFSET);
				netnum = getUint8(off + PACKET_SEQ_OFFSET);
				fetch0(off, netlen);
			}
			// reset position
			position = 1;
		} catch (SocketTimeoutException e) {
			logger.error("Socket timeout expired, closing connection", e);
			throw e;
		} catch (InterruptedIOException e) {
			logger.info("I/O interrupted while reading from client socket", e);
			throw e;
		} catch (ClosedByInterruptException e) {
			logger.info("I/O interrupted while reading from client socket", e);
			throw e;
		} catch (IOException e) {
			logger.warn("I/O error while reading from client socket", e);
			throw e;
		}
	}

	public long getLastReadTime() {
		return vio.getLastReadTime();
	}

	private void fetch0(final int offset, final int length) throws IOException {
		ensureCapacity(offset, length);
		int remainLen = length;
		int currentOff = offset;
		while (remainLen > 0) {
			int readLen = vio.vio_read_buff(buffer, currentOff, remainLen);
			remainLen -= readLen;
			currentOff += readLen;
		}
		limit = position = offset + length;
	}

	private void ensureCapacity(int offset, int length) {
		int newLen = buffer.length;
		// shrink buffer
		if (offset + length < DEFAULT_EVENT_PACKET_LENGTH) {
			newLen = DEFAULT_EVENT_PACKET_LENGTH;
		}
		// expand buffer
		while (offset + length > newLen) {
			newLen = newLen * 2;
		}
		if (newLen != buffer.length) {
			byte[] newBuffer = new byte[newLen];
			System.arraycopy(buffer, 0, newBuffer, 0, offset);
			this.buffer = newBuffer;
		}
	}
	
}
