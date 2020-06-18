package com.fan.mysql.async.util;

import io.netty.buffer.ByteBuf;

@SuppressWarnings("unused")
public class BufferDumper {

    public static String dumpAsHex(ByteBuf buffer) {
        int length = buffer.readableBytes();
        byte[] byteBuffer = new byte[length];

        buffer.markReaderIndex();
        buffer.readBytes(byteBuffer);
        buffer.resetReaderIndex();

        return dumpArrayAsHex(byteBuffer);
    }

    public static String dumpArrayAsHex(byte[] byteBuffer) {

        int length = byteBuffer.length;

        StringBuilder outputBuf = new StringBuilder(length * 4);

        int p = 0;
        int rows = length / 8;

        for (int i = 0; (i < rows) && (p < length); i++) {
            int ptemp = p;

            outputBuf.append(i).append(": ");

            for (int j = 0; j < 8; j++) {
                String hexVal = Integer.toHexString(byteBuffer[ptemp] & 0xff);

                if (hexVal.length() == 1) {
                    hexVal = "0" + hexVal; //$NON-NLS-1$
                }

                outputBuf.append(hexVal).append(" "); //$NON-NLS-1$
                ptemp++;
            }

            outputBuf.append("    "); //$NON-NLS-1$

            for (int j = 0; j < 8; j++) {
                int b = 0xff & byteBuffer[p];

                if (b > 32 && b < 127) {
                    outputBuf.append((char) b).append(" "); //$NON-NLS-1$
                } else {
                    outputBuf.append(". "); //$NON-NLS-1$
                }

                p++;
            }

            outputBuf.append("\n"); //$NON-NLS-1$
        }

        outputBuf.append(rows).append(": ");

        int n = 0;

        for (int i = p; i < length; i++) {
            String hexVal = Integer.toHexString(byteBuffer[i] & 0xff);

            if (hexVal.length() == 1) {
                hexVal = "0" + hexVal; //$NON-NLS-1$
            }

            outputBuf.append(hexVal).append(" "); //$NON-NLS-1$
            n++;
        }

        for (int i = n; i < 8; i++) {
            outputBuf.append("   "); //$NON-NLS-1$
        }

        outputBuf.append("    "); //$NON-NLS-1$

        for (int i = p; i < length; i++) {
            int b = 0xff & byteBuffer[i];

            if (b > 32 && b < 127) {
                outputBuf.append((char) b).append(" "); //$NON-NLS-1$
            } else {
                outputBuf.append(". "); //$NON-NLS-1$
            }
        }

        outputBuf.append("\n"); //$NON-NLS-1$
        outputBuf.append("Total ").append(byteBuffer.length).append(" bytes read\n");

        return outputBuf.toString();
    }
}