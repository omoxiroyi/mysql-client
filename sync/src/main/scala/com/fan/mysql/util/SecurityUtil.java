package com.fan.mysql.util;


import com.fan.mysql.config.Capabilities;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 * @author fan
 */
@SuppressWarnings("unused")
public class SecurityUtil {

    public static byte[] scramble411(String password, String seed) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-1"); //$NON-NLS-1$
        byte[] passwordHashStage1 = md.digest(password.getBytes());
        md.reset();
        byte[] passwordHashStage2 = md.digest(passwordHashStage1);
        md.reset();
        byte[] seedAsBytes = seed.getBytes(); // for debugging
        md.update(seedAsBytes);
        byte[] toBeXord = md.digest(passwordHashStage2);
        int numToXor = toBeXord.length;
        for (int i = 0; i < numToXor; i++) {
            toBeXord[i] = (byte) (toBeXord[i] ^ passwordHashStage1[i]);
        }
        return toBeXord;
    }

    public static boolean compare(String seed, String password, byte[] encryptedPassword)
            throws NoSuchAlgorithmException, UnsupportedEncodingException {
        boolean flag;
        byte[] passwordHashStage2 = getHashStage2(password, seed);
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        byte[] seedAsBytes = seed.getBytes(Capabilities.CODE_PAGE_1252);
        md.update(seedAsBytes);
        md.update(passwordHashStage2);
        byte[] temp = md.digest();
        int length = temp.length;
        byte[] hash_stage1 = new byte[length];
        for (int i = 0; i < length; i++) {
            hash_stage1[i] = (byte) (temp[i] ^ encryptedPassword[i]);
        }
        md.reset();
        byte[] candidate_hash2 = md.digest(hash_stage1);
        md.reset();
        flag = Arrays.equals(candidate_hash2, passwordHashStage2);
        return flag;
    }

    /**
     * get value hashStage2
     */
    private static byte[] getHashStage2(String password, String seed) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-1"); //$NON-NLS-1$
        byte[] passwordHashStage1 = md.digest(password.getBytes());
        md.reset();
        byte[] passwordHashStage2 = md.digest(passwordHashStage1);
        md.reset();
        return passwordHashStage2;
    }

    public static String scramble323(String pass, String seed) {
        if ((pass == null) || (pass.length() == 0)) {
            return pass;
        }
        byte b;
        double d;
        long[] pw = hash(seed);
        long[] msg = hash(pass);
        long max = 0x3fffffffL;
        long seed1 = (pw[0] ^ msg[0]) % max;
        long seed2 = (pw[1] ^ msg[1]) % max;
        char[] chars = new char[seed.length()];
        for (int i = 0; i < seed.length(); i++) {
            seed1 = ((seed1 * 3) + seed2) % max;
            seed2 = (seed1 + seed2 + 33) % max;
            d = (double) seed1 / (double) max;
            b = (byte) Math.floor((d * 31) + 64);
            chars[i] = (char) b;
        }
        seed1 = ((seed1 * 3) + seed2) % max;
        seed2 = (seed1 + seed2 + 33) % max;
        d = (double) seed1 / (double) max;
        b = (byte) Math.floor(d * 31);
        for (int i = 0; i < seed.length(); i++) {
            chars[i] ^= (char) b;
        }
        return new String(chars);
    }

    private static long[] hash(String src) {
        long nr = 1345345333L;
        long add = 7;
        long nr2 = 0x12345671L;
        long tmp;
        for (int i = 0; i < src.length(); ++i) {
            switch (src.charAt(i)) {
                case ' ':
                case '\t':
                    continue;
                default:
                    tmp = (0xff & src.charAt(i));
                    nr ^= ((((nr & 63) + add) * tmp) + (nr << 8));
                    nr2 += ((nr2 << 8) ^ nr);
                    add += tmp;
            }
        }
        long[] result = new long[2];
        result[0] = nr & 0x7fffffffL;
        result[1] = nr2 & 0x7fffffffL;
        return result;
    }
}
