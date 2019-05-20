package org.openplacereviews.opendb;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.io.UnsupportedEncodingException;

import static org.junit.Assert.assertEquals;

public class SecUtilsTest {

	@Test
	public void calculateHashTest() throws UnsupportedEncodingException {
		String in1 = "abc";
		String expected256_1 = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
		String expected1_1 = "a9993e364706816aba3e25717850c26c9cd0d89d";
		byte[] out256_1 = SecUtils.calculateHash(SecUtils.HASH_SHA256, in1.getBytes("UTF-8"), null);
		byte[] out1_1 = SecUtils.calculateHash(SecUtils.HASH_SHA1, in1.getBytes("UTF-8"), null);
		assertEquals(expected256_1, Hex.encodeHexString(out256_1));
		assertEquals(expected1_1, Hex.encodeHexString(out1_1));

		String in2 = "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq";
		String expected256_2 = "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1";
		String expected1_2 = "84983e441c3bd26ebaae4aa1f95129e5e54670f1";
		byte[] out256_2 = SecUtils.calculateHash(SecUtils.HASH_SHA256, null, in2.getBytes("UTF-8"));
		byte[] out1_2 = SecUtils.calculateHash(SecUtils.HASH_SHA1, null, in2.getBytes("UTF-8"));
		assertEquals(expected256_2, Hex.encodeHexString(out256_2));
		assertEquals(expected1_2, Hex.encodeHexString(out1_2));

		String in3 = StringUtils.repeat("a", 1000000);
		String expected256_3 = "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0";
		String expected1_3 = "34aa973cd4c4daa4f61eeb2bdbad27316534016f";
		byte[] out256_3 = SecUtils.calculateHash(SecUtils.HASH_SHA256, null, in3.getBytes("UTF-8"));
		byte[] out1_3 = SecUtils.calculateHash(SecUtils.HASH_SHA1, null, in3.getBytes("UTF-8"));
		assertEquals(expected256_3, Hex.encodeHexString(out256_3));
		assertEquals(expected1_3, Hex.encodeHexString(out1_3));
	}

	@Test(expected = UnsupportedOperationException.class)
	public void calculateUnsupportedHashTest() throws UnsupportedEncodingException {
		String in = "abc";
		SecUtils.calculateHash("sha224", null, in.getBytes("UTF-8"));
	}
}
