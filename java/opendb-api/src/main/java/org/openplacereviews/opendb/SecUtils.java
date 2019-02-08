package org.openplacereviews.opendb;

import java.io.UnsupportedEncodingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.spec.ECGenParameterSpec;
import java.security.spec.EncodedKeySpec;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;

import org.apache.commons.codec.digest.DigestUtils;
import org.bouncycastle.crypto.generators.SCrypt;
import org.bouncycastle.crypto.prng.FixedSecureRandom;

public class SecUtils {
	public static final String SIG_ALGO_SHA1_EC = "SHA1withECDSA";
	
	public static final String DECODE_BASE64 = "base64";
	
	public static void main(String[] args) throws NoSuchAlgorithmException, InvalidAlgorithmParameterException, InvalidKeySpecException, InvalidKeyException, SignatureException, UnsupportedEncodingException {
		KeyPair kp = generateEC256K1KeyPairFromPassword("openplacereviews", "", null);
		System.out.println(kp.getPrivate().getFormat());
		System.out.println(kp.getPrivate().getAlgorithm());
		String pr = Base64.getEncoder().encodeToString(kp.getPrivate().getEncoded());
		String pk = Base64.getEncoder().encodeToString(kp.getPublic().getEncoded());
		String algo = kp.getPrivate().getAlgorithm();
		System.out.println(String.format("Private key: %s %s\nPublic key: %s %s", 
				kp.getPrivate().getFormat(), pr, kp.getPublic().getFormat(), pk));
		String signMessageTest = "Hello this is a registration message test";
		byte[] signature = signMessageWithKey(kp, signMessageTest, SIG_ALGO_SHA1_EC);
		System.out.println(String.format("Signed message: %s %s", Base64.getEncoder().encodeToString(signature), signMessageTest) );
		
		
		KeyPair nk = getKeyPair(algo, kp.getPrivate().getFormat(), pr, kp.getPublic().getFormat(), pk);
		// validate
		pr = Base64.getEncoder().encodeToString(nk.getPrivate().getEncoded());
		pk = Base64.getEncoder().encodeToString(nk.getPublic().getEncoded());
		System.out.println(String.format("Private key: %s %s\nPublic key: %s %s", 
				nk.getPrivate().getFormat(), pr, nk.getPublic().getFormat(), pk));
		System.out.println(validateSignature(nk, signMessageTest, SIG_ALGO_SHA1_EC, signature));
		
	}
	
	
	public static EncodedKeySpec getKeySpecByFormat(String format, byte[] data) {
		switch(format) {
		case "PKCS#8": return new PKCS8EncodedKeySpec(data);
		case "X.509": return new X509EncodedKeySpec(data);
		}
		throw new IllegalArgumentException(format);
	}
	
	public static String encodeBase64(byte[] data) {
		return Base64.getEncoder().encodeToString(data);
	}
	
	public static KeyPair getKeyPair(String algo, String format, String prKey, 
			String pubformat, String pbKey) throws InvalidKeySpecException, NoSuchAlgorithmException {
		KeyFactory keyFactory = KeyFactory.getInstance(algo);
		PublicKey pb = null; 
		PrivateKey pr = null; 
		if(pbKey != null) {
			byte[] bytes;
			if (pubformat.startsWith(DECODE_BASE64 + ":")) {
				bytes = Base64.getDecoder().decode(pbKey);
				pubformat = pubformat.substring(DECODE_BASE64.length() + 1);
			} else {
				throw new IllegalArgumentException("Illegal decoding algorith");
			}
			bytes = Base64.getDecoder().decode(pbKey);
			pb = keyFactory.generatePublic(getKeySpecByFormat(pubformat, bytes));
		}
		if(prKey != null) {
			byte[] bytes;
			if(pubformat.startsWith(DECODE_BASE64)) {
				bytes = Base64.getDecoder().decode(prKey);
				pubformat = pubformat.substring(DECODE_BASE64.length());
			} else {
				throw new IllegalArgumentException("Illegal decoding algorith");
			}
			pr = keyFactory.generatePrivate(getKeySpecByFormat(format, bytes));
		}
		return new KeyPair(pb, pr);
	}

    // "EC:secp256k1:scrypt(salt,N:17,r:8,p:1,len:256)" algorithm - EC256K1_S17R8
	public static KeyPair generateEC256K1KeyPairFromPassword(String salt, String pwd, String algo) throws NoSuchAlgorithmException, InvalidAlgorithmParameterException, UnsupportedEncodingException {
		KeyPairGenerator kpg = KeyPairGenerator.getInstance("EC");
        ECGenParameterSpec ecSpec = new ECGenParameterSpec("secp256k1");
        if(pwd.length() < 10) {
        	throw new IllegalArgumentException("Less than 10 characters produces only 50 bit entropy");
        }
        byte[] bytes = pwd.getBytes("UTF-8");
        byte[] scrypt = SCrypt.generate(bytes, salt.getBytes("UTF-8"), 1 << 17, 8, 1, 256);
        kpg.initialize(ecSpec, new FixedSecureRandom(scrypt));
        return kpg.genKeyPair();
	}
	
	
	public static KeyPair generateEC256K1KeyPair()
			throws NoSuchAlgorithmException, InvalidAlgorithmParameterException, UnsupportedEncodingException {
		KeyPairGenerator kpg = KeyPairGenerator.getInstance("EC");
		ECGenParameterSpec ecSpec = new ECGenParameterSpec("secp256k1");
		kpg.initialize(ecSpec);
		return kpg.genKeyPair();
	}
	

	public static String signMessageWithKeyBase64(KeyPair keyPair, String msg, String hashAlgo) throws InvalidKeyException, SignatureException, NoSuchAlgorithmException, UnsupportedEncodingException {
        return Base64.getEncoder().encodeToString(signMessageWithKey(keyPair, msg, hashAlgo));
	}
	
	public static byte[] signMessageWithKey(KeyPair keyPair, String msg, String hashAlgo) throws InvalidKeyException, SignatureException, NoSuchAlgorithmException, UnsupportedEncodingException {
        Signature sig = Signature.getInstance(hashAlgo);
        sig.initSign(keyPair.getPrivate());
        sig.update(msg.getBytes("UTF-8"));
        byte[] signatureBytes = sig.sign();
        return signatureBytes;
	}
	
	public static boolean validateSignature(KeyPair keyPair, String msg, String hashAlgo, byte[] signature) throws SignatureException, InvalidKeyException, NoSuchAlgorithmException, UnsupportedEncodingException {
		Signature sig = Signature.getInstance(hashAlgo);
        sig.initVerify(keyPair.getPublic());
        sig.update(msg.getBytes("UTF-8"));
        return sig.verify(signature);
	}
	
	public static String calculateSha1(String msg) {
		 try {
			return DigestUtils.sha1Hex(msg.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			throw new IllegalStateException(e);
		}
	}
	
	public static String calculateSha256(String msg) {
		 try {
			return DigestUtils.sha256Hex(msg.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			throw new IllegalStateException(e);
		}
	}


	public static byte[] decodeSignature(String format, String digest) {
		try {
			if(format.equals(DECODE_BASE64)) {
				return Base64.getDecoder().decode(digest.getBytes("UTF-8"));
			}
		} catch (UnsupportedEncodingException e) {
			throw new IllegalStateException(e);
		}
		throw new IllegalArgumentException(format);
	}

}
