package org.opengeoreviews.opendb;

import java.io.UnsupportedEncodingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
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

import org.bouncycastle.crypto.generators.SCrypt;
import org.bouncycastle.crypto.prng.FixedSecureRandom;

public class SecUtils {
	public static final String SIG_ALGO_SHA1_EC = "SHA1withECDSA";
	public static void main(String[] args) throws NoSuchAlgorithmException, InvalidAlgorithmParameterException, InvalidKeySpecException, InvalidKeyException, SignatureException, UnsupportedEncodingException {
		KeyPair kp = generateKeyPairFromPassword("openplacereviews", "", null);
		System.out.println(kp.getPrivate().getFormat());
		System.out.println(kp.getPrivate().getAlgorithm());
		String pr = Base64.getEncoder().encodeToString(kp.getPrivate().getEncoded());
		String pk = Base64.getEncoder().encodeToString(kp.getPublic().getEncoded());
		String algo = kp.getPrivate().getAlgorithm();
		System.out.println(String.format("Private key: %s %s\nPublic key: %s %s", 
				kp.getPrivate().getFormat(), pr, kp.getPublic().getFormat(), pk));
		String signMessageTest = "Hello this is a registration message test  MD4CAQAwEAYHKoZIzj0CAQYFK4EEAAoEJzAlAgEBBCBEvjhtseGMORzDEzWZaguoT7LMA  MD4CAQAwEAYHKoZIzj0CAQYFK4EEAAoEJzAlAgEBBCBEvjhtseGMORzDEzWZaguoT7LMA MD4CAQAwEAYHKoZIzj0CAQYFK4EEAAoEJzAlAgEBBCBEvjhtseGMORzDEzWZaguoT7LMA MD4CAQAwEAYHKoZIzj0CAQYFK4EEAAoEJzAlAgEBBCBEvjhtseGMORzDEzWZaguoT7LMA";
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
	
	public static KeyPair getKeyPair(String algo, String format, String prKey, 
			String pubformat, String pbKey) throws InvalidKeySpecException, NoSuchAlgorithmException {
		KeyFactory keyFactory = KeyFactory.getInstance(algo);
		PublicKey pb = null; 
		PrivateKey pr = null; 
		if(pbKey != null) {
			byte[] bytes = Base64.getDecoder().decode(pbKey);
			pb = keyFactory.generatePublic(getKeySpecByFormat(pubformat, bytes));
		}
		if(prKey != null) {
			byte[] bytes = Base64.getDecoder().decode(prKey);
			pr = keyFactory.generatePrivate(getKeySpecByFormat(format, bytes));
		}
		return new KeyPair(pb, pr);
	}

    // "pwd__scrypt_nick_17_8_1_256__ec_secp256k1" algoirthm
	public static KeyPair generateKeyPairFromPassword(String user, String pwd, String algo) throws NoSuchAlgorithmException, InvalidAlgorithmParameterException, UnsupportedEncodingException {
		KeyPairGenerator kpg = KeyPairGenerator.getInstance("EC");
        ECGenParameterSpec ecSpec = new ECGenParameterSpec("secp256k1");
        if(pwd.length() < 10) {
        	throw new IllegalArgumentException("Less than 10 characters produces only 50 bit entropy");
        }
        byte[] bytes = pwd.getBytes("UTF-8");
        byte[] scrypt = SCrypt.generate(bytes, user.getBytes("UTF-8"), 1 << 17, 8, 1, 256);
        kpg.initialize(ecSpec, new FixedSecureRandom(scrypt));
        return kpg.genKeyPair();
	}
	

	// algorithm - SHA1withECDSA
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

}
