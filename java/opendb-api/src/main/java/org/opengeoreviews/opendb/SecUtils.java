package org.opengeoreviews.opendb;

import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.Signature;
import java.security.SignatureException;
import java.security.spec.ECGenParameterSpec;

import org.bouncycastle.crypto.generators.SCrypt;
import org.bouncycastle.crypto.prng.FixedSecureRandom;

public class SecUtils {

	

    // "pwd__scrypt_nick_17_8_1_256__ec_secp256k1" algoirthm
	public static KeyPair generateKeyPairFromPassword(String user, String pwd, String algo) throws NoSuchAlgorithmException, InvalidAlgorithmParameterException {
		KeyPairGenerator kpg = KeyPairGenerator.getInstance("EC");
        ECGenParameterSpec ecSpec = new ECGenParameterSpec("secp256k1");
        if(pwd.length() < 10) {
        	throw new IllegalArgumentException("Less than 10 characters produces only 50 bit entropy");
        }
        byte[] bytes = pwd.getBytes();
        byte[] scrypt = SCrypt.generate(bytes, user.getBytes(), 1 << 17, 8, 1, 256);
        kpg.initialize(ecSpec, new FixedSecureRandom(scrypt));
        return kpg.genKeyPair();
	}
	

	// algorithm - SHA1withECDSA
	public static byte[] signMessageWithKey(KeyPair keyPair, String msg) throws InvalidKeyException, SignatureException, NoSuchAlgorithmException {
        Signature sig = Signature.getInstance("SHA1withECDSA");
        sig.initSign(keyPair.getPrivate());
        sig.update(msg.getBytes());
        byte[] signatureBytes = sig.sign();
        return signatureBytes;
	}
	
	public static boolean validateSignature(KeyPair keyPair, String msg, String signature) throws SignatureException, InvalidKeyException, NoSuchAlgorithmException {
		Signature sig = Signature.getInstance("SHA1withECDSA");
        sig.initVerify(keyPair.getPublic());
        sig.update(msg.getBytes());
        return sig.verify(signature.getBytes());
	}

}
