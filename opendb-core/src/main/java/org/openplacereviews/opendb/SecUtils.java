package org.openplacereviews.opendb;

import static org.openplacereviews.opendb.ops.OpBlockchainRules.JSON_MSG_TYPE;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Security;
import java.security.Signature;
import java.security.SignatureException;
import java.security.spec.ECGenParameterSpec;
import java.security.spec.EncodedKeySpec;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.bouncycastle.crypto.generators.SCrypt;
import org.bouncycastle.crypto.prng.FixedSecureRandom;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;

public class SecUtils {
	private static final String SIG_ALGO_SHA1_EC = "SHA1withECDSA";
	private static final String SIG_ALGO_NONE_EC = "NonewithECDSA";
	
	public static final String SIG_ALGO_ECDSA = "ECDSA";
	public static final String ALGO_EC = "EC";
	public static final String ALGO_PROVIDER = "BC";
//	public static final String ALGO_PROVIDER = "SunEC";
	
	
	public static final String EC_256SPEC_K1 = "secp256k1";

	public static final String KEYGEN_PWD_METHOD_1 = "EC256K1_S17R8";
	public static final String DECODE_BASE64 = "base64";
	public static final String HASH_SHA256 = "sha256";
	public static final String HASH_SHA1 = "sha1";

	public static final String KEY_BASE64 = DECODE_BASE64;
	
	static {
		Security.addProvider(new BouncyCastleProvider());
	}

	public static void main1(String[] args) throws FailedVerificationException, NoSuchAlgorithmException, NoSuchProviderException {
//		try {
//			Provider p[] = Security.getProviders();
//			for (int i = 0; i < p.length; i++) {
//				System.out.println(p[i]);
//				for (Enumeration e = p[i].keys(); e.hasMoreElements();)
//					System.out.println("\t" + e.nextElement());
//			}
//		} catch (Exception e) {
//			System.out.println(e);
//		}
		KeyFactory keyFactory = KeyFactory.getInstance(ALGO_EC, ALGO_PROVIDER);
		System.out.println("0. Provider/Algorithm: '" + keyFactory.getProvider() + "' '" + keyFactory.getAlgorithm()+"'");
		KeyPair kp = SecUtils.getKeyPair(ALGO_EC,
				"base64:PKCS#8:MD4CAQAwEAYHKoZIzj0CAQYFK4EEAAoEJzAlAgEBBCDR+/ByIjTHZgfdnMfP9Ab5s14mMzFX+8DYqUiGmf/3rw=="
				, "base64:X.509:MFYwEAYHKoZIzj0CAQYFK4EEAAoDQgAEOMUiRZwU7wW8L3A1qaJPwhAZy250VaSxJmKCiWdn9EMeubXQgWNT8XUWLV5Nvg7O3sD+1AAQLG5kHY8nOc/AyA==");
//		KeyPair kp = generateECKeyPairFromPassword(KEYGEN_PWD_METHOD_1, "openplacereviews", "");
//		KeyPair kp = generateRandomEC256K1KeyPair();
		
		
		System.out.println("Validate key pair " + SecUtils.validateKeyPair(ALGO_EC, kp.getPrivate(), kp.getPublic()));
		String pr = encodeKey(KEY_BASE64, kp.getPrivate());
		String pk = encodeKey(KEY_BASE64, kp.getPublic());
		KeyPair nk = getKeyPair(ALGO_EC, pr, pk);
		System.out.println();
		System.out.println("1. Test write / read private / public key");
		printKeyPair(kp);
		printKeyPair(nk);
		
		System.out.println();
		System.out.println("2. Test signature for simple message");
		
		String signMessageTest = "Hello this is a registration message test";
		String signatureText = signMessageWithKeyBase64(kp, signMessageTest.getBytes(), SIG_ALGO_SHA1_EC, null);
		System.out.println("Validate signature !!!" + validateSignature(kp, signMessageTest.getBytes(), SIG_ALGO_SHA1_EC, decodeSignature(signatureText)) +"!!! '" + signatureText +"'");
		String androidSignatureText = "SHA1withECDSA:base64:MEYCIQC/0s6wNB0YA0GRFNLHpqQCkWH5EvvdJz6wWocCfTmHJwIhAM8eeKXbr0mx4N+VVRosUBodZtDVc2cnmdGdgQo9+UA4";
		System.out.println("Android Validate signature !!!" + validateSignature(kp, signMessageTest.getBytes(), SIG_ALGO_SHA1_EC, decodeSignature(androidSignatureText)) +"!!! '" + androidSignatureText+"'");

		
		System.out.println();
		System.out.println("3. Create hash for operation / sign / validate signature");
		JsonFormatter formatter = new JsonFormatter();
		String msg = "{\n" + 
				"		\"type\" : \"sys.signup\",\n" + 
				"		\"signed_by\": \"openplacereviews\",\n" + 
				"		\"create\": [{\n" + 
				"			\"id\": [\"openplacereviews\"],\n" + 
				"			\"name\" : \"openplacereviews\",\n" + 
				"			\"algo\": \"EC\",\n" + 
				"			\"auth_method\": \"provided\",\n" + 
				"			\"pubkey\": \"base64:X.509:MFYwEAYHKoZIzj0CAQYFK4EEAAoDQgAEn6GkOTN3SYc+OyCYCpqPzKPALvUgfUVNDJ+6eyBlCHI1/gKcVqzHLwaO90ksb29RYBiF4fW/PqHcECNzwJB+QA==\"\n" + 
				"		}]\n" + 
				"	}";

		OpOperation opOperation = formatter.parseOperation(msg);
		String hash = JSON_MSG_TYPE + ":"
				+ SecUtils.calculateHashWithAlgo(SecUtils.HASH_SHA256, null,
				formatter.opToJsonNoHash(opOperation));

		byte[] hashBytes = SecUtils.getHashBytes(hash);
		System.out.println("Sign operation hash: " + hash);
		String signatureOp = signMessageWithKeyBase64(kp, hashBytes, SecUtils.SIG_ALGO_ECDSA, null);
		System.out.println("Validate signature !!!" + validateSignature(kp, hashBytes, signatureOp) + "!!! " + signatureOp);
		String androidSignatureOp = "ECDSA:base64:MEQCIAsbxOH6M/UjnNk/o4CsE5/FfYuops5HtHAuTXk3+7VgAiAIOBI8Z8+gHnkAJEsqEBzlgHPhYvpwR/TM6uHssvX6ZQ==";
		System.out.println("Android Validate signature !!!" + validateSignature(kp, hashBytes, androidSignatureOp) +"!!! '" + androidSignatureOp+"'");


	}

	public static void main(String[] args) throws FailedVerificationException, NoSuchAlgorithmException, NoSuchProviderException {
		System.out.println("UPLOAD IMAGE 1");
		String pkey = "base64:PKCS#8:MD4CAQAwEAYHKoZIzj0CAQYFK4EEAAoEJzAlAgEBBCAOpUDyGrTPRPDQRCIRXysxC6gCgSTiNQ5nVEjhvsFITA==";
		String uname = "openplacereviews:test_1";
		//uploadImage(pkey, uname);
		System.out.println("END IMAGE 1");
		
		System.out.println("UPLOAD IMAGE 2");
		pkey = "base64:PKCS#8:MIGNAgEAMBAGByqGSM49AgEGBSuBBAAKBHYwdAIBAQQgvSSAiSI8dPR9PAoFtytzde7Dbex3WJEgxUSFHsPCwbygBwYFK4EEAAqhRANCAATPJHtVbGC3ICQVOw/RpaBIa8Af3vdo52ulM1hCnLqmeCsvqncSvjds8cDGlxBVRgSafUO0NTXcXd07L+eFmlw9";
		uname = "test123456789:opr_web";
		uploadImage(pkey, uname);
		System.out.println("END IMAGE 2");
	}
	
	//priv = 
	//
	public static int uploadImage(String privateKey, String username) throws FailedVerificationException {
		String[] placeId = new String[] { "9G2GCG", "wlkomu" };
		String image = "{\"type\":\"#image\",\"hash\":\"7d5c8838689bf4e3e8dd8d392112b2cc8bf8eae2fdd5e5a47c41014b3804660c\",\"extension\":\"\",\"cid\":\"QmRJJH5RGLqWDKMjLFRXuWKWARW8zpoNJfcvxm6iUqg482\"}";
		KeyPair kp = SecUtils.getKeyPair(ALGO_EC, privateKey, null);
		String signed = username;// + ":opr-web";
		JsonFormatter formatter = new JsonFormatter();

		OpOperation opOperation = new OpOperation();
		opOperation.setType("opr.place");
		List<Object> edits = new ArrayList<>();
		Map<String, Object> edit = new TreeMap<>();
		List<String> imageResponseList = new ArrayList<>();
		imageResponseList.add(image);
		List<String> ids = new ArrayList<>(Arrays.asList(placeId));
		Map<String, Object> change = new TreeMap<>();
		Map<String, Object> images = new TreeMap<>();
		Map<String, Object> outdoor = new TreeMap<>();
		outdoor.put("outdoor", imageResponseList);
		images.put("append", outdoor);
		change.put("version", "increment");
		change.put("images", images);
		edit.put("id", ids);
		edit.put("change", change);
		edit.put("current", new Object());
		edits.add(edit);
		opOperation.putObjectValue(OpOperation.F_EDIT, edits);
		opOperation.setSignedBy(signed);
		String hash = JSON_MSG_TYPE + ":"
				+ SecUtils.calculateHashWithAlgo(SecUtils.HASH_SHA256, null,
				formatter.opToJsonNoHash(opOperation));
		byte[] hashBytes = SecUtils.getHashBytes(hash);
		String signature = signMessageWithKeyBase64(kp, hashBytes, SecUtils.SIG_ALGO_SHA1_EC, null);
		opOperation.addOrSetStringValue("hash", hash);
		opOperation.addOrSetStringValue("signature", signature);
		String url = "https://test.openplacereviews.org/" + "api/auth/process-operation?addToQueue=true&dontSignByServer=false";
		String json = formatter.opToJson(opOperation);
		System.out.println("JSON: " + json);
		HttpURLConnection connection;
		try {
			connection = (HttpURLConnection) new URL(url).openConnection();
			connection.setRequestProperty("Content-Type", "application/json");
			connection.setConnectTimeout(10000);
			connection.setRequestMethod("POST");
			connection.setDoOutput(true);
			try {
				DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
				wr.write(json.getBytes());
			} catch (Exception e) {
				e.printStackTrace();
			}
			int rc = connection.getResponseCode();
			if (rc != 200) {
				BufferedReader br = new BufferedReader(new InputStreamReader(connection.getErrorStream()));
				String strCurrentLine;
				while ((strCurrentLine = br.readLine()) != null) {
					System.out.println(strCurrentLine);
				}
			}
			return rc;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return -1;
	}


	private static void printKeyPair(KeyPair nk) {
		String pr;
		String pk;
		pr = Base64.getEncoder().encodeToString(nk.getPrivate().getEncoded());
		pk = Base64.getEncoder().encodeToString(nk.getPublic().getEncoded());
		System.out.println(String.format("Private key: 'base64:%s:%s'\nPublic key: 'base64:%s:%s'", nk.getPrivate().getFormat(), pr, nk
				.getPublic().getFormat(), pk));
	}

	public static EncodedKeySpec decodeKey(String key) {
		if (key.startsWith(KEY_BASE64 + ":")) {
			key = key.substring(KEY_BASE64.length() + 1);
			int s = key.indexOf(':');
			if (s == -1) {
				throw new IllegalArgumentException(String.format("Key doesn't contain algorithm of hashing to verify"));
			}
			return getKeySpecByFormat(key.substring(0, s), Base64.getDecoder().decode(key.substring(s + 1)));
		}
		throw new IllegalArgumentException(String.format("Key doesn't contain algorithm of hashing to verify"));
	}

	public static String encodeKey(String algo, PublicKey pk) {
		if (algo.equals(KEY_BASE64)) {
			return SecUtils.KEY_BASE64 + ":" + pk.getFormat() + ":" + encodeBase64(pk.getEncoded());
		}
		throw new UnsupportedOperationException("Algorithm is not supported: " + algo);
	}

	public static String encodeKey(String algo, PrivateKey pk) {
		if (algo.equals(KEY_BASE64)) {
			return SecUtils.KEY_BASE64 + ":" + pk.getFormat() + ":" + encodeBase64(pk.getEncoded());
		}
		throw new UnsupportedOperationException("Algorithm is not supported: " + algo);
	}

	public static EncodedKeySpec getKeySpecByFormat(String format, byte[] data) {
		switch (format) {
		case "PKCS#8":
			return new PKCS8EncodedKeySpec(data);
		case "X.509":
			return new X509EncodedKeySpec(data);
		}
		throw new IllegalArgumentException(format);
	}

	public static String encodeBase64(byte[] data) {
		return Base64.getEncoder().encodeToString(data);
	}

	public static boolean validateKeyPair(String algo, PrivateKey privateKey, PublicKey publicKey)
			throws FailedVerificationException {
		if (!algo.equals(ALGO_EC)) {
			throw new FailedVerificationException("Algorithm is not supported: " + algo);
		}
		// create a challenge
		byte[] challenge = new byte[512];
		ThreadLocalRandom.current().nextBytes(challenge);

		try {
			// sign using the private key
			Signature sig = Signature.getInstance(SIG_ALGO_SHA1_EC, ALGO_PROVIDER);
			sig.initSign(privateKey);
			sig.update(challenge);
			byte[] signature = sig.sign();

			// verify signature using the public key
			sig.initVerify(publicKey);
			sig.update(challenge);

			boolean keyPairMatches = sig.verify(signature);
			return keyPairMatches;
		} catch (InvalidKeyException e) {
			throw new FailedVerificationException(e);
		} catch (NoSuchAlgorithmException e) {
			throw new FailedVerificationException(e);
		} catch (SignatureException e) {
			throw new FailedVerificationException(e);
		} catch (NoSuchProviderException e) {
			throw new FailedVerificationException(e);
		}
	}

	public static KeyPair getKeyPair(String algo, String prKey, String pbKey) throws FailedVerificationException {
		try {
			KeyFactory keyFactory = KeyFactory.getInstance(algo, ALGO_PROVIDER);
			PublicKey pb = null;
			PrivateKey pr = null;
			if (pbKey != null) {
				pb = keyFactory.generatePublic(decodeKey(pbKey));
			}
			if (prKey != null) {
				pr = keyFactory.generatePrivate(decodeKey(prKey));
			}
			return new KeyPair(pb, pr);
		} catch (NoSuchAlgorithmException e) {
			throw new FailedVerificationException(e);
		} catch (InvalidKeySpecException e) {
			throw new FailedVerificationException(e);
		} catch (NoSuchProviderException e) {
			throw new FailedVerificationException(e);
		}
	}

	public static KeyPair generateKeyPairFromPassword(String algo, String keygenMethod, String salt, String pwd)
			throws FailedVerificationException {
		if (algo.equals(ALGO_EC)) {
			return generateECKeyPairFromPassword(keygenMethod, salt, pwd);
		}
		throw new UnsupportedOperationException("Unsupported algo keygen method: " + algo);
	}

	public static KeyPair generateECKeyPairFromPassword(String keygenMethod, String salt, String pwd)
			throws FailedVerificationException {
		if (keygenMethod.equals(KEYGEN_PWD_METHOD_1)) {
			return generateEC256K1KeyPairFromPassword(salt, pwd);
		}
		throw new UnsupportedOperationException("Unsupported keygen method: " + keygenMethod);
	}

	// "EC:secp256k1:scrypt(salt,N:17,r:8,p:1,len:256)" algorithm - EC256K1_S17R8
	public static KeyPair generateEC256K1KeyPairFromPassword(String salt, String pwd)
			throws FailedVerificationException {
		try {
			KeyPairGenerator kpg = KeyPairGenerator.getInstance(ALGO_EC, ALGO_PROVIDER);
			ECGenParameterSpec ecSpec = new ECGenParameterSpec(EC_256SPEC_K1);
			if (pwd.length() < 10) {
				throw new IllegalArgumentException("Less than 10 characters produces only 50 bit entropy");
			}
			byte[] bytes = pwd.getBytes("UTF-8");
			byte[] scrypt = SCrypt.generate(bytes, salt.getBytes("UTF-8"), 1 << 17, 8, 1, 256);
			kpg.initialize(ecSpec, new FixedSecureRandom(scrypt));
			return kpg.genKeyPair();
		} catch (NoSuchAlgorithmException e) {
			throw new FailedVerificationException(e);
		} catch (UnsupportedEncodingException e) {
			throw new FailedVerificationException(e);
		} catch (InvalidAlgorithmParameterException e) {
			throw new FailedVerificationException(e);
		} catch (NoSuchProviderException e) {
			throw new FailedVerificationException(e);
		}
	}

	public static KeyPair generateRandomEC256K1KeyPair() throws FailedVerificationException {
		try {
			KeyPairGenerator kpg = KeyPairGenerator.getInstance(ALGO_EC, ALGO_PROVIDER);
			ECGenParameterSpec ecSpec = new ECGenParameterSpec(EC_256SPEC_K1);
			kpg.initialize(ecSpec);
			return kpg.genKeyPair();
		} catch (NoSuchAlgorithmException e) {
			throw new FailedVerificationException(e);
		} catch (InvalidAlgorithmParameterException e) {
			throw new FailedVerificationException(e);
		} catch (NoSuchProviderException e) {
			throw new FailedVerificationException(e);
		}
	}

	public static String signMessageWithKeyBase64(KeyPair keyPair, byte[] msg, String signAlgo, ByteArrayOutputStream out)
			throws FailedVerificationException {
		byte[] sigBytes = signMessageWithKey(keyPair, msg, signAlgo);
		if(out != null) {
			try {
				out.write(sigBytes);
			} catch (IOException e) {
				throw new IllegalStateException(e);
			}
		}
		String signature = Base64.getEncoder().encodeToString(sigBytes);
		return signAlgo + ":" + DECODE_BASE64 + ":" + signature;
	}

	public static byte[] signMessageWithKey(KeyPair keyPair, byte[] msg, String signAlgo)
			throws FailedVerificationException {
		try {
			Signature sig = Signature.getInstance(getInternalSigAlgo(signAlgo), ALGO_PROVIDER);
			sig.initSign(keyPair.getPrivate());
			sig.update(msg);
			byte[] signatureBytes = sig.sign();
			return signatureBytes;
		} catch (NoSuchAlgorithmException e) {
			throw new FailedVerificationException(e);
		} catch (InvalidKeyException e) {
			throw new FailedVerificationException(e);
		} catch (SignatureException e) {
			throw new FailedVerificationException(e);
		} catch (NoSuchProviderException e) {
			throw new FailedVerificationException(e);
		}
	}
	
	public static boolean validateSignature(KeyPair keyPair, byte[] msg, String sig)
			throws FailedVerificationException {
		if(sig == null || keyPair == null) {
			 return false;
		}
		int ind = sig.indexOf(':');
		String sigAlgo = sig.substring(0, ind);
		return validateSignature(keyPair, msg, sigAlgo, decodeSignature(sig.substring(ind + 1)));
	}

	public static boolean validateSignature(KeyPair keyPair, byte[] msg, String sigAlgo, byte[] signature)
			throws FailedVerificationException {
		if (keyPair == null) {
			return false;
		}
		try {
			Signature sig = Signature.getInstance(getInternalSigAlgo(sigAlgo), ALGO_PROVIDER);
			sig.initVerify(keyPair.getPublic());
			sig.update(msg);
			return sig.verify(signature);
		} catch (NoSuchAlgorithmException e) {
			throw new FailedVerificationException(e);
		} catch (InvalidKeyException e) {
			throw new FailedVerificationException(e);
		} catch (SignatureException e) {
			throw new FailedVerificationException(e);
		} catch (NoSuchProviderException e) {
			throw new FailedVerificationException(e);
		}
	}

	private static String getInternalSigAlgo(String sigAlgo) {
		return sigAlgo.equals(SIG_ALGO_ECDSA)? SIG_ALGO_NONE_EC : sigAlgo;
	}
	

	public static byte[] calculateHash(String algo, byte[] b1, byte[] b2) {
		byte[] m = mergeTwoArrays(b1, b2);
		if (algo.equals(HASH_SHA256)) {
			return DigestUtils.sha256(m);
		} else if (algo.equals(HASH_SHA1)) {
			return DigestUtils.sha1(m);
		}
		throw new UnsupportedOperationException();
	}

	public static byte[] mergeTwoArrays(byte[] b1, byte[] b2) {
		byte[] m = b1 == null ? b2 : b1;
		if(b2 != null && b1 != null) {
			m = new byte[b1.length + b2.length];
			System.arraycopy(b1, 0, m, 0, b1.length);
			System.arraycopy(b2, 0, m, b1.length, b2.length);
		}
		return m;
	}
	
	public static String calculateHashWithAlgo(String algo, String salt, String msg) {
		try {
			String hex = Hex.encodeHexString(calculateHash(algo, salt == null ? null : salt.getBytes("UTF-8"),
					msg == null ? null : msg.getBytes("UTF-8")));
			return algo + ":" + hex;
		} catch (UnsupportedEncodingException e) {
			throw new IllegalStateException(e);
		} 
	}
	
	public static String calculateHashWithAlgo(String algo, byte[] bts) {
		byte[] hash = calculateHash(algo, bts, null);
		return formatHashWithAlgo(algo, hash);
	}

	public static String formatHashWithAlgo(String algo, byte[] hash) {
		String hex = Hex.encodeHexString(hash);
		return algo + ":" + hex;
	}

	public static byte[] getHashBytes(String msg) {
		if(msg == null || msg.length() == 0) {
			// special case for empty hash
			return new byte[0];
		}
		int i = msg.lastIndexOf(':');
		String s = i >= 0 ? msg.substring(i + 1) : msg;
		try {
			return Hex.decodeHex(s);
		} catch (DecoderException e) {
			throw new IllegalArgumentException(e);
		}
	}
	

	public static boolean validateHash(String hash, String salt, String msg) {
		int s = hash.indexOf(":");
		if (s == -1) {
			throw new IllegalArgumentException(String.format("Hash %s doesn't contain algorithm of hashing to verify",
					s));
		}
		String v = calculateHashWithAlgo(hash.substring(0, s), salt, msg);
		return hash.equals(v);
	}

	public static byte[] decodeSignature(String digest) {
		try {
			int indexOf = digest.indexOf(DECODE_BASE64 + ":");
			if (indexOf != -1) {
				return Base64.getDecoder().decode(digest.substring(indexOf + DECODE_BASE64.length() + 1).
						getBytes("UTF-8"));
			}
		} catch (UnsupportedEncodingException e) {
			throw new IllegalStateException(e);
		}
		throw new IllegalArgumentException("Unknown format for signature " + digest);
	}

	public static String hexify(byte[] bytes) {
		if(bytes == null || bytes.length == 0) {
			return "";
		}
		return Hex.encodeHexString(bytes);
		
	}

	

}
