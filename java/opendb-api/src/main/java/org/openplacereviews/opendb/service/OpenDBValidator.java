package org.openplacereviews.opendb.service;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.api.ApiController;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpDefinitionBean;
import org.openplacereviews.opendb.ops.auth.LoginOperation;
import org.openplacereviews.opendb.ops.auth.SignUpOperation;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

@Service
public class OpenDBValidator {

	private Gson gson;
    
 	
 	// signature section
 	public static final String F_FORMAT = "format";
 	public static final String F_ALGO = "algo";
 	public static final String F_TYPE = "type";
 	public static final String F_DIGEST = "digest";
 	
 	public static final String JSON_MSG_TYPE = "json";
 	
 	
 	// TODO support operations chain
 	private static class ActiveUser {
 		protected String name;
 		protected OpDefinitionBean signUp;
 		protected List<OpDefinitionBean> logins = new ArrayList<OpDefinitionBean>();
 		
 	}
 	
 	Map<String, ActiveUser> activeUsers = new ConcurrentHashMap<String, ActiveUser>();
	
	public OpenDBValidator() {
		GsonBuilder builder = new GsonBuilder();
		builder.disableHtmlEscaping();
		builder.registerTypeAdapter(OpDefinitionBean.class, new OpDefinitionBean.OpDefinitionBeanAdapter());
		gson = builder.create();
	}
	
	
	
	public void addAuthOperation(String name, OpDefinitionBean op) {
		ActiveUser au = activeUsers.get(name);
		if(au == null) {
			au = new ActiveUser();
			au.name = name;
			activeUsers.put(name, au);
		}
		if(op.getOperationId().equals(SignUpOperation.OP_ID)) {
			if(au.signUp != null) {
				throw new IllegalArgumentException("User was already signed up");
			}
			au.signUp = op;
		} else if(op.getOperationId().equals(SignUpOperation.OP_ID)) {
			au.logins.add(op);
		}
	}
	
	public InputStream getBlock(String id) {
    	return ApiController.class.getResourceAsStream("/bootstrap/ogr-"+id+".json");
    }
	
	public OpBlock parseBootstrapBlock(String id) {
		return gson.fromJson(new InputStreamReader(getBlock(id)), OpBlock.class);
	}
	
	public OpDefinitionBean parseOperation(String opJson) {
		return gson.fromJson(opJson, OpDefinitionBean.class);
	}

	
	public String calculateOperationHash(OpDefinitionBean ob, boolean set) {
		String oldHash = (String) ob.remove(OpDefinitionBean.F_HASH);
		Object sig = ob.remove(OpDefinitionBean.F_SIGNATURE);
		
		String hash = SecUtils.calculateHash(SecUtils.HASH_SHA256, null, gson.toJson(ob));
		if(set) {
			ob.putStringValue(OpDefinitionBean.F_HASH,  hash);
		} else {
			ob.putStringValue(OpDefinitionBean.F_HASH, oldHash);
		}
		ob.putObjectValue(OpDefinitionBean.F_SIGNATURE, sig);
		return hash;
	}
	
	public String toValidateSignatureJson(OpDefinitionBean op) {
		Object sig = op.remove(OpDefinitionBean.F_SIGNATURE);
		String json = gson.toJson(op);
		op.putObjectValue(OpDefinitionBean.F_SIGNATURE, sig);
		return json;
	}

	public String toJson(OpBlock bl) {
		return gson.toJson(bl);
	}
	
	public String toJson(OpDefinitionBean op) {
		return gson.toJson(op);
	}

	

	
	
	public KeyPair getSignUpKeyPairFromPwd(String name, String pwd) throws NoSuchAlgorithmException, InvalidAlgorithmParameterException, UnsupportedEncodingException, InvalidKeyException, SignatureException, InvalidKeySpecException {
		ActiveUser au = activeUsers.get(name);
		if(au == null || au.signUp == null) {
			throw new IllegalStateException(String.format("User '%s' is not signed up", name));
		}
		String algo = au.signUp.getStringValue(SignUpOperation.F_ALGO);
		KeyPair keyPair = SecUtils.generateKeyPairFromPassword(
				algo, au.signUp.getStringValue(SignUpOperation.F_KEYGEN_METHOD), 
				au.signUp.getStringValue(SignUpOperation.F_SALT), pwd);
		KeyPair kp = SecUtils.getKeyPair(algo, null, au.signUp.getStringValue(SignUpOperation.F_PUBKEY));
		if(SecUtils.validateKeyPair(algo, keyPair.getPrivate(), kp.getPublic())) {
			return keyPair;
		}
		return null;
	}
	
	public KeyPair getSignUpKeyPair(String name, 
			String privatekey) throws NoSuchAlgorithmException, InvalidAlgorithmParameterException, UnsupportedEncodingException, InvalidKeyException, SignatureException, InvalidKeySpecException {
		ActiveUser au = activeUsers.get(name);
		if(au == null || au.signUp == null) {
			throw new IllegalStateException(String.format("User '%s' is not signed up", name));
		}
		String algo = au.signUp.getStringValue(SignUpOperation.F_ALGO);
		KeyPair kp = SecUtils.getKeyPair(algo, privatekey, au.signUp.getStringValue(SignUpOperation.F_PUBKEY));
		if(SecUtils.validateKeyPair(algo, kp.getPrivate(), kp.getPublic())) {
			return kp;
		}
		return null;
	}
	
	public KeyPair getLoginKeyPair(String name, String privateKey)
			throws NoSuchAlgorithmException, InvalidAlgorithmParameterException, UnsupportedEncodingException, InvalidKeyException, SignatureException, InvalidKeySpecException {
		ActiveUser au = activeUsers.get(name);
		if(au == null || au.signUp == null) {
			throw new IllegalStateException(String.format("User '%s' is not signed up", name));
		}
		for (OpDefinitionBean op : au.logins) {
			String algo = op.getStringValue(SignUpOperation.F_ALGO);
			KeyPair kp = SecUtils.getKeyPair(algo, privateKey,op.getStringValue(SignUpOperation.F_PUBKEY));
			if (SecUtils.validateKeyPair(algo, kp.getPrivate(), kp.getPublic())) {
				return kp;
			}
		}
		return null;
	}



	public OpDefinitionBean generateHashAndSign(OpDefinitionBean op, KeyPair... keyPair) throws InvalidKeyException,
			SignatureException, NoSuchAlgorithmException, UnsupportedEncodingException {
		calculateOperationHash(op, true);
    	String json = toValidateSignatureJson(op);
    	op.remove(OpDefinitionBean.F_SIGNATURE);
    	if(keyPair.length == 1) {
    		op.putObjectValue(OpDefinitionBean.F_SIGNATURE, getSignature(json, keyPair[0]));
    	} else {
    		List<Map<String, String>> lst = new ArrayList<Map<String,String>>();
    		for(KeyPair k : keyPair) {
    			lst.add(getSignature(json, k));
    		}
    		op.putObjectValue(OpDefinitionBean.F_SIGNATURE, lst);
    	}
    	return op;
	}



	private Map<String, String> getSignature(String json, KeyPair keyPair) throws InvalidKeyException,
			SignatureException, NoSuchAlgorithmException, UnsupportedEncodingException {
		String signature = SecUtils.signMessageWithKeyBase64(keyPair, json, SecUtils.SIG_ALGO_SHA1_EC);
    	Map<String, String> signatureMap = new TreeMap<>();
    	signatureMap.put(F_DIGEST, signature);
    	signatureMap.put(F_TYPE, "json");
    	signatureMap.put(F_ALGO, SecUtils.SIG_ALGO_SHA1_EC);
    	signatureMap.put(F_FORMAT, SecUtils.DECODE_BASE64);
		return signatureMap;
	}
	
	

	public boolean validateSignature(OpDefinitionBean ob) throws InvalidKeySpecException, NoSuchAlgorithmException, InvalidKeyException, SignatureException, UnsupportedEncodingException {
		Map<String, String> sig = ob.getStringMap(OpDefinitionBean.F_SIGNATURE);
		if(sig == null) {
			return false;
		}
		String name = ob.getSignedBy();
		if(name == null) {
			return false;
		}
		String sigAlgo = sig.get(F_ALGO);
		byte[] signature = SecUtils.decodeSignature(sig.get(F_FORMAT), sig.get(F_DIGEST));
		String msgType = sig.get(F_TYPE);
		String msg;
		if(!JSON_MSG_TYPE.equals(msgType)) {
			return false;
		} else {
			msg = toValidateSignatureJson(ob);
		}
		if(ob.getOperationId().equals(SignUpOperation.OP_ID)) {
			// signup operation is validated by itself
			KeyPair kp = getPublicKeyFromOp(ob);
			return SecUtils.validateSignature(kp, msg, sigAlgo, signature); 
		} else {
			ActiveUser au = activeUsers.get(name);
			if(au != null && au.signUp != null) {
				// login operation is validated only by sign up
				if(ob.getOperationId().equals(LoginOperation.OP_ID)) {
					KeyPair kp = getPublicKeyFromOp(au.signUp);
					return SecUtils.validateSignature(kp, msg, sigAlgo, signature);
				} else {
					// other operations are validated by any login
					for (OpDefinitionBean login : au.logins) {
						KeyPair kp = getPublicKeyFromOp(login);
						boolean vl = SecUtils.validateSignature(kp, msg, sigAlgo, signature);
						if (vl) {
							return vl;
						}
					}
				}
			}
		}
		return false;
		
	}

	private KeyPair getPublicKeyFromOp(OpDefinitionBean ob) throws InvalidKeySpecException, NoSuchAlgorithmException {
		String signUpalgo = ob.getStringValue(F_ALGO);
		String pbKey = ob.getStringValue(SignUpOperation.F_PUBKEY);
		return SecUtils.getKeyPair(signUpalgo, null, pbKey);
	}

}
