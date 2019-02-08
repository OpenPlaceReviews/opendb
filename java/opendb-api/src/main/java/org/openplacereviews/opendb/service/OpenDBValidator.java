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
import java.util.Map;
import java.util.TreeMap;

import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.api.ApiController;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpDefinitionBean;
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
 	
 	
	
	public OpenDBValidator() {
		GsonBuilder builder = new GsonBuilder();
		builder.disableHtmlEscaping();
		builder.registerTypeAdapter(OpDefinitionBean.class, new OpDefinitionBean.OpDefinitionBeanAdapter());
		gson = builder.create();
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
		
		String hash = "sha256:" + SecUtils.calculateSha256(gson.toJson(ob));
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

	public OpDefinitionBean generateSignatureFromPwd(String json, String pwd) throws NoSuchAlgorithmException, InvalidAlgorithmParameterException, UnsupportedEncodingException, InvalidKeyException, SignatureException {
		OpDefinitionBean op = parseOperation(json);
    	String hash = calculateOperationHash(op, true);
		KeyPair keyPair = SecUtils.generateEC256K1KeyPairFromPassword(op.getStringValue(
    			SignUpOperation.F_SALT), pwd, SignUpOperation.F_KEYGEN_METHOD);
    	op.remove(OpDefinitionBean.F_SIGNATURE);
    	String signature = SecUtils.signMessageWithKeyBase64(keyPair, json, SecUtils.SIG_ALGO_SHA1_EC);
    	OpDefinitionBean sig = new OpDefinitionBean();
    	sig.putStringValue(OpDefinitionBean.F_HASH, hash);
    	sig.putStringValue(SignUpOperation.F_PUBKEY_FORMAT, SecUtils.DECODE_BASE64 + ":" + keyPair.getPublic().getFormat());
    	sig.putStringValue(SignUpOperation.F_PUBKEY, SecUtils.encodeBase64(keyPair.getPublic().getEncoded()));
    	Map<String, String> signatureMap = new TreeMap<>();
    	signatureMap.put(F_DIGEST, signature);
    	signatureMap.put(F_TYPE, "json");
    	signatureMap.put(F_ALGO, SecUtils.SIG_ALGO_SHA1_EC);
    	signatureMap.put(F_FORMAT, SecUtils.DECODE_BASE64);
    	sig.putObjectValue(OpDefinitionBean.F_SIGNATURE, signatureMap);		
    	return sig;
	}
	
	
	public OpDefinitionBean generateHashAndSignatureFromPwd(OpDefinitionBean op, KeyPair keyPair) throws NoSuchAlgorithmException, InvalidAlgorithmParameterException, UnsupportedEncodingException, InvalidKeyException, SignatureException {
    	calculateOperationHash(op, true);
    	String json = toValidateSignatureJson(op);
    	op.remove(OpDefinitionBean.F_SIGNATURE);
    	String signature = SecUtils.signMessageWithKeyBase64(keyPair, json, SecUtils.SIG_ALGO_SHA1_EC);
    	Map<String, String> signatureMap = new TreeMap<>();
    	signatureMap.put(F_DIGEST, signature);
    	signatureMap.put(F_TYPE, "json");
    	signatureMap.put(F_ALGO, SecUtils.SIG_ALGO_SHA1_EC);
    	signatureMap.put(F_FORMAT, SecUtils.DECODE_BASE64);
    	op.putObjectValue(OpDefinitionBean.F_SIGNATURE, signatureMap);		
    	return op;
	}

	public boolean validateSignature(OpDefinitionBean ob) throws InvalidKeySpecException, NoSuchAlgorithmException, InvalidKeyException, SignatureException, UnsupportedEncodingException {
		// TODO
		Map<String, String> sig = ob.getStringMap(OpDefinitionBean.F_SIGNATURE);
		if(sig == null) {
			return false;
		}
		String algo = ob.getStringValue(F_ALGO);
		String pubformat = ob.getStringValue(SignUpOperation.F_PUBKEY_FORMAT);
		String pbKey = ob.getStringValue(SignUpOperation.F_PUBKEY);
		KeyPair kp = SecUtils.getKeyPair(algo, null, null, pubformat, pbKey);
		byte[] signature = SecUtils.decodeSignature(sig.get(F_FORMAT), sig.get(F_DIGEST));
		boolean validate = SecUtils.validateSignature(kp, toValidateSignatureJson(ob), 
				sig.get(F_ALGO), signature);
		return validate;
	}


}
