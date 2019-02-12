package org.openplacereviews.opendb.service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.KeyPair;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.openplacereviews.opendb.FailedVerificationException;
import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.Utils;
import org.openplacereviews.opendb.api.ApiController;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpDefinitionBean;
import org.openplacereviews.opendb.ops.OperationsRegistry;
import org.openplacereviews.opendb.ops.auth.LoginOperation;
import org.openplacereviews.opendb.ops.auth.SignUpOperation;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

@Service
public class OpenDBUsersRegistry {

	private Gson gson;
    
 	// signature section
 	public static final String F_ALGO = "algo";
 	public static final String F_DIGEST = "digest";
 	
 	public static final String JSON_MSG_TYPE = "json";


	private ActiveUsersContext blockUsers;
	private ActiveUsersContext queueUsers;
 	
	public OpenDBUsersRegistry() {
		GsonBuilder builder = new GsonBuilder();
		builder.disableHtmlEscaping();
		builder.registerTypeAdapter(OpDefinitionBean.class, new OpDefinitionBean.OpDefinitionBeanAdapter());
		gson = builder.create();
		blockUsers = new ActiveUsersContext(null);
		queueUsers = new ActiveUsersContext(blockUsers);
	}
	
	
	// operations to parse / format related
	public InputStream getBlock(String id) {
    	return ApiController.class.getResourceAsStream("/bootstrap/ogr-"+id+".json");
    }
	
	public OpBlock parseBootstrapBlock(String id) {
		return gson.fromJson(new InputStreamReader(getBlock(id)), OpBlock.class);
	}
	
	public OpDefinitionBean parseOperation(String opJson) {
		return gson.fromJson(opJson, OpDefinitionBean.class);
	}
	public String toJson(OpBlock bl) {
		return gson.toJson(bl);
	}
	
	public String toJson(OpDefinitionBean op) {
		return gson.toJson(op);
	}

	public String calculateMerkleTreeHash(OpBlock op) {
		List<byte[]> hashes = new ArrayList<byte[]>();
		for(OpDefinitionBean o: op.getOperations()) {
			byte[] hashBytes = SecUtils.getHashBytes(o.getHash());
			hashes.add(hashBytes);
		}
		return calculateMerkleTreeInPlaceHash(SecUtils.HASH_SHA256, hashes);
	}
	
	public String calculateSigMerkleTreeHash(OpBlock op) {
		List<byte[]> hashes = new ArrayList<byte[]>();
		for(OpDefinitionBean o: op.getOperations()) {
			byte[] hashBytes = SecUtils.getHashBytes(o.getSignatureHash());
			hashes.add(hashBytes);
		}
		return calculateMerkleTreeInPlaceHash(SecUtils.HASH_SHA256, hashes);
	}
	
	private String calculateMerkleTreeInPlaceHash(String algo, List<byte[]> hashes) {
		if(hashes.size() == 0) {
			return "";
		}
		if(hashes.size() <= 1) {
			return SecUtils.formatHashWithAlgo(algo, hashes.get(0));
		}
		List<byte[]> nextLevel = new ArrayList<byte[]>();
		for(int i = 0; i <hashes.size(); i+=2) {
			byte[] hsh = SecUtils.calculateHash(algo, hashes.get(i), i + 1 < hashes.size() ? hashes.get(i+1) : hashes.get(i));
			nextLevel.add(hsh);
		}
		return calculateMerkleTreeInPlaceHash(algo, nextLevel);
	}


	// hash and signature operations
	public String calculateOperationHash(OpDefinitionBean ob, boolean set) {
		String oldHash = (String) ob.remove(OpDefinitionBean.F_HASH);
		String sigHash = (String) ob.remove(OpDefinitionBean.F_SIGNATURE_HASH);
		Object sig = ob.remove(OpDefinitionBean.F_SIGNATURE);
		
		String hash = JSON_MSG_TYPE + ":" + SecUtils.calculateHashWithAlgo(SecUtils.HASH_SHA256, null, gson.toJson(ob));
		if(set) {
			ob.putStringValue(OpDefinitionBean.F_HASH, hash);
		} else {
			ob.putStringValue(OpDefinitionBean.F_HASH, oldHash);
		}
		ob.putObjectValue(OpDefinitionBean.F_SIGNATURE, sig);
		ob.putObjectValue(OpDefinitionBean.F_SIGNATURE_HASH, sigHash);
		return hash;
	}
	
	
	public String calculateSigOperationHash(OpDefinitionBean ob) {
		ByteArrayOutputStream bytesSigHash = new ByteArrayOutputStream();
		try {
			bytesSigHash.write(SecUtils.getHashBytes(ob.getHash()));

			if (ob.hasOneSignature()) {
				String dgst = ob.getStringMap(OpDefinitionBean.F_SIGNATURE).get(F_DIGEST);
				bytesSigHash.write(SecUtils.decodeSignature(dgst));
			} else {
				List<Map<String, String>> lst = ob.getListStringMap(OpDefinitionBean.F_SIGNATURE);
				for (Map<String, String> m : lst) {
					String dgst = m.get(F_DIGEST);
					bytesSigHash.write(SecUtils.decodeSignature(dgst));
				}
			}
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
    	return SecUtils.calculateHashWithAlgo(SecUtils.HASH_SHA256, bytesSigHash.toByteArray());
	}
	
	
	public OpDefinitionBean generateHashAndSign(OpDefinitionBean op, KeyPair... keyPair) throws FailedVerificationException {
		String hsh = calculateOperationHash(op, true);
		byte[] hashBytes = SecUtils.getHashBytes(hsh);
		ByteArrayOutputStream bytesSigHash = new ByteArrayOutputStream();
		try {
			bytesSigHash.write(hashBytes);
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
    	op.remove(OpDefinitionBean.F_SIGNATURE);
    	op.remove(OpDefinitionBean.F_SIGNATURE_HASH);
    	if(keyPair.length == 1) {
    		op.putObjectValue(OpDefinitionBean.F_SIGNATURE, getSignature(hashBytes, keyPair[0], bytesSigHash));
    	} else {
    		List<Map<String, String>> lst = new ArrayList<Map<String,String>>();
    		for(KeyPair k : keyPair) {
    			lst.add(getSignature(hashBytes, k, bytesSigHash));
    		}
    		op.putObjectValue(OpDefinitionBean.F_SIGNATURE, lst);
    	}
    	// signature hash is combination of all hash bytes
    	op.putStringValue(OpDefinitionBean.F_SIGNATURE_HASH,
    			SecUtils.calculateHashWithAlgo(SecUtils.HASH_SHA256, bytesSigHash.toByteArray()));
    	return op;
	}



	private Map<String, String> getSignature(byte[] hash, KeyPair keyPair, ByteArrayOutputStream out) throws FailedVerificationException {
		String signature = SecUtils.signMessageWithKeyBase64(keyPair, hash, SecUtils.SIG_ALGO_NONE_EC, out);
    	Map<String, String> signatureMap = new TreeMap<>();
    	signatureMap.put(F_DIGEST, signature);
    	signatureMap.put(F_ALGO, SecUtils.SIG_ALGO_NONE_EC);
		return signatureMap;
	}
	

	public boolean validateSignatures(ActiveUsersContext ctx, OpDefinitionBean ob) throws FailedVerificationException {
		if (ob.hasOneSignature()) {
			Map<String, String> sig = ob.getStringMap(OpDefinitionBean.F_SIGNATURE);
				boolean validate = validateSignature(ctx, ob, sig, ob.getSignedBy());
				if(!validate) {
					return false;
				}
		} else {
			List<Map<String, String>> sigs = ob.getListStringMap(OpDefinitionBean.F_SIGNATURE);
			for (int i = 0; i < sigs.size(); i++) {
				Map<String, String> sig = sigs.get(i);
					boolean validate = validateSignature(ctx, ob, sig, i == 0 ? ob.getSignedBy() : ob.getOtherSignedBy()
							.get(i - 1));
					if(!validate) {
						return false;
					}
			}
		}
		return true;
	}
	
	public boolean validateHash(OpDefinitionBean o) {
		return Utils.equals(calculateOperationHash(o, false), o.getHash());
	}
	
	public boolean validateSignatureHash(OpDefinitionBean o) {
		return Utils.equals(calculateSigOperationHash(o), o.getSignatureHash());
	}
	
	public boolean validateSignature(ActiveUsersContext ctx, OpDefinitionBean ob, Map<String, String> sig, String name) throws FailedVerificationException {
		if (sig == null) {
			return false;
		}
		if (name == null) {
			return false;
		}
		String sigAlgo = sig.get(F_ALGO);
		byte[] txHash = SecUtils.getHashBytes(ob.getHash());		
		byte[] signature = SecUtils.decodeSignature(sig.get(F_DIGEST));
		if (ob.getOperationId().equals(SignUpOperation.OP_ID) && ob.getStringValue(SignUpOperation.F_NAME).equals(name)) {
			// signup operation is validated by itself
			KeyPair kp = getPublicKeyFromOp(ob);
			return SecUtils.validateSignature(kp, txHash, sigAlgo, signature);
		} else if (ob.getOperationId().equals(LoginOperation.OP_ID)
				&& ob.getStringValue(SignUpOperation.F_NAME).equals(name)) {
			// login operation is validated only by sign up
			OpDefinitionBean signUp = ctx.getSignUpOperation(name);
			KeyPair kp = getPublicKeyFromOp(signUp);
			return SecUtils.validateSignature(kp, txHash, sigAlgo, signature);
		} else {
			List<OpDefinitionBean> logins = ctx.getLoginOperations(name, new ArrayList<OpDefinitionBean>());
			// other operations are validated by any login
			for (OpDefinitionBean login : logins) {
				KeyPair kp = getPublicKeyFromOp(login);
				boolean vl = SecUtils.validateSignature(kp, txHash, sigAlgo, signature);
				if (vl) {
					return vl;
				}
			}
		}
		return false;
	}

	private KeyPair getPublicKeyFromOp(OpDefinitionBean ob) throws FailedVerificationException {
		if(ob == null) {
			return null;
		}
		String signUpalgo = ob.getStringValue(F_ALGO);
		String pbKey = ob.getStringValue(SignUpOperation.F_PUBKEY);
		return SecUtils.getKeyPair(signUpalgo, null, pbKey);
	}

	

	// Users related operations
	public ActiveUsersContext getBlockUsers() {
		return blockUsers;
	}
	
	public ActiveUsersContext getQueueUsers() {
		return queueUsers;
	}


 	protected static class ActiveUser {
 		protected String name;
 		protected OpDefinitionBean signUp;
 		protected List<OpDefinitionBean> logins = new ArrayList<OpDefinitionBean>();
 		
 	}
 	
 	
 	public static class ActiveUsersContext {
 		
		ActiveUsersContext parent;
		Map<String, ActiveUser> users = new ConcurrentHashMap<String, ActiveUser>();

		public ActiveUsersContext(ActiveUsersContext parent) {
			this.parent = parent;
		}

		public List<OpDefinitionBean> getLoginOperations(String name, List<OpDefinitionBean> list) {
			if (parent != null) {
				parent.getLoginOperations(name, list);
			}
			ActiveUser au = users.get(name);
			if (au != null && au.signUp != null) {
				list.addAll(au.logins);
			}
			return list;
		}

 		public boolean removeAuthOperation(String name, OpDefinitionBean op, boolean deep) {
 			String h = op.getHash();
 			if(h == null) {
 				return false;
 			}
 			boolean deleted = false;
 			if(deep && parent != null) {
 				deleted = parent.removeAuthOperation(name, op, deep);
 			}
 			ActiveUser au = users.get(name);
			if (au != null && au.signUp != null) {
				if(Utils.equals(au.signUp.getHash(), h)) {
					au.signUp = null;
					deleted = true;
				}
				Iterator<OpDefinitionBean> it = au.logins.iterator();
				while(it.hasNext()) {
					OpDefinitionBean o = it.next();
					if(Utils.equals(o.getHash(), h)) {
						it.remove();
						deleted = true;
					}
				}
			}
			return deleted;
 		}
 		
		public boolean addAuthOperation(OpDefinitionBean op) {
			if(!op.getType().equals(OperationsRegistry.OP_TYPE_AUTH)) {
				return false;
			}
			String name = op.getStringValue(SignUpOperation.F_NAME);
			ActiveUser au = users.get(name);
			if (au == null) {
				au = new ActiveUser();
				au.name = name;
				users.put(name, au);
			}
			if (op.getOperationId().equals(SignUpOperation.OP_ID)) {
				OpDefinitionBean sop = getSignUpOperation(name);
				if (sop != null) {
					throw new IllegalArgumentException("User was already signed up");
				}
				au.signUp = op;
				return true;
			} else if (op.getOperationId().equals(LoginOperation.OP_ID)) {
				au.logins.add(op);
				return true;
			}
			return false;
		}
 		
 		public OpDefinitionBean getSignUpOperation(String name) {
 			OpDefinitionBean op = null;
 			if(parent != null) {
 				 op = parent.getSignUpOperation(name);
 			}
 			if(op == null) {
 				ActiveUser au = users.get(name);
 	 			if(au == null || au.signUp == null) {
 	 				return null;
 	 			}	
 	 			op = au.signUp;
 			}
 			return op;
 		}
 		
 		public KeyPair getSignUpKeyPairFromPwd(String name, String pwd) throws FailedVerificationException {
 			OpDefinitionBean op = getSignUpOperation(name);
 			if(op == null) {
 				return null;
 			}
 			String algo = op.getStringValue(SignUpOperation.F_ALGO);
 			KeyPair keyPair = SecUtils.generateKeyPairFromPassword(
 					algo, op.getStringValue(SignUpOperation.F_KEYGEN_METHOD), 
 					op.getStringValue(SignUpOperation.F_SALT), pwd);
 			KeyPair kp = SecUtils.getKeyPair(algo, null, op.getStringValue(SignUpOperation.F_PUBKEY));
 			if(SecUtils.validateKeyPair(algo, keyPair.getPrivate(), kp.getPublic())) {
 				return keyPair;
 			}
 			return null;
 		}
 		
 	
 		
 		public KeyPair getSignUpKeyPair(String name, String privatekey) throws FailedVerificationException {
 			OpDefinitionBean op = getSignUpOperation(name);
 			if(op == null) {
 				return null;
 			}
 			String algo = op.getStringValue(SignUpOperation.F_ALGO);
 			KeyPair kp = SecUtils.getKeyPair(algo, privatekey, op.getStringValue(SignUpOperation.F_PUBKEY));
 			if(SecUtils.validateKeyPair(algo, kp.getPrivate(), kp.getPublic())) {
 				return kp;
 			}
 			return null;
 		}
 		
 		public KeyPair getLoginKeyPair(String name, String privateKey) throws FailedVerificationException {
 			ActiveUser au = users.get(name);
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


 	}


	
 	
}
