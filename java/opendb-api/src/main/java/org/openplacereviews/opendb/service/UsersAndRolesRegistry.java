package org.openplacereviews.opendb.service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.KeyPair;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.DBConstants;
import org.openplacereviews.opendb.FailedVerificationException;
import org.openplacereviews.opendb.OUtils;
import org.openplacereviews.opendb.OpenDBServer;
import org.openplacereviews.opendb.OpenDBServer.MetadataDb;
import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpDefinitionBean;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.stereotype.Service;

@Service
public class UsersAndRolesRegistry {
	protected static final Log LOGGER = LogFactory.getLog(OpenDBServer.class);
    
	public static final String OP_SIGNUP_ID = "signup";
 	public static final String OP_LOGIN_ID = "login";
 	public static final String F_DIGEST = "digest"; // signature
 	
 	
 	public static final String F_ALGO = "algo"; // login, signup, signature
 	public static final String F_PUBKEY = "pubkey"; // login, signup
	public static final String F_NAME = "name"; // login, signup - name, login has with purpose like 'name:purpose', 
	public static final String F_SALT = "salt"; // signup (salt used for pwd or oauthid_hash)
	public static final String F_AUTH_METHOD = "auth_method"; // signup - pwd, oauth, provided
	public static final String F_OAUTH_PROVIDER = "oauth_provider"; // signup 
	public static final String F_OAUTHID_HASH = "oauthid_hash"; // hash with salt of the oauth_id 
	public static final String F_KEYGEN_METHOD = "keygen_method"; // optional login, signup (for pwd important)
	public static final String F_DETAILS = "details"; // signup
	
 	// transient - not stored in blockchain
 	public static final String F_PRIVATEKEY = "privatekey"; // private key to return to user
 	public static final String F_UID = "uid"; // user identifier

	public static final String METHOD_OAUTH = "oauth";
	public static final String METHOD_PWD = "pwd";
	public static final String METHOD_PROVIDED = "provided";
 	
 	public static final String JSON_MSG_TYPE = "json";
 	
 	// this char is not allowed in the nickname!
 	public static final char USER_LOGIN_CHAR = ':';

 	@Autowired
	private JsonFormatter formatter;
 	
 	@Autowired
	private DBDataManager dbManager;
 	
 	@Autowired
	private JdbcTemplate jdbcTemplate;


	private ActiveUsersContext blockUsers = new ActiveUsersContext(null);
	private ActiveUsersContext queueUsers = new ActiveUsersContext(blockUsers);
 	
	
	public void init(MetadataDb metadataDB) {
		LOGGER.info("... User database. Load all users (should be changed in future)...");
		if(metadataDB.tablesSpec.containsKey(DBConstants.USERS_TABLE)) {
			loadUser(blockUsers, "");
		}
		if(metadataDB.tablesSpec.containsKey(DBConstants.LOGINS_TABLE)) {
			loadLogins(blockUsers, "");
		}
		LOGGER.info(String.format("+++ User database is inititialized. Loaded %d users.", 
				blockUsers.users.size()));
	}
	
	public String getSignupDescription() {
		return "This operation signs up new user in DB."+
		"<br>This operation must be signed by signup key itself and the login key of the server that can signup users." +
		"<br>Supported fields:"+
		"<br>'name' : unique nickname" +
		"<br>'auth_method' : authorization method (oauth, pwd, provided)" +
		"<br>'pub_key' : public key for assymetric crypthograph" +
		"<br>'algo' : algorithm for assymetric crypthograph" +
		"<br>'keygen_method' : keygen is specified when pwd is used" +
		"<br>'oauthid_hash' : hash for oauth id which is calculated with 'salt'" +
		"<br>'oauth_provider' : oauth provider such as osm, fb, google" +
		"<br>'details' : json with details for spoken languages, avatar, country" +
		"<br>list of other fields";
	}
	
	public String getLoginDescription() {
		return "This operation logins an existing user to a specific 'site' (named login). " + 
	    "In case user was logged in under such name the previous login key pair will become invalid." +
		"<br>This operation must be signed by signup key of the user." +
	    "<br>Supported fields:"+
	    "<br>'name' : unique name for a user of the site or purpose of login" +
		"<br>'pub_key' : public key for assymetric crypthograph" +
		"<br>'algo' : algorithm for assymetric crypthograph" +
		"<br>'keygen_method' : later could be used to explain how the key was calculated" +
		"<br>'details' : json with details" +
		"<br>list of other fields";
	}

	
	private static boolean isAllowedNicknameSymbol(char c) {
		return c == ' ' || c == '$'  || c == '_' ||  
				c == '.' || c == '-' ;
	}
	
	public static boolean validateNickname(String name) {
		if(name.trim().length() == 0) {
			return false;
		}
		for(int i = 0; i < name.length(); i++) {
			char c = name.charAt(i);
			if(!Character.isLetter(c) && !Character.isDigit(c) && !isAllowedNicknameSymbol(c)) {
				return false;
			}
		}
		return true;
	}




	private void loadUser(ActiveUsersContext uctx, String condition) {
		jdbcTemplate.query("SELECT name, uid, algo, auth_method, keygen_method, "
				+ "oauth_provider, oauthid_hash, pubkey, salt, userdetails from  " + DBConstants.USERS_TABLE + condition, new RowCallbackHandler() {

			@Override
			public void processRow(ResultSet rs) throws SQLException {
				int col = 1;
				String name = rs.getString(col++);
				OpDefinitionBean op = new OpDefinitionBean();
				op.putStringValue(F_NAME, name);
				op.putObjectValue(F_UID, rs.getInt(col++));
				op.putStringValue(F_ALGO, rs.getString(col++));
				op.putStringValue(F_AUTH_METHOD, rs.getString(col++));
				op.putStringValue(F_KEYGEN_METHOD, rs.getString(col++));
				op.putStringValue(F_OAUTH_PROVIDER, rs.getString(col++));
				op.putStringValue(F_OAUTHID_HASH, rs.getString(col++));
				op.putStringValue(F_PUBKEY, rs.getString(col++));
				op.putStringValue(F_SALT, rs.getString(col++));
				op.putObjectValue(F_DETAILS, formatter.fromJsonToJsonObject(rs.getString(col++)));
				ActiveUser user = blockUsers.getOrCreateActiveUser(name);
				user.timestampAccess = System.currentTimeMillis();
				user.signUp = op;
			}
		});
	}
	
	private void loadLogins(ActiveUsersContext uctx, String condition) {
		jdbcTemplate.query("SELECT login, uid, name purpose, algo, pubkey from " + DBConstants.LOGINS_TABLE + condition, new RowCallbackHandler() {

			@Override
			public void processRow(ResultSet rs) throws SQLException {
				int col = 1;
				OpDefinitionBean op = new OpDefinitionBean();
				op.putStringValue(F_NAME, rs.getString(col++));
				op.putObjectValue(F_UID, rs.getInt(col++));
				String name = rs.getString(col++);
				String purpose = rs.getString(col++);
				op.putStringValue(F_ALGO, rs.getString(col++));
				op.putStringValue(F_PUBKEY, rs.getString(col++));
				ActiveUser user = blockUsers.getOrCreateActiveUser(name);
				user.logins.put(purpose , op);
				user.timestampAccess = System.currentTimeMillis();
				
			}
		});
	}
	
	
	
	public static String getSiteFromUser(String name) {
		int i = name.indexOf(USER_LOGIN_CHAR);
		return i >= 0 ? name.substring(i + 1) : "";
	}
	
	public static String getNicknameFromUser(String name) {
		int i = name.indexOf(USER_LOGIN_CHAR);
		return i >= 0 ? name.substring(0, i) : name;
	}
	
	public static String getUserFromNicknameAndSite(String nickname, String site) {
		return nickname + USER_LOGIN_CHAR + site;
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
		for (int i = 0; i < hashes.size(); i += 2) {
			byte[] hsh = SecUtils.calculateHash(algo, hashes.get(i), i + 1 < hashes.size() ? hashes.get(i + 1) : hashes.get(i));
			nextLevel.add(hsh);
		}
		return calculateMerkleTreeInPlaceHash(algo, nextLevel);
	}


	// hash and signature operations
	public String calculateOperationHash(OpDefinitionBean ob, boolean set) {
		String oldHash = (String) ob.remove(OpDefinitionBean.F_HASH);
		String sigHash = (String) ob.remove(OpDefinitionBean.F_SIGNATURE_HASH);
		Object sig = ob.remove(OpDefinitionBean.F_SIGNATURE);
		String hash = JSON_MSG_TYPE + ":" + SecUtils.calculateHashWithAlgo(SecUtils.HASH_SHA256, null, formatter.toJson(ob));
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
	
	public String validateRoles(ActiveUsersContext ctx, OpDefinitionBean ob) {
		Set<String> signatures = new TreeSet<String>();
		signatures.add(ob.getSignedBy());
		if(ob.getOtherSignedBy() != null) {
			signatures.addAll(ob.getOtherSignedBy());
		}
		if (ob.getOperationName().equals(UsersAndRolesRegistry.OP_SIGNUP_ID)) {
			if(!signatures.contains(ob.getStringValue(F_NAME))) {
				return "Signup operation must be signed by itself";
			}
		}
		if (ob.getOperationName().equals(OP_LOGIN_ID)) {
			String loginName = ob.getStringValue(F_NAME);
			if(loginName.indexOf(USER_LOGIN_CHAR) == -1 ) {
				return "Login should specify a site or a purpose name to login";
			}
			if(!signatures.contains(getNicknameFromUser(loginName))) {
				return "Login operation must be signed by sign up key";
			}
		}
		return null;
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
				if (!validate) {
					return false;
				}
			}
		}
		return true;
	}
	
	public boolean validateHash(OpDefinitionBean o) {
		return OUtils.equals(calculateOperationHash(o, false), o.getHash());
	}
	
	public boolean validateSignatureHash(OpDefinitionBean o) {
		return OUtils.equals(calculateSigOperationHash(o), o.getSignatureHash());
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
		KeyPair kp ;
		if (ob.getOperationName().equals(UsersAndRolesRegistry.OP_SIGNUP_ID) && ob.getStringValue(F_NAME).equals(name)) {
			// signup operation is validated by itself
			kp = ctx.getKeyPairFromOp(ob, null);
		} else {
			kp = ctx.getPublicKeyPair(name);
		}
		return SecUtils.validateSignature(kp, txHash, sigAlgo, signature);
	}



	// Users related operations
	public ActiveUsersContext getBlockUsers() {
		return blockUsers;
	}
	
	public ActiveUsersContext getQueueUsers() {
		return queueUsers;
	}


 	protected static class ActiveUser {
 		protected long timestampAccess;
		protected String name;
 		protected OpDefinitionBean signUp;
 		protected Map<String, OpDefinitionBean> logins = new TreeMap<String, OpDefinitionBean>();
 		
 	}
 	
 	
 	public static class ActiveUsersContext {
 		
		ActiveUsersContext parent;
		Map<String, ActiveUser> users = new ConcurrentHashMap<String, ActiveUser>();

		public ActiveUsersContext(ActiveUsersContext parent) {
			this.parent = parent;
		}
		
		public void clear() {
			users.clear();
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
				if(OUtils.equals(au.signUp.getHash(), h)) {
					au.signUp = null;
					deleted = true;
				}
				for(OpDefinitionBean o : au.logins.values()) {
					if(OUtils.equals(o.getHash(), h)) {
						au.logins.remove(getSiteFromUser(name));
						deleted = true;
						break;
					}
				}
			}
			return deleted;
 		}
 		
		public boolean addAuthOperation(OpDefinitionBean op) {
			if(!op.getOperationType().equals(OperationsRegistry.OP_TYPE_SYS) || 
					!(
					op.getOperationName().equals(OperationsRegistry.OP_LOGIN) || 
					op.getOperationName().equals(OperationsRegistry.OP_SIGNUP)
					)) {
				return false;
			}
			String name = op.getStringValue(UsersAndRolesRegistry.F_NAME);
			String nickname = getNicknameFromUser(name);
			String site = getSiteFromUser(name);
			ActiveUser au = getOrCreateActiveUser(nickname);
			if (op.getOperationName().equals(UsersAndRolesRegistry.OP_SIGNUP_ID)) {
				OpDefinitionBean sop = getSignUpOperation(nickname);
				if (sop != null) {
					throw new IllegalArgumentException("User was already signed up");
				}
				au.signUp = op;
				return true;
			} else if (op.getOperationName().equals(OP_LOGIN_ID)) {
				au.logins.put(site, op);
				return true;
			}
			return false;
		}

		private ActiveUser getOrCreateActiveUser(String name) {
			ActiveUser au = users.get(name);
			if (au == null) {
				au = new ActiveUser();
				au.name = name;
				users.put(name, au);
			}
			return au;
		}
		
		public OpDefinitionBean getLoginOperation(String name) {
			ActiveUser au = users.get(getNicknameFromUser(name));
			if (au != null && au.logins.containsKey(getSiteFromUser(name))) {
				return au.logins.get(getSiteFromUser(name));
			}
			if (parent != null) {
				return parent.getLoginOperation(name);
			}
			return null;
		}
 		
 		public OpDefinitionBean getSignUpOperation(String name) {
 			OpDefinitionBean op = null;
 			if(parent != null) {
 				 op = parent.getSignUpOperation(name);
 			}
 			if(op == null) {
 				ActiveUser au = users.get(getNicknameFromUser(name));
 	 			if(au != null) {
 	 				return au.signUp;
 	 			}	
 			}
 			return op;
 		}
 		
 		public KeyPair getSignUpKeyPairFromPwd(String name, String pwd) throws FailedVerificationException {
 			OpDefinitionBean op = getSignUpOperation(name);
 			if(op == null) {
 				return null;
 			}
 			String algo = op.getStringValue(UsersAndRolesRegistry.F_ALGO);
 			KeyPair keyPair = SecUtils.generateKeyPairFromPassword(
 					algo, op.getStringValue(UsersAndRolesRegistry.F_KEYGEN_METHOD), 
 					op.getStringValue(UsersAndRolesRegistry.F_SALT), pwd);
 			KeyPair kp = SecUtils.getKeyPair(algo, null, op.getStringValue(UsersAndRolesRegistry.F_PUBKEY));
 			if(SecUtils.validateKeyPair(algo, keyPair.getPrivate(), kp.getPublic())) {
 				return keyPair;
 			}
 			return null;
 		}
 		
 		public KeyPair getPublicSignUpKeyPair(String name) throws FailedVerificationException {
 			return getSignUpKeyPair(name, null);
 		}
 		
 		public KeyPair getPublicKeyPair(String name) throws FailedVerificationException {
 			if(name.indexOf(USER_LOGIN_CHAR) == -1) {
 				return getSignUpKeyPair(name, null);
 			}
 			return getLoginKeyPair(name, null);
 		}
 		
 		public KeyPair getSignUpKeyPair(String name, String privatekey) throws FailedVerificationException {
 			OpDefinitionBean op = getSignUpOperation(name);
 			if(op == null) {
 				return null;
 			}
 			return getKeyPairFromOp(op, privatekey);
 		}


		private KeyPair getKeyPairFromOp(OpDefinitionBean op, String privatekey) throws FailedVerificationException {
			String algo = op.getStringValue(UsersAndRolesRegistry.F_ALGO);
 			KeyPair kp = SecUtils.getKeyPair(algo, privatekey, op.getStringValue(UsersAndRolesRegistry.F_PUBKEY));
 			if(privatekey == null || SecUtils.validateKeyPair(algo, kp.getPrivate(), kp.getPublic())) {
 				return kp;
 			}
 			return null;
		}
 		
 		public KeyPair getPublicLoginKeyPair(String name) throws FailedVerificationException {
 			return getLoginKeyPair(name, null);
 		}
 		
		public KeyPair getLoginKeyPair(String name, String privateKey) throws FailedVerificationException {
			OpDefinitionBean op = getLoginOperation(name);
			if(op == null) {
 				return null;
 			}
 			return getKeyPairFromOp(op, privateKey);
		}


 	}

 	
}
