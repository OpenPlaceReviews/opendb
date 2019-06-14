package org.openplacereviews.opendb.ops;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.KeyPair;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.OUtils;
import org.openplacereviews.opendb.util.OpExprEvaluator;
import org.openplacereviews.opendb.util.OpExprEvaluator.EvaluationContext;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

/**
 * State less blockchain rules to validate roles and calculate hashes
 */
public class OpBlockchainRules {

	protected static final Log LOGGER = LogFactory.getLog(OpBlockchainRules.class);
	
	// it is questionable whether size validation should be part of blockchain or not
	public static final int MAX_BLOCK_SIZE_OPS = 1024;
	public static final int MAX_BLOCK_SIZE_MB = 1 << 20;
	public static final int MAX_OP_SIZE_MB = MAX_BLOCK_SIZE_MB / 4;
	
	public static final int BLOCK_VERSION = 1;
	// not used by this implementation
	private static final String BLOCK_CREATION_DETAILS = "";
	private static final long BLOCK_EXTRA = 0;

	// system operations
	public static final String OP_TYPE_SYS  = "sys.";
	// auth
	public static final String OP_LOGIN = OP_TYPE_SYS + "login";
	public static final String OP_SIGNUP = OP_TYPE_SYS + "signup";
	// roles / validation
	public static final String OP_ROLE = OP_TYPE_SYS + "role";
	public static final String OP_GRANT = OP_TYPE_SYS + "grant";
	public static final String OP_VALIDATE = OP_TYPE_SYS + "validate";
	// limit external ops 
	public static final String OP_LIMIT = OP_TYPE_SYS + "limit";
	// ddl?
	public static final String OP_TABLE = OP_TYPE_SYS + "table";
	// meta  & mapping
	public static final String OP_OPERATION = OP_TYPE_SYS + "operation";
	
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
	public static final String F_TYPE = "type";
	public static final String F_SUPER_ROLES = "super_roles";
	public static final String F_ROLES = "roles";
	public static final String F_ERROR_MESSAGE = "error_message"; // sys.validate

	// transient - not stored in blockchain
	public static final String F_PRIVATEKEY = "privatekey"; // private key to return to user
	public static final String F_UID = "uid"; // user identifier
	public static final String F_VALIDATE = "validate"; // sys.validate
	public static final String F_IF = "if"; // sys.validate

	public static final String METHOD_OAUTH = "oauth";
	public static final String METHOD_PWD = "pwd";
	public static final String METHOD_PROVIDED = "provided";

	public static final String JSON_MSG_TYPE = "json";

	// this char is not allowed in the nickname!
	public static final char USER_LOGIN_CHAR = ':';

	private static final String WILDCARD_RULE = "*";
	
	private JsonFormatter formatter;
	private ValidationListener logValidation;
	
	
	public OpBlockchainRules(JsonFormatter formatter, ValidationListener logValidation) {
		this.formatter = formatter;
		this.logValidation = logValidation;
	}
	
	
	public String getSignupDescription() {
		return "This operation signs up new user in DB."
				+ "<br>This operation must be signed by signup key itself and the login key of the server that can signup users."
				+ "<br>Supported fields:" + "<br>'name' : unique nickname"
				+ "<br>'auth_method' : authorization method (oauth, pwd, provided)"
				+ "<br>'pub_key' : public key for assymetric crypthograph"
				+ "<br>'algo' : algorithm for assymetric crypthograph"
				+ "<br>'keygen_method' : keygen is specified when pwd is used"
				+ "<br>'oauthid_hash' : hash for oauth id which is calculated with 'salt'"
				+ "<br>'oauth_provider' : oauth provider such as osm, fb, google"
				+ "<br>'details' : json with details for spoken languages, avatar, country"
				+ "<br>list of other fields";
	}

	public String getLoginDescription() {
		return "This operation logins an existing user to a specific 'site' (named login). "
				+ "In case user was logged in under such name the previous login key pair will become invalid."
				+ "<br>This operation must be signed by signup key of the user." + "<br>Supported fields:"
				+ "<br>'name' : unique name for a user of the site or purpose of login"
				+ "<br>'pub_key' : public key for assymetric crypthograph"
				+ "<br>'algo' : algorithm for assymetric crypthograph"
				+ "<br>'keygen_method' : later could be used to explain how the key was calculated"
				+ "<br>'details' : json with details" + "<br>list of other fields";
	}

	private static boolean isAllowedNicknameSymbol(char c) {
		return c == ' ' || c == '$' || c == '_' || c == '.' || c == '-';
	}

	public static boolean validateNickname(String name) {
		if (name.trim().length() == 0) {
			return false;
		}
		for (int i = 0; i < name.length(); i++) {
			char c = name.charAt(i);
			if (!Character.isLetter(c) && !Character.isDigit(c) && !isAllowedNicknameSymbol(c)) {
				return false;
			}
		}
		return true;
	}

	public static String getRawHash(String hs) {
		if(hs != null && hs.length() > 0) {
			int i = hs.lastIndexOf(':');
			if(i > 0) {
				return hs.substring(i + 1);
			}
			return hs;
		}
		return "";
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
		for (OpOperation o : op.getOperations()) {
			byte[] hashBytes = SecUtils.getHashBytes(o.getHash());
			hashes.add(hashBytes);
		}
		return calculateMerkleTreeInPlaceHash(SecUtils.HASH_SHA256, hashes);
	}

	public String calculateSigMerkleTreeHash(OpBlock op) {
		List<byte[]> hashes = new ArrayList<byte[]>();
		for (OpOperation o : op.getOperations()) {
			List<String> sigs = o.getSignatureList();
			byte[] bts = null;
			for (String s : sigs) {
				bts = SecUtils.mergeTwoArrays(bts, SecUtils.decodeSignature(s));
			}
			hashes.add(bts);
		}
		return calculateMerkleTreeInPlaceHash(SecUtils.HASH_SHA256, hashes);
	}

	private String calculateMerkleTreeInPlaceHash(String algo, List<byte[]> hashes) {
		if (hashes.size() == 0) {
			return "";
		}
		if (hashes.size() <= 1) {
			return SecUtils.formatHashWithAlgo(algo, hashes.get(0));
		}
		List<byte[]> nextLevel = new ArrayList<byte[]>();
		for (int i = 0; i < hashes.size(); i += 2) {
			byte[] hsh = SecUtils.calculateHash(algo, hashes.get(i),
					i + 1 < hashes.size() ? hashes.get(i + 1) : hashes.get(i));
			nextLevel.add(hsh);
		}
		return calculateMerkleTreeInPlaceHash(algo, nextLevel);
	}

	// hash and signature operations
	public String calculateOperationHash(OpOperation ob, boolean set) {
		String hash = JSON_MSG_TYPE + ":"
				+ SecUtils.calculateHashWithAlgo(SecUtils.HASH_SHA256, null, 
						formatter.opToJsonNoHash(ob));
		if (set) {
			ob.putStringValue(OpOperation.F_HASH, hash);
		}
		return hash;
	}
	

	public JsonFormatter getFormatter() {
		return formatter;
	}

	public OpOperation generateHashAndSign(OpOperation op, KeyPair... keyPair) throws FailedVerificationException {
		String hsh = calculateOperationHash(op, true);
		byte[] hashBytes = SecUtils.getHashBytes(hsh);
		op.remove(OpOperation.F_SIGNATURE);
		for (KeyPair o : keyPair) {
			String sig = SecUtils.signMessageWithKeyBase64(o, hashBytes, SecUtils.SIG_ALGO_ECDSA, null);
			op.addOrSetStringValue(OpOperation.F_SIGNATURE, sig);
		}
		return op;
	}

	
	public boolean validateRules(OpBlockChain blockchain, OpOperation o,
			List<OpObject> deletedObjsCache, Map<String, OpObject> refObjsCache, ValidationTimer timer) {
		if(OpBlockchainRules.OP_VALIDATE.equals(o.getType())) {
			// validate expression
			for(OpObject obj : o.getCreated()) {
				try {
					getValidateExpresions(F_IF, obj);
					getValidateExpresions(F_VALIDATE, obj);
				} catch(RuntimeException e) {
					return error(o, e, ErrorType.OP_INVALID_VALIDATE_EXPRESSION, o.getHash(), e.getMessage());
				}
			}
		}
		// these 2 validations could be registered as blockchain sys.validate
		if(OpBlockchainRules.OP_ROLE.equals(o.getType())) {
			Map<String, Set<String>> builtInRoles = getRoles(blockchain);
			for(OpObject obj : o.getCreated()) {
				String roleId = obj.getId().get(0);
				Set<String> existingDescendants = builtInRoles.get(roleId);
				for (String superRole : obj.getStringList(F_SUPER_ROLES)) {
					if (existingDescendants != null && existingDescendants.contains(superRole)) {
						return error(o, ErrorType.OP_ROLE_SUPER_ROLE_CIRCULAR_REF, o.getHash(), superRole, roleId);
					}
					if (!builtInRoles.containsKey(superRole)) {
						return error(o, ErrorType.OP_ROLE_SUPER_ROLE_DOESNT_EXIST, o.getHash(), superRole, roleId);
					}
				}
			}
		}
		// this validation could be registered as blockchain sys.validate
		if(OpBlockchainRules.OP_GRANT.equals(o.getType())) {
			Map<String, Set<String>> builtInRoles = getRoles(blockchain);
			for(OpObject obj : o.getCreated()) {
				for (String role : obj.getStringList(F_ROLES)) {
					if (!builtInRoles.containsKey(role)) {
						return error(o, ErrorType.OP_GRANT_ROLE_DOESNT_EXIST, o.getHash(), role, obj.getId().toString());
					}
				}
			}
		}
		Map<String, List<OpObject>> validationRules = getValidationRules(blockchain);
		List<OpObject> toValidate = validationRules.get(o.getType());
		if(toValidate != null) {
			for(OpObject rule : toValidate) {
				if(!validateRule(blockchain, rule, o, deletedObjsCache, refObjsCache, timer)) {
					return false;
				}
			}
		}
		toValidate = validationRules.get(WILDCARD_RULE);
		if(toValidate != null) {
			for(OpObject rule : toValidate) {
				if(!validateRule(blockchain, rule, o, deletedObjsCache, refObjsCache, timer)) {
					return false;
				}
			}
		}
		return true;
	}

	private boolean validateRule(OpBlockChain blockchain, OpObject rule, OpOperation o, List<OpObject> deletedObjsCache,
			Map<String, OpObject> refObjsCache, ValidationTimer timer) {
		JsonArray deletedArray = (JsonArray) formatter.toJsonElement(deletedObjsCache);
		for(int i = 0; i < deletedArray.size(); i++) {
			((JsonObject)deletedArray.get(i)).addProperty(OpOperation.F_TYPE, deletedObjsCache.get(i).getParentType());
		}
		JsonObject refsMap = formatter.toJsonElement(refObjsCache).getAsJsonObject();
		for(String key : refObjsCache.keySet()) {
			((JsonObject) refsMap.get(key)).addProperty(OpOperation.F_TYPE, refObjsCache.get(key).getParentType());
		}
		JsonObject opJsonObj = formatter.toJsonElement(o).getAsJsonObject();
		EvaluationContext ctx = new EvaluationContext(blockchain, opJsonObj, deletedArray, refsMap);
		List<OpExprEvaluator> vld = getValidateExpresions(F_VALIDATE, rule);
		List<OpExprEvaluator> ifs = getValidateExpresions(F_IF, rule);
		for(OpExprEvaluator s : ifs) {
			if(!s.evaluateBoolean(ctx)) {
				return true;
			}
		}
		for (OpExprEvaluator s : vld) {
			if (!s.evaluateBoolean(ctx)) {
				return error(o, ErrorType.OP_VALIDATION_FAILED, o.getHash(), rule.getId(),
						rule.getStringValue(F_ERROR_MESSAGE));
			}
		}
		return true;
	}

	@SuppressWarnings("unchecked")
	private List<OpExprEvaluator> getValidateExpresions(String field, OpObject rule) {
		List<OpExprEvaluator> validate = (List<OpExprEvaluator>) rule.getCacheObject(field);
		if(validate == null) {
			validate = new ArrayList<OpExprEvaluator>();
			for (String expr : rule.getStringList(field)) {
				validate.add(OpExprEvaluator.parseExpression(expr));
			}
			rule.putCacheObject(field, validate);
		}
		return validate;
	}
	
	
	@SuppressWarnings("unchecked")
	public Map<String, Set<String>> getRoles(OpBlockChain blockchain) {
		OpBlockChain.ObjectsSearchRequest req = new OpBlockChain.ObjectsSearchRequest();
		req.requestCache = true;
		blockchain.getObjects(OP_ROLE, req);
		Map<String, Set<String>> rolesMap = (Map<String,  Set<String>>) req.cacheObject;
		if(rolesMap == null) {
			rolesMap = new TreeMap<String, Set<String>>();
			for(OpObject vld : req.result) {
				String roleId = vld.getId().get(0);
				rolesMap.put(roleId, new TreeSet<String>());
			}
			for(OpObject vld : req.result) {
				String roleId = vld.getId().get(0);
				rolesMap.get(roleId).add(roleId);
				for(String superRole : vld.getStringList(F_SUPER_ROLES)) {
					Set<String> sr = rolesMap.get(superRole);
					if(sr != null) {
						sr.add(roleId);
					}
				}
			}
			recalculateFullRolesMap(rolesMap);
			blockchain.setCacheAfterSearch(req, rolesMap);
		}
		return rolesMap;
	}


	private void recalculateFullRolesMap(Map<String, Set<String>> rolesMap) {
		boolean changed = true;
		// number of iteration depends on the roles depth and in practice it shouldn't be more than 5-6 iterations
		while(changed) {
			changed = false;
			for(String superRole : rolesMap.keySet()) {
				Set<String> underlyingSuperRoles = rolesMap.get(superRole);
				
				Iterator<String> it = underlyingSuperRoles.iterator();
				while(it.hasNext()) {
					String role = it.next();
					if(!role.equals(superRole)) {
						// combine all underlying roles
						Set<String> underlyingRoles = rolesMap.get(role);
						boolean added = underlyingSuperRoles.addAll(underlyingRoles);
						if(added) {
							changed = true;
							it = underlyingSuperRoles.iterator();
						}
					}
				}
			}
			
		}
	}

	@SuppressWarnings("unchecked")
	private Map<String, List<OpObject>> getValidationRules(OpBlockChain blockchain) {
		OpBlockChain.ObjectsSearchRequest req = new OpBlockChain.ObjectsSearchRequest();
		req.requestCache = true;
		blockchain.getObjects(OP_VALIDATE, req);
		Map<String, List<OpObject>> validationRules = (Map<String,  List<OpObject>>) req.cacheObject;
		if(validationRules == null) {
			validationRules = new TreeMap<String, List<OpObject>>();
			for(OpObject vld : req.result) {
				for(String type : vld.getStringList(F_TYPE)) {
					if(!validationRules.containsKey(type)) {
						validationRules.put(type, new ArrayList<OpObject>());
					}
					validationRules.get(type).add(vld);
				}
			}
			blockchain.setCacheAfterSearch(req, validationRules);
		}
		return validationRules;
	}
	
	public boolean validateBlock(OpBlockChain blockChain, OpBlock block, OpBlock prevBlockHeader, boolean validateSignature) {
		String blockHash = block.getFullHash();
		int blockId = block.getBlockId();
		int pid = -1;
		if (prevBlockHeader != null) {
			if (!OUtils.equals(prevBlockHeader.getFullHash(), block.getStringValue(OpBlock.F_PREV_BLOCK_HASH))) {
				return error(block, ErrorType.BLOCK_PREV_HASH, prevBlockHeader.getFullHash(),
						block.getStringValue(OpBlock.F_PREV_BLOCK_HASH), blockHash);
			}
			pid = prevBlockHeader.getBlockId();
		}
		if (pid + 1 != blockId) {
			return error(block, ErrorType.BLOCK_PREV_ID, pid, block.getBlockId(), blockHash);
		}
		int dupBl = blockChain.getBlockDepth(block);
		if (dupBl != -1) {
			return error(block, ErrorType.BLOCK_HASH_IS_DUPLICATED, blockHash, block.getBlockId(), dupBl);
		}
		if (block.getOperations().size() == 0) {
			return error(block, ErrorType.BLOCK_EMPTY, blockHash);
		}
		if (!OUtils.equals(calculateMerkleTreeHash(block), block.getStringValue(OpBlock.F_MERKLE_TREE_HASH))) {
			return error(block, ErrorType.BLOCK_MERKLE_TREE_FAILED, blockHash, calculateMerkleTreeHash(block),
					block.getStringValue(OpBlock.F_MERKLE_TREE_HASH));
		}
		if (!OUtils.equals(calculateSigMerkleTreeHash(block), block.getStringValue(OpBlock.F_SIG_MERKLE_TREE_HASH))) {
			return error(block, ErrorType.BLOCK_SIG_MERKLE_TREE_FAILED, blockHash, calculateSigMerkleTreeHash(block),
					block.getStringValue(OpBlock.F_SIG_MERKLE_TREE_HASH));
		}
		if (!OUtils.equals(calculateHash(block), block.getFullHash())) {
			return error(block, ErrorType.BLOCK_HASH_FAILED, block.getFullHash(), calculateHash(block));
		}
		
		if(!validateSignature) {
			return true;
		}
		OpObject keyObj = getLoginKeyObj(blockChain, block.getStringValue(OpBlock.F_SIGNED_BY));
		boolean validateSig = true;
		Exception ex = null;
		try {
			KeyPair pk = getKeyPairFromObj(keyObj, null);
			byte[] blHash = SecUtils.getHashBytes(block.getFullHash());
			if (pk != null && SecUtils.validateSignature(pk, blHash, block.getSignature())) {
				validateSig = true;
			} else {
				validateSig = false;
			}
		} catch (FailedVerificationException e) {
			validateSig = false;
			ex = e;
		} catch (RuntimeException e) {
			validateSig = false;
			ex = e;
		}
		if (!validateSig) {
			return error(block, ErrorType.BLOCK_SIGNATURE_FAILED, blockHash, block.getStringValue(OpBlock.F_SIGNED_BY), ex);
		}
		return true;
	}
	
	public boolean validateSignatures(OpBlockChain ctx, OpOperation ob) {
		List<String> sigs = ob.getSignatureList();
		List<String> signedBy = ob.getSignedBy();
		if (signedBy.size() != sigs.size()) {
			return error(ob, ErrorType.OP_SIGNATURE_FAILED, ob.getHash(), sigs);
		}
		byte[] txHash = SecUtils.getHashBytes(ob.getHash());
		boolean signByItself = false;
		String signupName = "";
		if(OpBlockchainRules.OP_SIGNUP.equals(ob.getType()) && ob.getCreated().size() == 1) {
			signupName =  ob.getCreated().get(0).getId().get(0);
			OpObject obj = ctx.getObjectByName(OpBlockchainRules.OP_SIGNUP, signupName);
			signByItself = obj == null || obj.getStringValue(F_AUTH_METHOD).equals(METHOD_OAUTH);
		}
		// 1st signup could be signed by itself
		for (int i = 0; i < sigs.size(); i++) {
			Exception cause = null;
			boolean validate = false;
			try {
				String sig = sigs.get(i);
				String signedByName = signedBy.get(i);
				OpObject keyObj;
				if(signByItself && signedByName.equals(signupName)) {
					keyObj = ob.getCreated().get(0);
				} else {
					keyObj = getLoginKeyObj(ctx, signedByName);
				}
				KeyPair kp = getKeyPairFromObj(keyObj, null);
				validate = SecUtils.validateSignature(kp, txHash, sig);
			} catch (Exception e) {
				cause = e;
			}
			if (!validate) {
				return error(ob, cause, ErrorType.OP_SIGNATURE_FAILED, ob.getHash(), sigs.get(i));
			}
		}
		return true;
	}
	
	
	public boolean validateOp(OpBlockChain opBlockChain, OpOperation u, List<OpObject> deletedObjsCache,
			Map<String, OpObject> refObjsCache, ValidationTimer vld) {
		if(!OUtils.equals(calculateOperationHash(u, false), u.getHash())) {
			return error(u, ErrorType.OP_HASH_IS_NOT_CORRECT, calculateOperationHash(u, false), u.getHash());
		}
		
		int sz = formatter.opToJson(u).length();
		if (sz > OpBlockchainRules.MAX_OP_SIZE_MB) {
			return error(u, ErrorType.OP_SIZE_IS_EXCEEDED, u.getHash(), sz, OpBlockchainRules.MAX_OP_SIZE_MB);
		}
		int timerSigs = vld.startExtra();
		boolean valid = validateSignatures(opBlockChain, u);
		vld.measure(timerSigs, ValidationTimer.OP_SIG);
		if(!valid) {
			return valid;
		}
		int timerRoles = vld.startExtra();
		valid = validateRules(opBlockChain, u, deletedObjsCache, refObjsCache, vld);
		vld.measure(timerRoles, ValidationTimer.OP_SIG);
		
		if(!valid) {
			return valid;
		}
		return true;
	}
	
	public KeyPair getLoginKeyPair(OpBlockChain ctx, String signedByName, String privateKey) throws FailedVerificationException {
		OpObject obj = getLoginKeyObj(ctx, signedByName);
		return getKeyPairFromObj(obj, privateKey);
	}

	public OpObject getLoginKeyObj(OpBlockChain ctx, String signedByName) {
		OpObject keyObj;
		int n = signedByName.indexOf(USER_LOGIN_CHAR);
		if(n == -1) {
			keyObj = ctx.getObjectByName(OpBlockchainRules.OP_SIGNUP, signedByName);
		} else  {
			keyObj = ctx.getObjectByName(OpBlockchainRules.OP_LOGIN, signedByName.substring(0, n),
					signedByName.substring(n + 1));
		}
		return keyObj;
	}

	

	public KeyPair getSignUpKeyPairFromPwd(OpBlockChain blc,
			String name, String pwd) throws FailedVerificationException {
		OpObject op = blc.getObjectByName(OpBlockchainRules.OP_SIGNUP, name); 
		if (op == null) {
			return null;
		}
		String algo = op.getStringValue(F_ALGO);
		KeyPair keyPair = SecUtils.generateKeyPairFromPassword(algo, op.getStringValue(F_KEYGEN_METHOD),
				op.getStringValue(F_SALT), pwd);
		KeyPair kp = SecUtils.getKeyPair(algo, null, op.getStringValue(F_PUBKEY));
		if (SecUtils.validateKeyPair(algo, keyPair.getPrivate(), kp.getPublic())) {
			return keyPair;
		}
		return null;
	}	
	private KeyPair getKeyPairFromObj(OpObject op, String privatekey) throws FailedVerificationException {
		if(op == null) {
			return null;
		}
		String algo = op.getStringValue(F_ALGO);
		KeyPair kp = SecUtils.getKeyPair(algo, privatekey, op.getStringValue(F_PUBKEY));
		if (privatekey == null || SecUtils.validateKeyPair(algo, kp.getPrivate(), kp.getPublic())) {
			return kp;
		}
		return null;
	}

	
	public OpBlock createAndSignBlock(Collection<OpOperation> ops, OpBlock prevOpBlockHeader, String serverUser, KeyPair serverKeyPair)
			throws FailedVerificationException {
		OpBlock block = new OpBlock();
		block.operations.addAll(ops);
		block.setDate(OpBlock.F_DATE, System.currentTimeMillis());
		block.putObjectValue(OpBlock.F_BLOCKID, prevOpBlockHeader == null ? 0 : (prevOpBlockHeader.getBlockId() + 1));
		block.putStringValue(OpBlock.F_PREV_BLOCK_HASH, prevOpBlockHeader == null ? "" : prevOpBlockHeader.getFullHash());
		block.putStringValue(OpBlock.F_MERKLE_TREE_HASH, calculateMerkleTreeHash(block));
		block.putStringValue(OpBlock.F_SIG_MERKLE_TREE_HASH, calculateSigMerkleTreeHash(block));
		if (serverUser != null) {
			block.putStringValue(OpBlock.F_SIGNED_BY, serverUser);
		}
		block.putObjectValue(OpBlock.F_VERSION, BLOCK_VERSION);
		block.putObjectValue(OpBlock.F_EXTRA, BLOCK_EXTRA);
		block.putStringValue(OpBlock.F_DETAILS, BLOCK_CREATION_DETAILS);
		block.putStringValue(OpBlock.F_HASH, calculateHash(block));
		if (serverKeyPair != null) {
			byte[] hashBytes = SecUtils.getHashBytes(block.getFullHash());
			block.putStringValue(OpBlock.F_SIGNATURE,
					SecUtils.signMessageWithKeyBase64(serverKeyPair, hashBytes, SecUtils.SIG_ALGO_ECDSA, null));
		}
		block.makeImmutable();
		return block;
	}
	
	public static String calculateSuperblockHash(int size, String lastBlockHash) {
		return String.format("%08x", size) + lastBlockHash;
	}
	
	public String calculateHash(OpBlock block) {
		ByteArrayOutputStream bs = new ByteArrayOutputStream();
		DataOutputStream dous = new DataOutputStream(bs);
		try {
			dous.writeInt(block.getIntValue(OpBlock.F_VERSION, BLOCK_VERSION));
			dous.writeInt(block.getBlockId());
			dous.write(SecUtils.getHashBytes(block.getStringValue(OpBlock.F_PREV_BLOCK_HASH)));
			dous.writeLong(block.getDate(OpBlock.F_DATE));
			dous.write(SecUtils.getHashBytes(block.getStringValue(OpBlock.F_MERKLE_TREE_HASH)));
			dous.write(SecUtils.getHashBytes(block.getStringValue(OpBlock.F_SIG_MERKLE_TREE_HASH)));
			dous.writeLong(block.getLongValue(OpBlock.F_EXTRA, BLOCK_EXTRA));
			if(!OUtils.isEmpty(block.getStringValue(OpBlock.F_DETAILS))) {
				dous.write(block.getStringValue(OpBlock.F_DETAILS).getBytes("UTF-8"));
			}
			dous.write(block.getStringValue(OpBlock.F_DETAILS).getBytes("UTF-8"));
			dous.flush();
			return SecUtils.calculateHashWithAlgo(SecUtils.HASH_SHA256, bs.toByteArray());
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
	}

	
	public boolean error(OpObject o, ErrorType e, Object... args) {
		String eMsg = e.getErrorFormat(args);
		if(logValidation != null) {
			logValidation.logError(o, e, eMsg, null);
		}
		throw new IllegalArgumentException(e.getErrorFormat(args));
	}
	
	public boolean error(OpObject o, Exception cause, ErrorType e, Object... args) {
		String eMsg = e.getErrorFormat(args);
		if(logValidation != null) {
			logValidation.logError(o, e, eMsg, null);
		}
		throw new IllegalArgumentException(e.getErrorFormat(args), cause);
	}
	
	public static enum ErrorType {
		BLOCK_PREV_HASH("Previous block hash is not equal '%s' != '%s': block '%s'"),
		BLOCK_PREV_ID("Previous block id is not equal '%d' != '%d': block '%s'"),
		BLOCK_EMPTY("Block '%s' doesn't have any operations"),
		BLOCK_MERKLE_TREE_FAILED("Block '%s': failed to validate merkle tree '%s' != '%s'"), 
		BLOCK_SIG_MERKLE_TREE_FAILED("Block '%s': failed to validate signature merkle tree '%s' != '%s'"),
		BLOCK_HASH_FAILED("Block '%s': failed to validate hash '%s'"), 
		BLOCK_SIGNATURE_FAILED("Block '%s': signature of '%s' failed to validate"),
		BLOCK_HASH_IS_DUPLICATED("Block hash is duplicated '%s' in block '%d' and '%d'"),
		
		OP_SIZE_IS_EXCEEDED("Operation '%s' size '%d' exceeds the limit '%d'"),
		OP_HASH_IS_DUPLICATED("Operation '%s' hash is duplicated in block '%s'"),
		OP_HASH_IS_NOT_CORRECT("Operation hash is not correct '%s' != '%s'"),
		OP_SIGNATURE_FAILED("Operation '%s': signature by '%s' could not be validated"),
		
		NEW_OBJ_DOUBLE_CREATED("Operation '%s': object '%s' was already created"),
		DEL_OBJ_NOT_FOUND("Operation '%s': object to delete '%s' wasn't found"),
		DEL_OBJ_DOUBLE_DELETED("Operation '%s': object '%s' was already deleted at block '%s'"),
		REF_OBJ_NOT_FOUND("Operation '%s': object to reference wasn't found '%s'"),
		
		OP_VALIDATION_FAILED("Operation '%s': failed validation rule '%s'. %s"),
		OP_INVALID_VALIDATE_EXPRESSION("Operation '%s': validate expression couldn't be parsed. %s"),
		
		OP_ROLE_SUPER_ROLE_DOESNT_EXIST("Operation '%s': super role '%s' defined for '%s' is not defined"),
		OP_ROLE_SUPER_ROLE_CIRCULAR_REF("Operation '%s': super role '%s' defined for '%s' has circular references"),
		OP_GRANT_ROLE_DOESNT_EXIST("Operation '%s': role '%s' which is granted to '%s' doesn't exist"),
		
		MGMT_CANT_DELETE_NON_LAST_OPERATIONS("Operation '%s' couldn't be validated cause the parent operation '%s' is going to be deleted"),
		MGMT_REPLICATION_IO_FAILED("Replication sync has failed"),
		MGMT_REPLICATION_BLOCK_DOWNLOAD_FAILED("Replication: replication of '%s' block has failed"),
		MGMT_REPLICATION_BLOCK_CONFLICTS("Replication: replication has conflicting blocks '%s'-there vs '%s'-here: '%s'")
		;
		private final String msg;

		ErrorType(String msg) {
			this.msg = msg;
		}
		
		public String getErrorFormat(Object... args) {
			return String.format(msg, args);
		}
	}
	
	public static interface ValidationListener {
		
		void logError(OpObject o, ErrorType e, String msg, Exception cause);
		
	}

	
}
