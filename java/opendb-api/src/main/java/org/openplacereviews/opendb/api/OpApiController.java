package org.openplacereviews.opendb.api;

import java.security.KeyPair;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.openplacereviews.opendb.FailedVerificationException;
import org.openplacereviews.opendb.OUtils;
import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.ops.OpBlockchainRules;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;


@Controller
@RequestMapping("/api/auth")
public class OpApiController {
    
	public static final String ADMIN_LOGIN_NAME = "admin_name";
	public static final String ADMIN_LOGIN_PWD = "admin_pwd";
	
	Map<String, KeyPair> keyPairs = new TreeMap<>(); 
	
    @Autowired
    private BlocksManager manager;
    
    @Autowired
    private JsonFormatter formatter;

    @GetMapping(path = "/admin-status")
	public ResponseEntity<String> serverLogin(HttpSession session) {
		String serverUser = getServerUser(session);
		return ResponseEntity.status(HttpStatus.OK).body(
				"{\"admin\":\"" + (serverUser == null ? "" : serverUser) + "\"}");
	}
    
    @PostMapping(path = "/admin-login")
    @ResponseBody
    public ResponseEntity<String> serverLogin(@RequestParam(required = true) String name, 
    		@RequestParam(required = true) String pwd, HttpSession session, HttpServletResponse response) {
    	if(OUtils.equals(manager.getServerUser(), name) && 
    			OUtils.equals(manager.getServerPrivateKey(), pwd)) {
    		session.setAttribute(ADMIN_LOGIN_NAME, name);
    		session.setAttribute(ADMIN_LOGIN_PWD, pwd);
    		session.setMaxInactiveInterval(-1);
    		keyPairs.put(name, manager.getServerLoginKeyPair());
    	    return ResponseEntity.ok().body("{\"status\":\"OK\"}");
    	}
    	return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body("{\"status\":\"ERROR\"}");
    }
    
    public boolean validateServerLogin(HttpSession session) {
    	Object loginName = session.getAttribute(ADMIN_LOGIN_NAME);
    	return loginName != null && keyPairs.containsKey(loginName);
	}
    
    private KeyPair getServerLoginKeyPair(HttpSession session) {
    	return keyPairs.get(session.getAttribute(ADMIN_LOGIN_NAME));
	}
    
    private String getServerUser(HttpSession session) {
		return (String) session.getAttribute(ADMIN_LOGIN_NAME);
	}
    
    private ResponseEntity<String> unauthorizedByServer() {
    	return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
    			.body("{\"status\":\"ERROR\"}");
	}
    
    @PostMapping(path = "/sign-operation")
    @ResponseBody
    public ResponseEntity<String> signMessage(HttpSession session, 
    		@RequestParam(required = true) String json, @RequestParam(required = false) String name, 
    		@RequestParam(required = false) String pwd, @RequestParam(required = false) String privateKey, 
    		@RequestParam(required = false, defaultValue = "false") boolean dontSignByServer,
    		@RequestParam(required = false, defaultValue = "false") boolean addToQueue,
    		@RequestParam(required = false, defaultValue = "false") boolean validate)
			throws FailedVerificationException {
    	if(!validateServerLogin(session)) {
    		return unauthorizedByServer();
    	}
		KeyPair kp = null;
		KeyPair altKp = null;
		OpOperation op = formatter.parseOperation(json);
		if (!OUtils.isEmpty(name)) {
			if (!OUtils.isEmpty(pwd)) {
				kp = manager.getLoginKeyPairFromPwd(name, pwd);
			} else if (!OUtils.isEmpty(privateKey)) {
				if(validate) {
					kp = manager.getLoginKeyPair(name, privateKey);
				} else {
					kp = SecUtils.getKeyPair(SecUtils.ALGO_EC, privateKey, null);
				}
				
			}
			if (kp == null) {
				throw new IllegalArgumentException("Couldn't validate sign up key");
			}
			op.setSignedBy(name);
		}
		if (!OUtils.isEmpty(getServerUser(session)) && !dontSignByServer) {
			if (!OUtils.isEmpty(name)) {
				op.addOtherSignedBy(getServerUser(session));
				altKp = getServerLoginKeyPair(session);
			} else {
				op.setSignedBy(getServerUser(session));
				kp = getServerLoginKeyPair(session);
			}
		}
		if (altKp != null) {
			manager.generateHashAndSign(op, kp, altKp);
		} else if(kp != null) {
			manager.generateHashAndSign(op, kp);
		}
		if(addToQueue) {
			manager.addOperation(op);
		}
		return ResponseEntity.ok(formatter.fullObjectToJson(op));
	}

	@PostMapping(path = "/signup")
    @ResponseBody
    public ResponseEntity<String> signup(HttpSession session, @RequestParam(required = true) String name,  
    		@RequestParam(required = false) String pwd,  
    		@RequestParam(required = false) String algo, @RequestParam(required = false) String privateKey, @RequestParam(required = false) String publicKey,
    		@RequestParam(required = false) String oauthProvider, @RequestParam(required = false) String oauthId, 
    		@RequestParam(required = false) String userDetails) throws FailedVerificationException {
		if(!validateServerLogin(session)) {
    		return unauthorizedByServer();
    	}
    	OpOperation op = new OpOperation();
    	name = name.trim(); // reduce errors by having trailing spaces
    	if(!OpBlockchainRules.validateNickname(name)) {
    		throw new IllegalArgumentException(String.format("The nickname '%s' couldn't be validated", name));
    	}
    	
    	op.setType(OpBlockchainRules.OP_SIGNUP);
    	OpObject obj = new OpObject();
    	op.addNew(obj);
    	obj.setId(name);
    	if(!OUtils.isEmpty(userDetails)) {
    		obj.putObjectValue(OpBlockchainRules.F_DETAILS, formatter.fromJsonToTreeMap(userDetails));
    	}
    	
		if (OUtils.isEmpty(algo)) {
    		algo = SecUtils.ALGO_EC;
    	}
    	KeyPair keyPair;
    	KeyPair otherKeyPair = null;
    	if(!OUtils.isEmpty(pwd)) {
    		obj.putStringValue(OpBlockchainRules.F_AUTH_METHOD, OpBlockchainRules.METHOD_PWD);
    		algo = SecUtils.ALGO_EC;
    		String salt = name;
    		String keyGen = SecUtils.KEYGEN_PWD_METHOD_1;
    		keyPair = SecUtils.generateKeyPairFromPassword(algo, keyGen, salt, pwd);
    		obj.putStringValue(OpBlockchainRules.F_SALT, salt);
    		obj.putStringValue(OpBlockchainRules.F_KEYGEN_METHOD, keyGen);
        	op.setSignedBy(name);
    		if(!OUtils.isEmpty(getServerUser(session))) {
    			op.addOtherSignedBy(getServerUser(session));
    			otherKeyPair = getServerLoginKeyPair(session);
    			if(otherKeyPair == null) {
    				throw new IllegalArgumentException(
    						String.format("Server %s to signup user doesn't have valid login key", getServerUser(session)));
    			}
    		}
    	} else if(!OUtils.isEmpty(oauthId)) {
    		obj.putStringValue(OpBlockchainRules.F_AUTH_METHOD, OpBlockchainRules.METHOD_OAUTH);
    		obj.putStringValue(OpBlockchainRules.F_SALT, name);
    		obj.putStringValue(OpBlockchainRules.F_OAUTHID_HASH, SecUtils.calculateHashWithAlgo(SecUtils.HASH_SHA256, name, oauthId));
    		obj.putStringValue(OpBlockchainRules.F_OAUTH_PROVIDER, oauthProvider);
    		keyPair = getServerLoginKeyPair(session);
    		op.setSignedBy(getServerUser(session));
    	} else {
    		obj.putStringValue(OpBlockchainRules.F_AUTH_METHOD, OpBlockchainRules.METHOD_PROVIDED);
    		op.setSignedBy(name);
    		keyPair = SecUtils.getKeyPair(algo, privateKey, publicKey);
    	}
    	if(keyPair == null) {
			throw new IllegalArgumentException(
					String.format("Signup private / public key could not be generated"));
		}
    	obj.putStringValue(OpBlockchainRules.F_ALGO, algo);
    	obj.putStringValue(OpBlockchainRules.F_PUBKEY, SecUtils.encodeKey(SecUtils.KEY_BASE64, keyPair.getPublic()));
    	
    	if(otherKeyPair == null) {
    		manager.generateHashAndSign(op, keyPair);
    	} else {
    		manager.generateHashAndSign(op, keyPair, otherKeyPair);
    	}
    	manager.addOperation(op);
    	return ResponseEntity.ok(formatter.fullObjectToJson(op));
    }
    
    @PostMapping(path = "/login")
    @ResponseBody
    public ResponseEntity<String> login(HttpSession session,
    		@RequestParam(required = true) String name,  @RequestParam(required = false) String pwd, 
    		@RequestParam(required = false) String signupPrivateKey,
    		@RequestParam(required = false) String oauthProvider, @RequestParam(required = false) String oauthId, 
    		@RequestParam(required = false) String loginAlgo, @RequestParam(required = false) String loginPubKey) throws FailedVerificationException {
    	if(!validateServerLogin(session)) {
    		return unauthorizedByServer();
    	}
    	OpOperation op = new OpOperation();
    	op.setType(OpBlockchainRules.OP_LOGIN);
    	OpObject obj = new OpObject();
    	op.addNew(obj);
    	
		KeyPair kp = null;
		KeyPair otherKeyPair = null;
		String nickname = OpBlockchainRules.getNicknameFromUser(name);
		String purpose = OpBlockchainRules.getSiteFromUser(name);
		obj.setId(nickname, purpose);
		if(!OpBlockchainRules.validateNickname(purpose)) {
    		throw new IllegalArgumentException(String.format("The purpose '%s' couldn't be validated", purpose));
    	}
		String serverName = getServerUser(session);
		OpObject sop = manager.getLoginObj(nickname);
		if (!OUtils.isEmpty(pwd) || !OUtils.isEmpty(signupPrivateKey)) {
			if(!OUtils.isEmpty(signupPrivateKey)) {
				kp = manager.getLoginKeyPair(nickname, signupPrivateKey);	
			} else {
				kp = manager.getLoginKeyPairFromPwd(nickname, pwd);
			}
			op.setSignedBy(nickname);
			// sign with server is it necessary or make it optional? 
			if(!OUtils.isEmpty(serverName)) {
    			op.addOtherSignedBy(serverName);
    			otherKeyPair = getServerLoginKeyPair(session);
    			if(otherKeyPair == null) {
    				throw new IllegalArgumentException(
    						String.format("Server %s to signup user doesn't have valid login key", serverName));
    			}
    		}
		} else if (!OUtils.isEmpty(oauthId)) {
			kp = getServerLoginKeyPair(session);
			if(!SecUtils.validateHash(sop.getStringValue(OpBlockchainRules.F_OAUTHID_HASH), 
					sop.getStringValue(OpBlockchainRules.F_SALT), oauthId) || 
					!oauthProvider.equals(sop.getStringValue(OpBlockchainRules.F_OAUTH_PROVIDER))) {
				throw new IllegalArgumentException("User was registered with different oauth id");
			}
			op.setSignedBy(serverName);
		}
		if (kp == null) {
			throw new IllegalArgumentException("Couldn't validate/find sign up key or server key for oauth");
		}
    	
    	KeyPair loginPair;
		if (!OUtils.isEmpty(loginPubKey)) {
			obj.putStringValue(OpBlockchainRules.F_ALGO, loginAlgo);
    		loginPair = SecUtils.getKeyPair(loginAlgo, null, loginPubKey);
    	} else {
    		obj.putStringValue(OpBlockchainRules.F_ALGO, SecUtils.ALGO_EC);
    		loginPair = SecUtils.generateRandomEC256K1KeyPair();
    	}
		obj.putStringValue(OpBlockchainRules.F_PUBKEY, SecUtils.encodeKey(SecUtils.KEY_BASE64, loginPair.getPublic()));
		Map<String, Object> refs = new TreeMap<String, Object>();
		refs.put("s", Arrays.asList(OpBlockchainRules.OP_SIGNUP, nickname));
    	op.putObjectValue(OpOperation.F_REF, refs);

    	if(otherKeyPair == null) {
    		manager.generateHashAndSign(op, kp);
    	} else {
    		manager.generateHashAndSign(op, kp, otherKeyPair);
    	}
    	
    	manager.addOperation(op);
    	// private key won't be stored on opendb
    	if(loginPair.getPrivate() != null) {
    		op.putCacheObject(OpBlockchainRules.F_PRIVATEKEY, SecUtils.encodeKey(SecUtils.KEY_BASE64, loginPair.getPrivate()));
    	}
    	return ResponseEntity.ok(formatter.fullObjectToJson(op));
    }
    
    
}
