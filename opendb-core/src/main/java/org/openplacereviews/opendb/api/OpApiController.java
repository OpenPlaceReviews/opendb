package org.openplacereviews.opendb.api;

import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.ops.OpBlockchainRules;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.OUtils;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.security.KeyPair;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import static org.openplacereviews.opendb.ops.OpBlockchainRules.F_ROLES;
import static org.openplacereviews.opendb.ops.OpBlockchainRules.OP_GRANT;


@Controller
@RequestMapping("/api/auth")
public class OpApiController {
    
	public static final String ADMIN_LOGIN_NAME = "admin_name";
	public static final String ADMIN_LOGIN_PWD = "admin_pwd";
	public static final String ROLE_ADMINISTRATOR = "administrator";

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
    	OpObject user = manager.getBlockchain().getObjectByName(OP_GRANT, name);
    	boolean isAdministrator = false;
    	if (user != null) {
    		if (user.getStringList(F_ROLES).contains(ROLE_ADMINISTRATOR)) {
    			isAdministrator = true;
			}
		}
    	if(OUtils.equals(manager.getServerUser(), name) && 
    			OUtils.equals(manager.getServerPrivateKey(), pwd) || isAdministrator) {
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

	@PostMapping(path = "/process-operation")
	@ResponseBody
	public ResponseEntity<String> processOperation(HttpSession session,
												   @RequestBody(required = true) String json,
												   @RequestParam(required = false) String name,
												   @RequestParam(required = false) String pwd,
												   @RequestParam(required = false) String privateKey,
												   @RequestParam(required = false, defaultValue = "false")
														   boolean dontSignByServer,
												   @RequestParam(required = false, defaultValue = "false")
														   boolean addToQueue,
												   @RequestParam(required = false, defaultValue = "false")
														   boolean validate)
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
		if(validate) {
			manager.validateOperation(op);
		}
		if(addToQueue) {
			manager.addOperation(op);
		}
		return ResponseEntity.ok(formatter.fullObjectToJson(op));
	}

	@PostMapping(path = "/signup")
    @ResponseBody
    public ResponseEntity<String> signup(HttpSession session, @RequestParam(required = true) String name,  
    		@RequestParam(required = false, defaultValue = "false") boolean onlyValidate,
    		@RequestParam(required = false) String pwd,  
    		@RequestParam(required = false) String algo, @RequestParam(required = false) String privateKey, @RequestParam(required = false) String publicKey,
    		@RequestParam(required = false) String oauthProvider, @RequestParam(required = false) String oauthId,
    		@RequestParam(required = false) String pwdOld,  
    		@RequestParam(required = false) String algoOld, @RequestParam(required = false) String privateKeyOld, @RequestParam(required = false) String publicKeyOld,
    		@RequestParam(required = false) String oauthProviderOld, @RequestParam(required = false) String oauthIdOld, 
    		@RequestParam(required = false) String userDetails) throws FailedVerificationException {
		if(!validateServerLogin(session)) {
    		return unauthorizedByServer();
    	}
    	OpOperation op = new OpOperation();
    	name = name.trim(); // reduce errors by having trailing spaces
    	if(!OpBlockchainRules.validateNickname(name)) {
    		throw new IllegalArgumentException(String.format("The nickname '%s' couldn't be validated", name));
    	}
    	if(OUtils.isEmpty(pwd) && OUtils.isEmpty(oauthId) && OUtils.isEmpty(privateKey)) {
    		throw new IllegalArgumentException("Signup method is not specified");
    	}
    	boolean edit = true;
    	if(OUtils.isEmpty(pwdOld) && OUtils.isEmpty(oauthIdOld) && OUtils.isEmpty(privateKeyOld)) {
    		edit = false;
    	}
    	KeyPair signKeyPair = null;
    	String signName = name;
    	if (OUtils.isEmpty(algo)) {
    		algo = SecUtils.ALGO_EC;
    	}
		if (OUtils.isEmpty(algoOld)) {
    		algoOld = SecUtils.ALGO_EC;
    	}
		
    	if(edit) {
    		OpObject loginObj = manager.getLoginObj(name);
    		if(loginObj == null) {
    			throw new IllegalArgumentException("There is nothing to edit cause signup obj doesn't exist");
    		} else {
    			op.addDeleted(loginObj.getId());
    			String authMethod = loginObj.getStringValue(OpBlockchainRules.F_AUTH_METHOD);
    			if(OpBlockchainRules.METHOD_PWD.equals(authMethod)) {
    				signKeyPair = SecUtils.generateKeyPairFromPassword(algoOld, loginObj.getStringValue(OpBlockchainRules.F_KEYGEN_METHOD), 
        					loginObj.getStringValue(OpBlockchainRules.F_SALT), pwdOld);
    			} else if(OpBlockchainRules.METHOD_OAUTH.equals(authMethod)) {
    				String phash = SecUtils.calculateHashWithAlgo(SecUtils.HASH_SHA256, loginObj.getStringValue(OpBlockchainRules.F_SALT), 
    						oauthIdOld);
    				if(!phash.equals(loginObj.getStringValue(OpBlockchainRules.F_OAUTHID_HASH)) || 
    						!oauthProviderOld.equals(loginObj.getStringValue(OpBlockchainRules.F_OAUTH_PROVIDER))) {
    					throw new IllegalArgumentException(String.format("Couldn't validate specified oauth credentions '%s':'%s'", oauthIdOld, oauthProviderOld));
    				}
    			} else {
    				signKeyPair = SecUtils.getKeyPair(algo, privateKey, publicKey);
    			}
    			
    			
    		}
    	}
    	
    	op.setType(OpBlockchainRules.OP_SIGNUP);
    	OpObject obj = new OpObject();
    	op.addCreated(obj);
    	obj.setId(name);
    	if(!OUtils.isEmpty(userDetails)) {
    		obj.putObjectValue(OpBlockchainRules.F_DETAILS, formatter.fromJsonToTreeMap(userDetails));
    	}
    	KeyPair newKeyPair = null;
    	if(!OUtils.isEmpty(pwd)) {
    		obj.putStringValue(OpBlockchainRules.F_AUTH_METHOD, OpBlockchainRules.METHOD_PWD);
    		algo = SecUtils.ALGO_EC;
    		String salt = name;
    		String keyGen = SecUtils.KEYGEN_PWD_METHOD_1;
    		newKeyPair = SecUtils.generateKeyPairFromPassword(algo, keyGen, salt, pwd);
    		obj.putStringValue(OpBlockchainRules.F_SALT, salt);
    		obj.putStringValue(OpBlockchainRules.F_KEYGEN_METHOD, keyGen);
    		if(signKeyPair == null) {
    			signKeyPair = newKeyPair;
    		}
    	} else if(!OUtils.isEmpty(oauthId)) {
    		obj.putStringValue(OpBlockchainRules.F_AUTH_METHOD, OpBlockchainRules.METHOD_OAUTH);
    		obj.putStringValue(OpBlockchainRules.F_SALT, name);
    		obj.putStringValue(OpBlockchainRules.F_OAUTHID_HASH, SecUtils.calculateHashWithAlgo(SecUtils.HASH_SHA256, name, oauthId));
    		obj.putStringValue(OpBlockchainRules.F_OAUTH_PROVIDER, oauthProvider);
    		if(signKeyPair == null) {
    			newKeyPair = null;
        		signName = getServerUser(session);
        		signKeyPair = getServerLoginKeyPair(session);
    		}
    	} else {
    		obj.putStringValue(OpBlockchainRules.F_AUTH_METHOD, OpBlockchainRules.METHOD_PROVIDED);
    		newKeyPair = SecUtils.getKeyPair(algo, privateKey, publicKey);
    		if(signKeyPair == null) {
    			signKeyPair = newKeyPair;
    		}
    	}
    	if(signKeyPair == null) {
			throw new IllegalArgumentException(
					String.format("Signup private / public key could not be generated"));
		}
    	if(newKeyPair != null) {
    		obj.putStringValue(OpBlockchainRules.F_ALGO, algo);
    		obj.putStringValue(OpBlockchainRules.F_PUBKEY, SecUtils.encodeKey(SecUtils.KEY_BASE64, newKeyPair.getPublic()));
    	}
    	
    	op.setSignedBy(signName);
    	String serverUser = getServerUser(session);
		if(!OUtils.isEmpty(serverUser) && !serverUser.equals(signName)) {
			op.addOtherSignedBy(serverUser);
			KeyPair secondSignKeyPair = getServerLoginKeyPair(session);
			if(secondSignKeyPair == null) {
				throw new IllegalArgumentException(
						String.format("Server %s to signup user doesn't have valid login key", getServerUser(session)));
			}
			manager.generateHashAndSign(op, signKeyPair, secondSignKeyPair);
		} else {
			manager.generateHashAndSign(op, signKeyPair);
		}
		
    	if(onlyValidate) {
    		manager.validateOperation(op);
    	} else {
    		manager.addOperation(op);
    	}
    	return ResponseEntity.ok(formatter.fullObjectToJson(op));
    }
    
    @PostMapping(path = "/login")
    @ResponseBody
    public ResponseEntity<String> login(HttpSession session,
    		@RequestParam(required = true) String name, 
    		@RequestParam(required = false, defaultValue = "false") boolean edit, @RequestParam(required = false, defaultValue = "false") boolean delete,
    		@RequestParam(required = false, defaultValue = "false") boolean onlyValidate,
    		@RequestParam(required = false) String pwd, 
    		@RequestParam(required = false) String signupPrivateKey,
    		@RequestParam(required = false) String oauthProvider, @RequestParam(required = false) String oauthId, 
    		@RequestParam(required = false) String loginAlgo, @RequestParam(required = false) String loginPubKey,
    		@RequestParam(required = false) String userDetails) throws FailedVerificationException {
    	if(!validateServerLogin(session)) {
    		return unauthorizedByServer();
    	}
    	OpOperation op = new OpOperation();
    	op.setType(OpBlockchainRules.OP_LOGIN);
    	if(edit) {
    		OpObject loginObj = manager.getLoginObj(name);
    		if(loginObj == null) {
    			if(delete) {
    				throw new IllegalArgumentException("There is nothing to edit cause login obj doesn't exist");
    			}
    		} else {
    			op.addDeleted(loginObj.getId());
    		}
    	}
    	
    	
		KeyPair kp = null;
		KeyPair otherKeyPair = null;
		String nickname = OpBlockchainRules.getNicknameFromUser(name);
		String purpose = OpBlockchainRules.getSiteFromUser(name);
		if(!OpBlockchainRules.validateNickname(purpose)) {
    		throw new IllegalArgumentException(String.format("The purpose '%s' couldn't be validated", purpose));
    	}
		String serverName = getServerUser(session);
		OpObject sop = manager.getLoginObj(nickname);
		if (!OUtils.isEmpty(pwd) || !OUtils.isEmpty(signupPrivateKey)) {
			String signedBy = nickname;
			if(!OUtils.isEmpty(signupPrivateKey)) {
				try {
					kp = manager.getLoginKeyPair(nickname, signupPrivateKey);
				} catch (FailedVerificationException e) {
					// ignore it here
				}	
				if(kp == null) {
					kp = manager.getLoginKeyPair(name, signupPrivateKey);
					signedBy = name;
				}
			} else {
				kp = manager.getLoginKeyPairFromPwd(nickname, pwd);
			}
			op.setSignedBy(signedBy);
			// sign with server is it necessary or make it optional? 
			if(!OUtils.isEmpty(serverName) && OUtils.isEmpty(signupPrivateKey)) {
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
		} else if (delete) {
			kp = getServerLoginKeyPair(session);
			op.setSignedBy(serverName);
		}
		if (kp == null) {
			throw new IllegalArgumentException("Couldn't validate/find sign up key or server key for oauth");
		}
    	
    	KeyPair loginPair = null;
		if (!delete) {
			OpObject obj = new OpObject();
			op.addCreated(obj);
			if (!OUtils.isEmpty(userDetails)) {
				obj.putObjectValue(OpBlockchainRules.F_DETAILS, formatter.fromJsonToTreeMap(userDetails));
			}
			obj.setId(nickname, purpose);
			if (!OUtils.isEmpty(loginPubKey)) {
				obj.putStringValue(OpBlockchainRules.F_ALGO, loginAlgo);
				loginPair = SecUtils.getKeyPair(loginAlgo, null, loginPubKey);
			} else {
				obj.putStringValue(OpBlockchainRules.F_ALGO, SecUtils.ALGO_EC);
				loginPair = SecUtils.generateRandomEC256K1KeyPair();
			}
			obj.putStringValue(OpBlockchainRules.F_PUBKEY, SecUtils.encodeKey(SecUtils.KEY_BASE64, loginPair.getPublic()));
		}
		Map<String, Object> refs = new TreeMap<String, Object>();
		refs.put("s", Arrays.asList(OpBlockchainRules.OP_SIGNUP, nickname));
    	op.putObjectValue(OpOperation.F_REF, refs);

    	if(otherKeyPair == null) {
    		manager.generateHashAndSign(op, kp);
    	} else {
    		manager.generateHashAndSign(op, kp, otherKeyPair);
    	}
    	if(onlyValidate) {
    		manager.validateOperation(op);
    	} else {
    		manager.addOperation(op);
    	}
    	// private key won't be stored on opendb
    	if(loginPair != null && loginPair.getPrivate() != null) {
    		op.putCacheObject(OpBlockchainRules.F_PRIVATEKEY, SecUtils.encodeKey(SecUtils.KEY_BASE64, loginPair.getPrivate()));
    	}
    	return ResponseEntity.ok(formatter.fullObjectToJson(op));
    }
    
    
}
