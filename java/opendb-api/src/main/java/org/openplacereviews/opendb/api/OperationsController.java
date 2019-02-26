package org.openplacereviews.opendb.api ;

import java.security.KeyPair;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.FailedVerificationException;
import org.openplacereviews.opendb.OUtils;
import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.ops.OpBlockchainRules;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.OperationsQueueManager;
import org.openplacereviews.opendb.service.OperationsRegistry;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping("/op")
public class OperationsController {
	
    protected static final Log LOGGER = LogFactory.getLog(OperationsController.class);
    
    @Autowired
    private BlocksManager manager;
    
    @Autowired
    private OperationsQueueManager queue;
    
    @Autowired
    private JsonFormatter formatter;

    @PostMapping(path = "/sign")
    @ResponseBody
    public String signMessage(@RequestParam(required = true) String json, @RequestParam(required = false) String name, 
    		@RequestParam(required = false) String pwd, @RequestParam(required = false) String privateKey, 
    		@RequestParam(required = false) String dontSignByServer)
			throws FailedVerificationException {
		KeyPair kp = null;
		KeyPair altKp = null;
		OpOperation op = formatter.parseOperation(json);
		if (!OUtils.isEmpty(name)) {
			if (!OUtils.isEmpty(pwd)) {
				kp = validation.getQueueUsers().getSignUpKeyPairFromPwd(name, pwd);
			} else if (!OUtils.isEmpty(privateKey)) {
				kp = validation.getQueueUsers().getSignUpKeyPair(name, privateKey);
			}
			if (kp == null) {
				throw new IllegalArgumentException("Couldn't validate sign up key");
			}
			op.setSignedBy(name);
		}
		if (!OUtils.isEmpty(manager.getServerUser()) && OUtils.isEmpty(dontSignByServer)) {
			if (!OUtils.isEmpty(name)) {
				op.addOtherSignedBy(manager.getServerUser());
				altKp = manager.getServerLoginKeyPair(validation.getQueueUsers());
			} else {
				op.setSignedBy(manager.getServerUser());
				kp = manager.getServerLoginKeyPair(validation.getQueueUsers());
			}
		}
		if (altKp != null) {
			validation.generateHashAndSign(op, kp, altKp);
		} else if(kp != null) {
			validation.generateHashAndSign(op, kp);
		}
		return formatter.toJson(op);
	}
    
    @PostMapping(path = "/signup")
    @ResponseBody
    public String signup(@RequestParam(required = true) String name,  
    		@RequestParam(required = false) String pwd,  
    		@RequestParam(required = false) String algo, @RequestParam(required = false) String privateKey, @RequestParam(required = false) String publicKey,
    		@RequestParam(required = false) String oauthProvider, @RequestParam(required = false) String oauthId, 
    		@RequestParam(required = false) String userDetails) throws FailedVerificationException {
    	OpOperation op = new OpOperation();
    	name = name.trim(); // reduce errors by having trailing spaces
    	if(!OpBlockchainRules.validateNickname(name)) {
    		throw new IllegalArgumentException(String.format("The nickname '%s' couldn't be validated", name));
    	}
    	
    	op.setOperationType(OperationsRegistry.OP_SIGNUP);
    	op.putStringValue(OpBlockchainRules.F_NAME, name);
    	if(!OUtils.isEmpty(userDetails)) {
    		op.putObjectValue(OpBlockchainRules.F_DETAILS, formatter.fromJsonToTreeMap(userDetails));
    	}
    	
		if (OUtils.isEmpty(algo)) {
    		algo = SecUtils.ALGO_EC;
    	}
    	KeyPair keyPair;
    	KeyPair otherKeyPair = null;
    	if(!OUtils.isEmpty(pwd)) {
    		op.putStringValue(OpBlockchainRules.F_AUTH_METHOD, OpBlockchainRules.METHOD_PWD);
    		algo = SecUtils.ALGO_EC;
    		String salt = name;
    		String keyGen = SecUtils.KEYGEN_PWD_METHOD_1;
    		keyPair = SecUtils.generateKeyPairFromPassword(algo, keyGen, salt, pwd);
    		op.putStringValue(OpBlockchainRules.F_SALT, salt);
        	op.putStringValue(OpBlockchainRules.F_KEYGEN_METHOD, keyGen);
    		op.setSignedBy(name);
    		if(!OUtils.isEmpty(manager.getServerUser())) {
    			op.addOtherSignedBy(manager.getServerUser());
    			otherKeyPair = manager.getServerLoginKeyPair(validation.getQueueUsers());
    			if(otherKeyPair == null) {
    				throw new IllegalArgumentException(
    						String.format("Server %s to signup user doesn't have valid login key", manager.getServerUser()));
    			}
    		}
    	} else if(!OUtils.isEmpty(oauthId)) {
    		op.putStringValue(OpBlockchainRules.F_AUTH_METHOD, OpBlockchainRules.METHOD_OAUTH);
    		op.putStringValue(OpBlockchainRules.F_SALT, name);
			op.putStringValue(OpBlockchainRules.F_OAUTHID_HASH, SecUtils.calculateHashWithAlgo(SecUtils.HASH_SHA256, name, oauthId));
			op.putStringValue(OpBlockchainRules.F_OAUTH_PROVIDER, oauthProvider);
    		keyPair = manager.getServerLoginKeyPair(validation.getQueueUsers());
    		op.setSignedBy(manager.getServerUser());
    	} else {
    		op.putStringValue(OpBlockchainRules.F_AUTH_METHOD, OpBlockchainRules.METHOD_PROVIDED);
    		op.setSignedBy(name);
    		keyPair = SecUtils.getKeyPair(algo, privateKey, publicKey);
    	}
    	if(keyPair == null) {
			throw new IllegalArgumentException(
					String.format("Signup private / public key could not be generated"));
		}
    	op.putStringValue(OpBlockchainRules.F_ALGO, algo);
    	op.putStringValue(OpBlockchainRules.F_PUBKEY, SecUtils.encodeKey(SecUtils.KEY_BASE64, keyPair.getPublic()));
    	
    	if(otherKeyPair == null) {
    		validation.generateHashAndSign(op, keyPair);
    	} else {
    		validation.generateHashAndSign(op, keyPair, otherKeyPair);
    	}
    	queue.addOperation(op);
        return formatter.toJson(op);
    }
    
    @PostMapping(path = "/login")
    @ResponseBody
    public String login(@RequestParam(required = true) String name,  
    		@RequestParam(required = false) String pwd, @RequestParam(required = false) String signupPrivateKey,
    		@RequestParam(required = false) String oauthProvider, @RequestParam(required = false) String oauthId, 
    		@RequestParam(required = false) String loginAlgo, @RequestParam(required = false) String loginPubKey) throws FailedVerificationException {
    	OpOperation op = new OpOperation();
    	op.setOperationType(OperationsRegistry.OP_LOGIN);
    	op.putStringValue(OpBlockchainRules.F_NAME, name);
		KeyPair kp = null;
		KeyPair otherKeyPair = null;
		String nickname = OpBlockchainRules.getNicknameFromUser(name);
		String purpose = OpBlockchainRules.getSiteFromUser(name);
		if(!OpBlockchainRules.validateNickname(purpose)) {
    		throw new IllegalArgumentException(String.format("The purpose '%s' couldn't be validated", purpose));
    	}
		ActiveUsersContext queueUsers = validation.getQueueUsers();
		String serverName = manager.getServerUser();
		if (!OUtils.isEmpty(pwd) || !OUtils.isEmpty(signupPrivateKey)) {
			if(!OUtils.isEmpty(signupPrivateKey)) {
				kp = queueUsers.getSignUpKeyPair(nickname, signupPrivateKey);	
			} else {
				kp = queueUsers.getSignUpKeyPairFromPwd(nickname, pwd);
			}
			op.setSignedBy(nickname);
			// sign with server is it necessary or make it optional? 
			if(!OUtils.isEmpty(serverName)) {
    			op.addOtherSignedBy(serverName);
    			otherKeyPair = manager.getServerLoginKeyPair(queueUsers);
    			if(otherKeyPair == null) {
    				throw new IllegalArgumentException(
    						String.format("Server %s to signup user doesn't have valid login key", serverName));
    			}
    		}
		} else if (!OUtils.isEmpty(oauthId)) {
			kp = manager.getServerLoginKeyPair(queueUsers);
			OpOperation sop = queueUsers.getSignUpOperation(nickname);
			if(!SecUtils.validateHash(sop.getStringValue(OpBlockchainRules.F_OAUTHID_HASH), 
					sop.getStringValue(OpBlockchainRules.F_SALT), oauthId) || 
					!oauthProvider.equals(sop.getStringValue(OpBlockchainRules.F_OAUTH_PROVIDER))) {
				throw new IllegalArgumentException("User was registered with different oauth id");
			}
			op.setSignedBy(serverName);
		}
		if (kp == null) {
			throw new IllegalArgumentException("Couldn't validate sign up key or server key for oauth");
		}
    	
    	KeyPair loginPair;
		if (!OUtils.isEmpty(loginPubKey)) {
			op.putStringValue(OpBlockchainRules.F_ALGO, loginAlgo);
    		loginPair = SecUtils.getKeyPair(loginAlgo, null, loginPubKey);
    	} else {
    		op.putStringValue(OpBlockchainRules.F_ALGO, SecUtils.ALGO_EC);
    		loginPair = SecUtils.generateRandomEC256K1KeyPair();
    	}
		op.putStringValue(OpBlockchainRules.F_PUBKEY, SecUtils.encodeKey(SecUtils.KEY_BASE64, loginPair.getPublic()));
    	
    	
    	if(otherKeyPair == null) {
    		validation.generateHashAndSign(op, kp);
    	} else {
    		validation.generateHashAndSign(op, kp, otherKeyPair);
    	}
    	
    	queue.addOperation(op);
    	// private key won't be stored on opendb
    	if(loginPair.getPrivate() != null) {
    		OpOperation copy = new OpOperation(op);
    		copy.putStringValue(UsersAndRolesRegistry.F_PRIVATEKEY, SecUtils.encodeKey(SecUtils.KEY_BASE64, loginPair.getPrivate()));
    		return formatter.toJson(copy);
    	}
        return formatter.toJson(op);
    }
}