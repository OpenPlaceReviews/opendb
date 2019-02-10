package org.openplacereviews.opendb.api ;

import java.security.KeyPair;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.Utils;
import org.openplacereviews.opendb.ops.OpDefinitionBean;
import org.openplacereviews.opendb.ops.OperationsRegistry;
import org.openplacereviews.opendb.ops.auth.LoginOperation;
import org.openplacereviews.opendb.ops.auth.SignUpOperation;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.OpenDBValidator;
import org.openplacereviews.opendb.service.OperationsQueue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

@Controller
@RequestMapping("/op")
public class OperationsController {
	
    protected static final Log LOGGER = LogFactory.getLog(OperationsController.class);
    
    @Autowired
    private BlocksManager manager;
    
    @Autowired
    private OpenDBValidator validation;
    
    @Autowired
    private OperationsQueue queue;
    
    private Gson gson = new GsonBuilder().create();

    @PostMapping(path = "/sign")
    @ResponseBody
    public String signMessage(@RequestParam(required = true) String json, @RequestParam(required = true) String name, 
    		@RequestParam(required = false) String pwd, @RequestParam(required = false) String privateKey) throws Exception {
    	KeyPair kp = null;
		if (!Utils.isEmpty(pwd)) {
			kp = validation.getSignUpKeyPairFromPwd(name, pwd);
		} else if (!Utils.isEmpty(privateKey)) {
			kp = validation.getSignUpKeyPair(name, privateKey);
		}
		if (kp == null) {
			throw new IllegalArgumentException("Couldn't validate sign up key");
		}
		OpDefinitionBean op = validation.parseOperation(json);
		op.setSignedBy(name);
    	validation.generateHashAndSign(op, kp);
        return validation.toJson(op);
    }

    @PostMapping(path = "/signup")
    @ResponseBody
    public String signup(@RequestParam(required = true) String name, @RequestParam(required = false) String serverName, 
    		@RequestParam(required = false) String pwd, 
    		@RequestParam(required = false) String algo,
    		@RequestParam(required = false) String oauthProvider, @RequestParam(required = false) String oauthId, 
    		@RequestParam(required = false) String privateKey, 
    		@RequestParam(required = false) String publicKey,
    		@RequestParam(required = false) String userDetails) throws Exception {
    	OpDefinitionBean op = new OpDefinitionBean();
    	name = name.trim(); // reduce errors by having trailing spaces
    	if(!SignUpOperation.validateNickname(name)) {
    		throw new IllegalArgumentException(String.format("The nickname '%s' couldn't be validated", name));
    	}
    	
    	op.setType(OperationsRegistry.OP_TYPE_AUTH);
    	op.setOperation(SignUpOperation.OP_ID);
    	op.putStringValue(SignUpOperation.F_NAME, name);
    	if(!Utils.isEmpty(userDetails)) {
    		op.putObjectValue(SignUpOperation.F_DETAILS, gson.fromJson(userDetails, Map.class));
    	}
    	
		if (Utils.isEmpty(algo)) {
    		algo = SecUtils.ALGO_EC;
    	}
    	KeyPair keyPair;
    	KeyPair otherKeyPair = null;
    	if(!Utils.isEmpty(pwd)) {
    		op.putStringValue(SignUpOperation.F_AUTH_METHOD, SignUpOperation.METHOD_PWD);
    		algo = SecUtils.ALGO_EC;
    		String salt = name;
    		String keyGen = SecUtils.KEYGEN_PWD_METHOD_1;
    		keyPair = SecUtils.generateKeyPairFromPassword(algo, keyGen, salt, pwd);
    		op.putStringValue(SignUpOperation.F_SALT, salt);
        	op.putStringValue(SignUpOperation.F_KEYGEN_METHOD, keyGen);
    		op.setSignedBy(name);
    		if(serverName != null) {
    			op.addOtherSignedBy(serverName);
    			otherKeyPair = validation.getLoginKeyPair(serverName, privateKey);
    		}
    	} else if(!Utils.isEmpty(oauthId)) {
    		op.putStringValue(SignUpOperation.F_AUTH_METHOD, SignUpOperation.METHOD_OAUTH);
    		op.putStringValue(SignUpOperation.F_SALT, name);
			op.putStringValue(SignUpOperation.F_OAUTHID_HASH, SecUtils.calculateHash(SecUtils.HASH_SHA256_SALT, name, oauthId));
			op.putStringValue(SignUpOperation.F_OAUTH_PROVIDER, oauthProvider);
    		keyPair = validation.getLoginKeyPair(serverName, privateKey);
    		op.setSignedBy(serverName);
    	} else {
    		op.putStringValue(SignUpOperation.F_AUTH_METHOD, SignUpOperation.METHOD_PROVIDED);
    		op.setSignedBy(name);
    		keyPair = SecUtils.getKeyPair(algo, privateKey, publicKey);
    	}
    	op.putStringValue(SignUpOperation.F_ALGO, algo);
    	
    	op.putStringValue(SignUpOperation.F_PUBKEY, SecUtils.encodeKey(SecUtils.KEY_BASE64, keyPair.getPublic()));
    	
    	if(otherKeyPair == null) {
    		validation.generateHashAndSign(op, keyPair);
    	} else {
    		validation.generateHashAndSign(op, keyPair, otherKeyPair);
    	}
    	validation.addAuthOperation(name, op);
    	queue.addOperation(op);
        return validation.toJson(op);
    }
    
    @PostMapping(path = "/login")
    @ResponseBody
    public String login(@RequestParam(required = true) String name, @RequestParam(required = false) String serverName, 
    		@RequestParam(required = false) String pwd, 
    		@RequestParam(required = false) String oauthProvider, @RequestParam(required = false) String oauthId, 
    		@RequestParam(required = false) String privateKey, 
    		@RequestParam(required = false) String loginAlgo, 
    		@RequestParam(required = false) String loginPubKey) throws Exception {
    	OpDefinitionBean op = new OpDefinitionBean();
    	op.setType(OperationsRegistry.OP_TYPE_AUTH);
    	op.setOperation(LoginOperation.OP_ID);
    	op.putStringValue(LoginOperation.F_NAME, name);
		KeyPair kp = null;
		KeyPair otherKeyPair = null;
		if (!Utils.isEmpty(pwd)) {
			kp = validation.getSignUpKeyPairFromPwd(name, pwd);
			op.setSignedBy(name);
			if(serverName != null) {
    			op.addOtherSignedBy(serverName);
    			otherKeyPair = validation.getLoginKeyPair(serverName, privateKey);
    		}
		} else if (!Utils.isEmpty(oauthId)) {
			kp = validation.getLoginKeyPair(serverName, privateKey);
			OpDefinitionBean sop = validation.getSignUpOperation(name);
			if(!SecUtils.validateHash(sop.getStringValue(SignUpOperation.F_OAUTHID_HASH), 
					sop.getStringValue(SignUpOperation.F_SALT), oauthId) || 
					!oauthProvider.equals(sop.getStringValue(SignUpOperation.F_OAUTH_PROVIDER))) {
				throw new IllegalArgumentException("User was registered with different oauth id");
			}
			op.setSignedBy(serverName);
		} else if (!Utils.isEmpty(privateKey)) {
			kp = validation.getSignUpKeyPair(name, privateKey);
			op.setSignedBy(name);
		}
		if (kp == null) {
			throw new IllegalArgumentException("Couldn't validate sign up key or server key for oauth");
		}
    	
    	
    	
    	KeyPair loginPair;
		if (!Utils.isEmpty(loginAlgo)) {
			op.putStringValue(LoginOperation.F_ALGO, loginAlgo);
    		loginPair = SecUtils.getKeyPair(loginAlgo, null, loginPubKey);
			
    	} else {
    		op.putStringValue(LoginOperation.F_ALGO, SecUtils.ALGO_EC);
    		loginPair = SecUtils.generateRandomEC256K1KeyPair();
    	}
		op.putStringValue(LoginOperation.F_PUBKEY, SecUtils.encodeKey(SecUtils.KEY_BASE64, loginPair.getPublic()));
    	
    	
    	if(otherKeyPair == null) {
    		validation.generateHashAndSign(op, kp);
    	} else {
    		validation.generateHashAndSign(op, kp, otherKeyPair);
    	}
    	validation.addAuthOperation(name, op);
    	queue.addOperation(op);
    	// private key won't be stored on opendb
    	if(loginPair.getPrivate() != null) {
    		OpDefinitionBean copy = new OpDefinitionBean(op);
    		copy.putStringValue(LoginOperation.F_PRIVATEKEY, SecUtils.encodeKey(SecUtils.KEY_BASE64, loginPair.getPrivate()));
    		return validation.toJson(copy);
    	}
        return validation.toJson(op);
    }
}