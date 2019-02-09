package org.openplacereviews.opendb.api ;

import java.security.KeyPair;

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

    // TODO support login Key Pair!
    @PostMapping(path = "/sign")
    @ResponseBody
    public String signMessage(@RequestParam(required = true) String json, @RequestParam(required = true) String name, 
    		@RequestParam(required = false) String pwd, 
    		@RequestParam(required = false) String privateKey, @RequestParam(required = false) String privateKeyFormat) throws Exception {
    	KeyPair kp = null;
		if (!Utils.isEmpty(pwd)) {
			kp = validation.getSignUpKeyPairFromPwd(name, pwd);
		} else if (!Utils.isEmpty(privateKey)) {
			kp = validation.getSignUpKeyPair(name, privateKey, privateKeyFormat);
		}
		if (kp == null) {
			throw new IllegalArgumentException("Couldn't validate sign up key");
		}
		OpDefinitionBean op = validation.parseOperation(json);
		op.setSignedBy(name);
    	validation.generateHashAndSign(op, kp);
        return validation.toJson(op);
    }

    // TODO signup oauth
    // TODO sign by server relay
    @PostMapping(path = "/signup")
    @ResponseBody
    public String signup(@RequestParam(required = true) String name, 
    		@RequestParam(required = false) String pwd, 
    		@RequestParam(required = false) String algo, 
    		@RequestParam(required = false) String privateKey, @RequestParam(required = false) String privateKeyFormat,
    		@RequestParam(required = false) String publicKey, @RequestParam(required = false) String publicKeyFormat) throws Exception {
    	OpDefinitionBean op = new OpDefinitionBean();
    	name = name.trim(); // reduce errors by having trailing spaces
    	if(!SignUpOperation.validateNickname(name)) {
    		throw new IllegalArgumentException(String.format("The nickname '%s' couldn't be validated", name));
    	}
    	String salt = name;
    	String keyGen = SecUtils.KEYGEN_PWD_METHOD_1;
    	
    	op.setType(OperationsRegistry.OP_TYPE_AUTH);
    	op.setOperation(SignUpOperation.OP_ID);
    	op.putStringValue(SignUpOperation.F_NAME, name);
    	op.putStringValue(SignUpOperation.F_SALT, salt);
    	op.putStringValue(SignUpOperation.F_KEYGEN_METHOD, keyGen);
    	if(Utils.isEmpty(algo )) {
    		algo = SecUtils.ALGO_EC;
    	}
    	KeyPair keyPair;
    	if(!Utils.isEmpty(pwd)) {
    		op.putStringValue(SignUpOperation.F_AUTH_METHOD, SignUpOperation.METHOD_PWD);
    		algo = SecUtils.ALGO_EC;
    		keyPair = SecUtils.generateKeyPairFromPassword(algo, keyGen, salt, pwd);
    	} else {
    		op.putStringValue(SignUpOperation.F_AUTH_METHOD, SignUpOperation.METHOD_PROVIDED);
    		keyPair = SecUtils.getKeyPair(algo, privateKeyFormat, privateKey, publicKeyFormat, publicKey);
    	}
    	op.putStringValue(SignUpOperation.F_ALGO, algo);
    	op.setSignedBy(name);
    	op.putStringValue(SignUpOperation.F_PUBKEY_FORMAT, SecUtils.DECODE_BASE64 + ":" + keyPair.getPublic().getFormat());
    	op.putStringValue(SignUpOperation.F_PUBKEY, SecUtils.encodeBase64(keyPair.getPublic().getEncoded()));
    	
    	validation.generateHashAndSignatureFromPwd(op, keyPair);
    	validation.addAuthOperation(name, op);
    	queue.addOperation(op);
        return validation.toJson(op);
    }
    
    // TODO login oauth
    // TODO sign by server relay
    @PostMapping(path = "/login")
    @ResponseBody
    public String login(@RequestParam(required = true) String name, 
    		@RequestParam(required = false) String pwd, 
    		@RequestParam(required = false) String signUpPrivateKey, @RequestParam(required = false) String signUpPrivateKeyFormat,
    		@RequestParam(required = false) String loginAlgo, 
    		@RequestParam(required = false) String loginPubKey, @RequestParam(required = false) String loginPubKeyFormat) throws Exception {
		KeyPair kp = null;
		if (!Utils.isEmpty(pwd)) {
			kp = validation.getSignUpKeyPairFromPwd(name, pwd);
		} else if (!Utils.isEmpty(signUpPrivateKey)) {
			kp = validation.getSignUpKeyPair(name, signUpPrivateKey, signUpPrivateKeyFormat);
		}
		if (kp == null) {
			throw new IllegalArgumentException("Couldn't validate sign up key");
		}
    	
    	OpDefinitionBean op = new OpDefinitionBean();
    	op.setType(OperationsRegistry.OP_TYPE_AUTH);
    	op.setOperation(LoginOperation.OP_ID);
    	op.putStringValue(LoginOperation.F_NAME, name);
    	op.setSignedBy(name);
    	KeyPair loginPair ;
		if (!Utils.isEmpty(loginAlgo)) {
    		loginPair = SecUtils.getKeyPair(loginAlgo, null, null, loginPubKeyFormat, loginPubKey);
    	} else {
    		op.putStringValue(LoginOperation.F_ALGO, SecUtils.ALGO_EC);
    		loginPair = SecUtils.generateRandomEC256K1KeyPair();
    	}
    	op.putStringValue(LoginOperation.F_PUBKEY_FORMAT, SecUtils.DECODE_BASE64 + ":" + loginPair.getPublic().getFormat());
		op.putStringValue(LoginOperation.F_PUBKEY, SecUtils.encodeBase64(loginPair.getPublic().getEncoded()));
    	
    	
    	validation.generateHashAndSignatureFromPwd(op, kp);
    	validation.addAuthOperation(name, op);
    	queue.addOperation(op);
    	OpDefinitionBean copy = new OpDefinitionBean(op);
    	if(loginPair.getPrivate() != null) {
    		copy.putStringValue(LoginOperation.F_PRIVATEKEY_FORMAT, SecUtils.DECODE_BASE64 + ":" + loginPair.getPrivate().getFormat());
    		copy.putStringValue(LoginOperation.F_PRIVATEKEY, SecUtils.encodeBase64(loginPair.getPrivate().getEncoded()));
    	}
        return validation.toJson(copy);
    }
}