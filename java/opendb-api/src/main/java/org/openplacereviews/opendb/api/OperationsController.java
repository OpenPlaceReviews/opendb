package org.openplacereviews.opendb.api ;

import java.io.UnsupportedEncodingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;

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

    // TODO signup oauth, by keys
    @PostMapping(path = "/signup")
    @ResponseBody
    public String signup(@RequestParam(required = true) String name, 
    		@RequestParam(required = false) String pwd, 
    		@RequestParam(required = false) String privateKey, @RequestParam(required = false) String privateKeyFormat) throws NoSuchAlgorithmException, InvalidAlgorithmParameterException, UnsupportedEncodingException, InvalidKeyException, SignatureException {
    	OpDefinitionBean op = new OpDefinitionBean();
    	String salt = name;
    	String keyGen = SecUtils.KEYGEN_PWD_METHOD_1;
    	String algo = SecUtils.ALGO_EC;
    	op.setType(OperationsRegistry.OP_TYPE_AUTH);
    	op.setOperation(SignUpOperation.OP_ID);
    	op.putStringValue(SignUpOperation.F_NAME, name);
    	op.putStringValue(SignUpOperation.F_SALT, salt);
    	op.putStringValue(SignUpOperation.F_KEYGEN_METHOD, keyGen);
    	op.putStringValue(SignUpOperation.F_ALGO, algo);
    	op.putStringValue(SignUpOperation.F_AUTH_METHOD, SignUpOperation.METHOD_PWD);
    	op.setSignedBy(name);
    	KeyPair keyPair = SecUtils.generateKeyPairFromPassword(algo, keyGen, salt, pwd);
    	op.putStringValue(SignUpOperation.F_PUBKEY_FORMAT, SecUtils.DECODE_BASE64 + ":" + keyPair.getPublic().getFormat());
    	op.putStringValue(SignUpOperation.F_PUBKEY, SecUtils.encodeBase64(keyPair.getPublic().getEncoded()));
    	
    	validation.generateHashAndSignatureFromPwd(op, keyPair);
    	validation.addAuthOperation(name, op);
    	queue.addOperation(op);
        return validation.toJson(op);
    }
    
    // TODO login oauth, provide login Public Key
    @PostMapping(path = "/login")
    @ResponseBody
    public String login(@RequestParam(required = true) String name, 
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
    	
    	OpDefinitionBean op = new OpDefinitionBean();
    	op.setType(OperationsRegistry.OP_TYPE_AUTH);
    	op.setOperation(LoginOperation.OP_ID);
    	op.putStringValue(LoginOperation.F_NAME, name);
    	op.setSignedBy(name);
    	op.putStringValue(LoginOperation.F_ALGO, SecUtils.ALGO_EC);
    	KeyPair loginPair = SecUtils.generateRandomEC256K1KeyPair();
    	op.putStringValue(LoginOperation.F_PUBKEY_FORMAT, SecUtils.DECODE_BASE64 + ":" + loginPair.getPublic().getFormat());
    	op.putStringValue(LoginOperation.F_PUBKEY, SecUtils.encodeBase64(loginPair.getPublic().getEncoded()));
    	
    	
    	validation.generateHashAndSignatureFromPwd(op, kp);
    	validation.addAuthOperation(name, op);
    	queue.addOperation(op);
    	OpDefinitionBean copy = new OpDefinitionBean(op);
    	
    	copy.putStringValue(LoginOperation.F_PRIVATEKEY_FORMAT, SecUtils.DECODE_BASE64 + ":" + loginPair.getPrivate().getFormat());
    	copy.putStringValue(LoginOperation.F_PRIVATEKEY, SecUtils.encodeBase64(loginPair.getPrivate().getEncoded()));
        return validation.toJson(copy);
    }
}