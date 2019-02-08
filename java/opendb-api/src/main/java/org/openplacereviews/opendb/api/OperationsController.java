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
    
    public static final String DEFAULT_SIGNUP_METHOD = "EC256K1_S17R8";
    public static final String DEFAULT_LOGIN_METHOD = "EC256K1";
    public static final String DEFAULT_SIGNUP_ALGO = "EC";
    public static final String DEFAULT_LOGIN_ALGO = "EC";
    
    @Autowired
    private BlocksManager manager;
    
    @Autowired
    private OpenDBValidator validation;
    
    @Autowired
    private OperationsQueue queue;

    
    
    @PostMapping(path = "/sign")
    @ResponseBody
    public String signMessage(@RequestParam(required = true) String json, 
    		@RequestParam(required = false) String pwd, @RequestParam(required = false) String prKey) throws NoSuchAlgorithmException, InvalidAlgorithmParameterException, UnsupportedEncodingException, InvalidKeyException, SignatureException {
    	OpDefinitionBean sig = validation.generateSignatureFromPwd(json, pwd);
        return validation.toJson(sig);
    }
    
    @PostMapping(path = "/signup")
    @ResponseBody
    public String signup(@RequestParam(required = true) String name, 
    		@RequestParam(required = false) String pwd, @RequestParam(required = false) String prKey) throws NoSuchAlgorithmException, InvalidAlgorithmParameterException, UnsupportedEncodingException, InvalidKeyException, SignatureException {
    	OpDefinitionBean op = new OpDefinitionBean();
    	op.setType(SignUpOperation.OP_ID);
    	op.setOperation(OperationsRegistry.OP_TYPE_AUTH);
    	op.putStringValue(SignUpOperation.F_NAME, name);
    	op.putStringValue(SignUpOperation.F_SALT, name);
    	op.putStringValue(SignUpOperation.F_KEYGEN_METHOD, DEFAULT_SIGNUP_METHOD);
    	op.putStringValue(SignUpOperation.F_ALGO, DEFAULT_SIGNUP_ALGO);
    	op.putStringValue(SignUpOperation.F_AUTH_METHOD, "pwd");
    	op.setSignedBy(name);
    	KeyPair keyPair = SecUtils.generateEC256K1KeyPairFromPassword(op.getStringValue(
    			SignUpOperation.F_SALT), pwd, op.getStringValue(SignUpOperation.F_KEYGEN_METHOD));
    	op.putStringValue(SignUpOperation.F_PUBKEY_FORMAT, SecUtils.DECODE_BASE64 + ":" + keyPair.getPublic().getFormat());
    	op.putStringValue(SignUpOperation.F_PUBKEY, SecUtils.encodeBase64(keyPair.getPublic().getEncoded()));
    	
    	validation.generateHashAndSignatureFromPwd(op, keyPair);
    	validation.addAuthOperation(name, op);
    	queue.addOperation(op);
        return validation.toJson(op);
    }
    
    
    @PostMapping(path = "/login")
    @ResponseBody
    public String login(@RequestParam(required = true) String name, 
    		@RequestParam(required = false) String pwd, @RequestParam(required = false) String prKey) throws NoSuchAlgorithmException, InvalidAlgorithmParameterException, UnsupportedEncodingException, InvalidKeyException, SignatureException {
    	OpDefinitionBean op = new OpDefinitionBean();
    	op.setType(LoginOperation.OP_ID);
    	op.setOperation(OperationsRegistry.OP_TYPE_AUTH);
    	op.putStringValue(LoginOperation.F_NAME, name);
    	op.putStringValue(LoginOperation.F_KEYGEN_METHOD, DEFAULT_LOGIN_METHOD);
    	op.putStringValue(LoginOperation.F_ALGO, DEFAULT_LOGIN_ALGO);
    	op.setSignedBy(name);
    	KeyPair loginPair = SecUtils.generateEC256K1KeyPair();
    	
    	op.putStringValue(LoginOperation.F_PUBKEY_FORMAT, SecUtils.DECODE_BASE64 + ":" + loginPair.getPublic().getFormat());
    	op.putStringValue(LoginOperation.F_PUBKEY, SecUtils.encodeBase64(loginPair.getPublic().getEncoded()));
    	
    	validation.generateHashAndSignatureFromPwd(op, name, pwd);
    	validation.addAuthOperation(name, op);
    	queue.addOperation(op);
    	OpDefinitionBean copy = new OpDefinitionBean(op);
    	
    	copy.putStringValue(LoginOperation.F_PRIVATEKEY_FORMAT, SecUtils.DECODE_BASE64 + ":" + loginPair.getPrivate().getFormat());
    	copy.putStringValue(LoginOperation.F_PRIVATEKEY, SecUtils.encodeBase64(loginPair.getPrivate().getEncoded()));
        return validation.toJson(copy);
    }
}