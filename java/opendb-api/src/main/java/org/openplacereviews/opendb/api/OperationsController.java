package org.openplacereviews.opendb.api ;

import java.io.UnsupportedEncodingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.ops.OpDefinitionBean;
import org.openplacereviews.opendb.service.BlocksFormatting;
import org.openplacereviews.opendb.service.BlocksManager;
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
    private BlocksFormatting formatter;
    
    @PostMapping(path = "/sign")
    @ResponseBody
    public String signMessage(@RequestParam(required = true) String json, @RequestParam(required = true) String pwd) throws NoSuchAlgorithmException, InvalidAlgorithmParameterException, UnsupportedEncodingException, InvalidKeyException, SignatureException {
    	OpDefinitionBean op = formatter.parseOperation(json);
    	String hash = formatter.calculateOperationHash(op, true);
    	KeyPair keyPair = SecUtils.generateKeyPairFromPassword(op.getStringValue(
    			OpDefinitionBean.F_SALT), pwd, OpDefinitionBean.F_KEYGEN_METHOD);
    	op.remove(OpDefinitionBean.F_SIGNATURE);
    	String signature = SecUtils.signMessageWithKeyBase64(keyPair, json, SecUtils.SIG_ALGO_SHA1_EC);
    	OpDefinitionBean sig = new OpDefinitionBean();
    	sig.putStringValue(OpDefinitionBean.F_HASH, hash);
    	sig.putStringValue(OpDefinitionBean.F_PUBKEY_FORMAT, SecUtils.DECODE_BASE64 + ":" + keyPair.getPublic().getFormat());
    	sig.putStringValue(OpDefinitionBean.F_PUBKEY, SecUtils.encodeBase64(keyPair.getPublic().getEncoded()));
    	Map<String, String> signatureMap = new TreeMap<>();
    	signatureMap.put(OpDefinitionBean.F_DIGEST, signature);
    	signatureMap.put(OpDefinitionBean.F_TYPE, "json");
    	signatureMap.put(OpDefinitionBean.F_ALGO, SecUtils.SIG_ALGO_SHA1_EC);
    	signatureMap.put(OpDefinitionBean.F_FORMAT, SecUtils.DECODE_BASE64);
    	sig.putObjectValue(OpDefinitionBean.F_SIGNATURE, signatureMap);
        return formatter.toJson(sig);
    }
}