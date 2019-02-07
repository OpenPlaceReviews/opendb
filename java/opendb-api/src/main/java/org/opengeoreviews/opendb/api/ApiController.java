package org.opengeoreviews.opendb.api ;

import java.io.UnsupportedEncodingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.opengeoreviews.opendb.SecUtils;
import org.opengeoreviews.opendb.ops.OpBlock;
import org.opengeoreviews.opendb.ops.OpDefinitionBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping("/api")
public class ApiController {
	
    protected static final Log LOGGER = LogFactory.getLog(ApiController.class);
    
    @Autowired
    private JdbcTemplate jdbcTemplate;
    
    @Autowired
    private BlocksManager manager;
    
    @Autowired
    private OperationsQueue queue;
    
    @Autowired
    private BlocksFormatting formatter;


    @GetMapping(path = "/status", produces = "text/html;charset=UTF-8")
    @ResponseBody
    public String status() {
        return "OK";
    }
    
    @GetMapping(path = "/test", produces = "text/html;charset=UTF-8")
    @ResponseBody
    public InputStreamResource testHarness() {
        return new InputStreamResource(ApiController.class.getResourceAsStream("/test.html"));
    }
    
    @PostMapping(path = "/msg/sign")
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
    
    @PostMapping(path = "/queue/add")
    @ResponseBody
    public String addToQueue(@RequestParam(required = true) String id) {
    	OpBlock block = formatter.parseBootstrapBlock(id);
    	queue.addOperations(block.getOperations());
        return "OK";
    }
    
    @PostMapping(path = "/queue/clear")
    @ResponseBody
    public String addToQueue() {
    	queue.clearOperations();
        return "OK";
    }
    
    @GetMapping(path = "/queue/list", produces = "text/json;charset=UTF-8")
    @ResponseBody
    public String queueList() throws InvalidKeyException, SignatureException, NoSuchAlgorithmException, UnsupportedEncodingException, InvalidKeySpecException {
    	OpBlock bl = new OpBlock();
    	for(OpDefinitionBean ob : queue.getOperationsQueue()) {
//    		formatter.calculateOperationHash(ob, false);
    		Map<String, String> sig = ob.getStringMap(OpDefinitionBean.F_SIGNATURE);
    		if(sig != null) {
//    			pubkey_format
    			String algo = ob.getStringValue(OpDefinitionBean.F_ALGO);
    			String pubformat = ob.getStringValue(OpDefinitionBean.F_PUBKEY_FORMAT);
    			String pbKey = ob.getStringValue(OpDefinitionBean.F_PUBKEY);
    			KeyPair kp = SecUtils.getKeyPair(algo, null, null, pubformat, pbKey);
    			byte[] signature = SecUtils.decodeSignature(sig.get(OpDefinitionBean.F_FORMAT), sig.get(OpDefinitionBean.F_DIGEST));
    			boolean validate = SecUtils.validateSignature(kp, formatter.toValidateSignatureJson(ob), sig.get(OpDefinitionBean.F_ALGO), signature);
    			sig.put("valid", validate + "");
    		}
    		bl.getOperations().add(ob);	
    	}
    	return formatter.toJson(bl);
    }
    
    
    @PostMapping(path = "/block/create", produces = "text/json;charset=UTF-8")
    @ResponseBody
    public String createBlock() {
    	return manager.createBlock();
    }
    
    
    @GetMapping(path = "/block/content", produces = "text/json;charset=UTF-8")
    @ResponseBody
    public InputStreamResource block(@RequestParam(required = true) String id) {
        return new InputStreamResource(formatter.getBlock(id));
    }
    
    

    
    @PostMapping(path = "/block/bootstrap", produces = "text/html;charset=UTF-8")
    @ResponseBody
    public String bootstrap() {
    	OpBlock block = formatter.parseBootstrapBlock("1");
    	manager.replicateBlock(block);
        return "OK";
    }
}