package org.openplacereviews.opendb.api ;

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.SecUtils;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpDefinitionBean;
import org.openplacereviews.opendb.service.BlocksFormatting;
import org.openplacereviews.opendb.service.OperationsQueue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping("/queue")
public class QueueController {
	
    protected static final Log LOGGER = LogFactory.getLog(QueueController.class);
    
    @Autowired
    private OperationsQueue queue;
    
    @Autowired
    private BlocksFormatting formatter;

    @PostMapping(path = "/add")
    @ResponseBody
    public String addToQueue(@RequestParam(required = true) String id) {
    	OpBlock block = formatter.parseBootstrapBlock(id);
    	queue.addOperations(block.getOperations());
        return "OK";
    }
    
    @PostMapping(path = "/clear")
    @ResponseBody
    public String addToQueue() {
    	queue.clearOperations();
        return "OK";
    }
    
    @GetMapping(path = "/list", produces = "text/json;charset=UTF-8")
    @ResponseBody
    public String queueList() throws InvalidKeyException, SignatureException, NoSuchAlgorithmException, UnsupportedEncodingException, InvalidKeySpecException {
    	OpBlock bl = new OpBlock();
    	for(OpDefinitionBean ob : queue.getOperationsQueue()) {
//    		formatter.calculateOperationHash(ob, false);
    		// TODO
    		Map<String, String> sig = ob.getStringMap(OpDefinitionBean.F_SIGNATURE);
    		if(sig != null) {
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
    
}