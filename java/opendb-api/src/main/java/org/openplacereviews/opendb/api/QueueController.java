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
import org.openplacereviews.opendb.service.OpenDBValidator;
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
    private OpenDBValidator formatter;

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
    		Map<String, String> sig = ob.getStringMap(OpDefinitionBean.F_SIGNATURE);
    		if(sig != null) {
    			boolean validate = formatter.validateSignature(ob);
    			sig.put("valid", validate + "");
    		}
    		bl.getOperations().add(ob);	
    	}
    	return formatter.toJson(bl);
    }
    
}