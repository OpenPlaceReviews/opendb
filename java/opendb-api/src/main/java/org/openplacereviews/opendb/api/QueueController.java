package org.openplacereviews.opendb.api ;

import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.FailedVerificationException;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpDefinitionBean;
import org.openplacereviews.opendb.service.OpenDBUsersRegistry;
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
    private OpenDBUsersRegistry validator;

    @PostMapping(path = "/add")
    @ResponseBody
    public String addToQueue(@RequestParam(required = true) String json) {
    	OpDefinitionBean op = validator.parseOperation(json);
    	queue.addOperation(op);
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
    public String queueList() throws FailedVerificationException {
    	OpBlock bl = new OpBlock();
    	for(OpDefinitionBean ob : queue.getOperationsQueue()) {
    		if(ob.hasOneSignature()) {
    			Map<String, String> sig = ob.getStringMap(OpDefinitionBean.F_SIGNATURE);
    			if(sig != null) {
    				boolean validate = validator.validateSignature(validator.getQueueUsers(), ob, sig, ob.getSignedBy());
        			sig.put("valid", validate + "");    				
    			}
    		} else {
    			List<Map<String, String>> sigs = ob.getListStringMap(OpDefinitionBean.F_SIGNATURE);
    			for(int i = 0; i < sigs.size(); i++) {
    				Map<String, String> sig  = sigs.get(i);
    				if(sig != null) {
    					boolean validate = validator.validateSignature(validator.getQueueUsers(), ob, 
    							sig, i == 0 ? ob.getSignedBy() : ob.getOtherSignedBy().get(i - 1));
            			sig.put("valid", validate + "");    				
        			}	
    			}
    		}
    		
    		bl.getOperations().add(ob);	
    	}
    	return validator.toJson(bl);
    }
    
}