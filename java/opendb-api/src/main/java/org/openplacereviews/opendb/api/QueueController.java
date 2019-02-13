package org.openplacereviews.opendb.api ;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.FailedVerificationException;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpDefinitionBean;
import org.openplacereviews.opendb.service.OpenDBUsersRegistry;
import org.openplacereviews.opendb.service.OperationsQueueManager;
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
    private OperationsQueueManager queue;
    
    @Autowired
    private OpenDBUsersRegistry validator;

    @PostMapping(path = "/add")
    @ResponseBody
    public String addToQueue(@RequestParam(required = true) String json) {
    	OpDefinitionBean op = validator.parseOperation(json);
    	queue.addOperation(op);
    	return "{\"status\":\"OK\"}";
    }
    
    @PostMapping(path = "/clear")
    @ResponseBody
    public String addToQueue() {
    	queue.clearOperations();
        return "{\"status\":\"OK\"}";
    }
    
    @GetMapping(path = "/list", produces = "text/json;charset=UTF-8")
    @ResponseBody
	public String queueList() throws FailedVerificationException {
		OpBlock bl = new OpBlock();
		for (OpDefinitionBean ob : queue.getOperationsQueue()) {
			Map<String, String> validation = new LinkedHashMap<String, String>();
			validation.put("validate_signatures", validator.validateSignatures(validator.getQueueUsers(), ob)+ "");
			validation.put("validate_hash", validator.validateHash(ob)+ "");
			validation.put("validate_sig_hash", validator.validateSignatureHash(ob) + "");
			OpDefinitionBean c = new OpDefinitionBean(ob);
			c.putObjectValue("validation", validation);
			bl.getOperations().add(c);
		}
		return validator.toJson(bl);
	}
    
}