package org.openplacereviews.opendb.api ;

import java.security.KeyPair;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpDefinitionBean;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.JsonFormatter;
import org.openplacereviews.opendb.service.OpenDBUsersRegistry;
import org.openplacereviews.opendb.service.OperationsQueueManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.jayway.jsonpath.internal.Utils;

@Controller
@RequestMapping("/block")
public class BlockController {
	
    protected static final Log LOGGER = LogFactory.getLog(BlockController.class);
    
    @Autowired
    private BlocksManager manager;
    
    @Autowired
    private OperationsQueueManager queue;
    
    @Autowired
    private OpenDBUsersRegistry usersRegistry;
    
    @Autowired
    private JsonFormatter formatter;

    @PostMapping(path = "/create", produces = "text/json;charset=UTF-8")
    @ResponseBody
    public String createBlock() {
    	return manager.createBlock();
    }
    
    
    @GetMapping(path = "/content", produces = "text/json;charset=UTF-8")
    @ResponseBody
    public InputStreamResource block(@RequestParam(required = true) String id) {
        return new InputStreamResource(formatter.getBlock(id));
    }
    
    
    @PostMapping(path = "/bootstrap", produces = "text/html;charset=UTF-8")
    @ResponseBody
    public String bootstrap(@RequestParam(required = false) String serverName,
    		@RequestParam(required = false) String privateKey) throws Exception {
    	serverName = manager.getServerUser();
    	privateKey = manager.getServerPrivateKey();
    	OpBlock block = formatter.parseBootstrapBlock("1");
		if (!Utils.isEmpty(serverName)) {
			KeyPair kp = null;
			for (OpDefinitionBean o : block.getOperations()) {
				OpDefinitionBean op = o;
				if (!Utils.isEmpty(serverName) && Utils.isEmpty(o.getSignedBy())) {
					if(kp == null) {
						kp = usersRegistry.getQueueUsers().getLoginKeyPair(serverName, privateKey);
					}
					op.setSignedBy(serverName);
					op = usersRegistry.generateHashAndSign(op, kp);
				}
				queue.addOperation(op);
			}
		}
		manager.createBlock();
	 	return "{\"status\":\"OK\"}";
    }
}