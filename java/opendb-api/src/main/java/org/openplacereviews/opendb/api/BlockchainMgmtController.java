package org.openplacereviews.opendb.api ;

import java.security.KeyPair;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.FailedVerificationException;
import org.openplacereviews.opendb.OUtils;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;


@Controller
@RequestMapping("/blockchain-mgmt")
public class BlockchainMgmtController {
	
    protected static final Log LOGGER = LogFactory.getLog(BlockchainMgmtController.class);
    
    @Autowired
    private BlocksManager manager;
    
    
    @Autowired
    private JsonFormatter formatter;

    @PostMapping(path = "/create", produces = "text/json;charset=UTF-8")
    @ResponseBody
    public String createBlock() throws FailedVerificationException {
    	OpBlock block = manager.createBlock();
    	if(block == null) {
    		return "{\"status\":\"FAILED\", \"msg\":\"Block creation failed\"}";
    	}
    	return formatter.objectToJson(block);
    }
    
    @PostMapping(path = "/revert-superblock", produces = "text/json;charset=UTF-8")
    @ResponseBody
    public String revertSuperblock() throws FailedVerificationException {
    	if(!manager.revertSuperblock()) {
    		return "{\"status\":\"FAILED\", \"msg\":\"Revert super block failed\"}";
    	}
    	return "{\"status\":\"OK\", \"msg\":\"Blocks are reverted and operations added to the queue.\"}";
    }
    
    @PostMapping(path = "/toggle-pause", produces = "text/json;charset=UTF-8")
    @ResponseBody
    public String toggleBlockCreation() {
    	if(manager.isBlockchainPaused()) {
    		manager.resumeBlockCreation();
    	} else {
    		manager.pauseBlockCreation();
    	}
    	return "{\"status\":\"OK\"}";
    }
    
    @PostMapping(path = "/bootstrap", produces = "text/html;charset=UTF-8")
    @ResponseBody
    public String bootstrap(@RequestParam(required = false) String serverName,
    		@RequestParam(required = false) String privateKey) throws Exception {
    	serverName = manager.getServerUser();
    	privateKey = manager.getServerPrivateKey();
    	OpBlock block = formatter.parseBootstrapBlock("1");
		if (!OUtils.isEmpty(serverName)) {
			KeyPair kp = null;
			for (OpOperation o : block.getOperations()) {
				OpOperation op = o;
				if (!OUtils.isEmpty(serverName) && o.getSignedBy().isEmpty()) {
					if(kp == null) {
						kp = manager.getLoginKeyPair(serverName, privateKey);
					}
					op.setSignedBy(serverName);
					op = manager.generateHashAndSign(op, kp);
				}
				manager.addOperation(op);
			}
		}
		return "{}";
    }
    
    
}