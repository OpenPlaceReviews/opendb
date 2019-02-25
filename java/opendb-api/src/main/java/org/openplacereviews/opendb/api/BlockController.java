package org.openplacereviews.opendb.api ;

import java.security.KeyPair;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.FailedVerificationException;
import org.openplacereviews.opendb.OUtils;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.BlocksManager.BlockchainState;
import org.openplacereviews.opendb.service.UsersAndRolesRegistry;
import org.openplacereviews.opendb.service.OperationsQueueManager;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;


@Controller
@RequestMapping("/block")
public class BlockController {
	
    protected static final Log LOGGER = LogFactory.getLog(BlockController.class);
    
    @Autowired
    private BlocksManager manager;
    
    @Autowired
    private OperationsQueueManager queue;
    
    @Autowired
    private UsersAndRolesRegistry usersRegistry;
    
    @Autowired
    private JsonFormatter formatter;

    @PostMapping(path = "/create", produces = "text/json;charset=UTF-8")
    @ResponseBody
    public String createBlock() {
    	return manager.createBlock();
    }
    
    @PostMapping(path = "/toggle-pause", produces = "text/json;charset=UTF-8")
    @ResponseBody
    public String toggleBlockCreation() {
    	if(manager.getCurrentState() == BlockchainState.BLOCKCHAIN_PAUSED) {
    		manager.resumeBlockCreation();
    	} else if(manager.getCurrentState() == BlockchainState.BLOCKCHAIN_READY) {
    		manager.pauseBlockCreation();
    	} else {
			return "{\"status\":\"FAILED\", \"msg\":\"Current state is not ready: " + manager.getCurrentState() + "\"}";
    	}
    	return "{\"status\":\"OK\"}";
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
		if (!OUtils.isEmpty(serverName)) {
			KeyPair kp = null;
			for (OpOperation o : block.getOperations()) {
				OpOperation op = o;
				if (!OUtils.isEmpty(serverName) && o.getSignedBy().isEmpty()) {
					if(kp == null) {
						kp = usersRegistry.getQueueUsers().getLoginKeyPair(serverName, privateKey);
					}
					op.setSignedBy(serverName);
					op = usersRegistry.generateHashAndSign(op, kp);
				}
				queue.addOperation(op);
			}
		}
		// return manager.createBlock();
		return "{}";
    }
    
    public static class BlockchainResult {
    	public String status;
    	public String serverUser;
    	public OpBlock currentBlock;
		public OpOperation currentTx;
		public List<OpBlock> blockchain;
    }
    
    
    @GetMapping(path = "/list", produces = "text/json;charset=UTF-8")
    @ResponseBody
	public String blocksList() throws FailedVerificationException {
		BlockchainResult res = new BlockchainResult();
		res.blockchain = manager.getBlockcchain();
		res.serverUser = manager.getServerUser();
		res.currentBlock = manager.getCurrentBlock();
		res.currentTx = manager.getCurrentTx();
		res.status = manager.getCurrentState().name();
		return formatter.objectToJson(res);
	}
}