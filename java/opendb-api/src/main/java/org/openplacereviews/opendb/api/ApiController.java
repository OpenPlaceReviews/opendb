package org.openplacereviews.opendb.api ;

import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.FailedVerificationException;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.service.BlocksManager;
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
@RequestMapping("/api")
public class ApiController {
	
    protected static final Log LOGGER = LogFactory.getLog(ApiController.class);
    
    @Autowired
    private BlocksManager manager;
    
    @Autowired
    private JsonFormatter formatter;
    
    @GetMapping(path = "/status", produces = "text/html;charset=UTF-8")
    @ResponseBody
    public String status() {
     	return "{\"status\":\"OK\"}";
    }
    
    @GetMapping(path = "/admin", produces = "text/html;charset=UTF-8")
    @ResponseBody
    public InputStreamResource testHarness() {
        return new InputStreamResource(ApiController.class.getResourceAsStream("/admin.html"));
    }
 
   
    @PostMapping(path = "/queue/clear")
    @ResponseBody
    public String addToQueue() {
    	manager.clearQueue();
        return "{\"status\":\"OK\"}";
    }
    
    @GetMapping(path = "/queue", produces = "text/json;charset=UTF-8")
    @ResponseBody
	public String queueList() throws FailedVerificationException {
		OpBlock bl = new OpBlock();
		for (OpOperation ob : manager.getBlockchain().getOperations()) {
			bl.getOperations().add(ob);
		}
		return formatter.toJson(bl);
	}
    
    public static class BlockchainResult {
    	public String status;
    	public String serverUser;
		public Collection<OpBlock> blockchain;
    }
    
    
    @GetMapping(path = "/blocks", produces = "text/json;charset=UTF-8")
    @ResponseBody
	public String blocksList(@RequestParam(required = false, defaultValue="50") int depth) throws FailedVerificationException {
		BlockchainResult res = new BlockchainResult();
		res.blockchain = manager.getBlockchain().getBlocks(depth);
		res.serverUser = manager.getServerUser();
		res.status = manager.getCurrentState();
		return formatter.objectToJson(res);
	}
    

    
    @GetMapping(path = "/block", produces = "text/json;charset=UTF-8")
    @ResponseBody
    public InputStreamResource block(@RequestParam(required = true) String id) {
        return new InputStreamResource(formatter.getBlock(id));
    }
    
}