package org.openplacereviews.opendb.api ;

import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.FailedVerificationException;
import org.openplacereviews.opendb.OUtils;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpBlockChain.ObjectsSearchRequest;
import org.openplacereviews.opendb.ops.OpBlockchainRules;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.LogOperationService;
import org.openplacereviews.opendb.service.LogOperationService.LogEntry;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
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
    
    @Autowired
    private LogOperationService logService;
    
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
 
   
    @GetMapping(path = "/queue", produces = "text/json;charset=UTF-8")
    @ResponseBody
	public String queueList() throws FailedVerificationException {
		OpBlock bl = new OpBlock();
		for (OpOperation ob : manager.getBlockchain().getQueueOperations()) {
			bl.addOperation(ob);
		}
		return formatter.fullObjectToJson(bl);
	}
    
    public static class LogResult {
		public Collection<LogEntry> logs;
    }

    @GetMapping(path = "/logs", produces = "text/json;charset=UTF-8")
    @ResponseBody
	public String logsList() throws FailedVerificationException {
    	LogResult r = new LogResult();
    	r.logs = logService.getLog();
		return formatter.fullObjectToJson(r);
	}
    
    public static class BlockchainResult {
    	public String status;
    	public String serverUser;
		public Collection<OpBlock> blockchain;
    }
    
    public static class ObjectsResult {
		public Collection<OpObject> objects;
    }
    
    
    @GetMapping(path = "/blocks", produces = "text/json;charset=UTF-8")
    @ResponseBody
	public String blocksList(@RequestParam(required = false, defaultValue="50") int depth) throws FailedVerificationException {
		BlockchainResult res = new BlockchainResult();
		res.blockchain = manager.getBlockchain().getBlockHeaders(depth);
		res.serverUser = manager.getServerUser();
		res.status = manager.getCurrentState();
		return formatter.fullObjectToJson(res);
	}
    
    @GetMapping(path = "/block-by-hash", produces = "text/json;charset=UTF-8")
    @ResponseBody
    public String getBlockByHash(@RequestParam(required = true) String hash) {
    	OpBlock blockHeader = manager.getBlockchain().getBlockHeaderByRawHash(OpBlockchainRules.getRawHash(hash));
    	return formatter.fullObjectToJson(blockHeader);
    }
    
    
    
    
    @GetMapping(path = "/objects", produces = "text/json;charset=UTF-8")
    @ResponseBody
	public String objects(@RequestParam(required = true) String type, 
			@RequestParam(required = false, defaultValue="100") int limit) throws FailedVerificationException {
    	OpBlockChain blc = manager.getBlockchain();
    	ObjectsResult res = new ObjectsResult();
    	ObjectsSearchRequest r = new ObjectsSearchRequest();
    	r.limit = limit;
    	blc.getObjects(type, r);
    	res.objects = r.result;
		return formatter.fullObjectToJson(res);
	}
    
    
    @GetMapping(path = "/object-by-id", produces = "text/json;charset=UTF-8")
    @ResponseBody
	public String objects(@RequestParam(required = true) String type, 
			@RequestParam(required = false) String key, @RequestParam(required = false) String key2) throws FailedVerificationException {
    	OpBlockChain blc = manager.getBlockchain();
    	OpObject obj;
    	if(OUtils.isEmpty(key2)) {
    		obj = blc.getObjectByName(type, key);
    	} else {
    		obj = blc.getObjectByName(type, key, key2);
    	}
		return formatter.fullObjectToJson(obj);
	}
    

    
    @GetMapping(path = "/block-bootstrap", produces = "text/json;charset=UTF-8")
    @ResponseBody
    public InputStreamResource block(@RequestParam(required = true) String id) {
        return new InputStreamResource(formatter.getBlock(id));
    }
    
}