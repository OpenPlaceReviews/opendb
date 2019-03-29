package org.openplacereviews.opendb.api ;

import java.security.KeyPair;

import javax.servlet.http.HttpSession;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.FailedVerificationException;
import org.openplacereviews.opendb.OUtils;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.LogOperationService;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;


@Controller
@RequestMapping("/api/mgmt")
public class MgmtController {
	
    protected static final Log LOGGER = LogFactory.getLog(MgmtController.class);
    
    @Autowired
    private BlocksManager manager;
    
    @Autowired
    private LogOperationService logService;
    
    @Autowired
    private JsonFormatter formatter;
    
    public boolean validateServerLogin(HttpSession session) {
    	String loginName = (String) session.getAttribute(OpApiController.ADMIN_LOGIN_NAME);
    	return OUtils.equals(loginName, manager.getServerUser());
	}
    
    private KeyPair getServerLoginKeyPair(HttpSession session) {
    	return manager.getServerLoginKeyPair();
	}
    
    private String getServerUser(HttpSession session) {
    	return manager.getServerUser();
	}
    
    private ResponseEntity<String> unauthorizedByServer() {
    	return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
    			.body("{\"status\":\"ERROR\"}");
	}
    
    @PostMapping(path = "/create", produces = "text/json;charset=UTF-8")
    @ResponseBody
    public ResponseEntity<String> createBlock(HttpSession session) throws FailedVerificationException {
    	if(!validateServerLogin(session)) {
    		return unauthorizedByServer();
    	}
    	OpBlock block = manager.createBlock();
    	if(block == null) {
    		return ResponseEntity.status(HttpStatus.BAD_REQUEST).
    				body("{\"status\":\"FAILED\", \"msg\":\"Block creation failed\"}");
    	}
    	return ResponseEntity.ok(formatter.fullObjectToJson(block));
    }
    
    @PostMapping(path = "/queue-clear")
    @ResponseBody
    public ResponseEntity<String> clearQueue(HttpSession session) {
    	if(!validateServerLogin(session)) {
    		return unauthorizedByServer();
    	}
    	manager.clearQueue();
        return ResponseEntity.ok("{\"status\":\"OK\"}");
    }
    
    @PostMapping(path = "/logs-clear")
    @ResponseBody
    public ResponseEntity<String> logsClear(HttpSession session) {
    	if(!validateServerLogin(session)) {
    		return unauthorizedByServer();
    	}
    	logService.clearLogs();
    	return ResponseEntity.ok("{\"status\":\"OK\"}");
    }
    
    
    @PostMapping(path = "/revert-superblock", produces = "text/json;charset=UTF-8")
    @ResponseBody
    public ResponseEntity<String> revertSuperblock(HttpSession session) throws FailedVerificationException {
    	if(!validateServerLogin(session)) {
    		return unauthorizedByServer();
    	}
    	if(!manager.revertSuperblock()) {
    		return ResponseEntity.ok("{\"status\":\"FAILED\", \"msg\":\"Revert super block failed\"}");
    	}
    	return ResponseEntity.ok("{\"status\":\"OK\", \"msg\":\"Blocks are reverted and operations added to the queue.\"}");
    }
    
    @PostMapping(path = "/compact", produces = "text/json;charset=UTF-8")
    @ResponseBody
    public ResponseEntity<String> compact(HttpSession session) throws FailedVerificationException {
    	if(!validateServerLogin(session)) {
    		return unauthorizedByServer();
    	}
    	if(!manager.compact()) {
    		return ResponseEntity.ok("{\"status\":\"FAILED\", \"msg\":\"Compacting blocks failed\"}");
    	}
    	return ResponseEntity.ok("{\"status\":\"OK\", \"msg\":\"Blocks are compacted.\"}");
    }
    
    @PostMapping(path = "/toggle-pause", produces = "text/json;charset=UTF-8")
    @ResponseBody
    public ResponseEntity<String> toggleBlockCreation(HttpSession session) {
    	if(!validateServerLogin(session)) {
    		return unauthorizedByServer();
    	}
    	if(manager.isBlockchainPaused()) {
    		manager.resumeBlockCreation();
    	} else {
    		manager.pauseBlockCreation();
    	}
    	return ResponseEntity.ok("{\"status\":\"OK\"}");
    }
    
    @PostMapping(path = "/bootstrap", produces = "text/html;charset=UTF-8")
    @ResponseBody
    public ResponseEntity<String> bootstrap(HttpSession session) throws Exception {
    	if(!validateServerLogin(session)) {
    		return unauthorizedByServer();
    	}
    	String serverName = getServerUser(session);
    	KeyPair serverLoginKeyPair = getServerLoginKeyPair(session);
    	manager.bootstrap(serverName, serverLoginKeyPair);
		return ResponseEntity.ok("{}");
    }
    
    
}