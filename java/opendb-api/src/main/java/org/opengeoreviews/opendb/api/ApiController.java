package org.opengeoreviews.opendb.api ;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.opengeoreviews.opendb.ops.OpBlock;
import org.opengeoreviews.opendb.ops.OpDefinitionBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.jdbc.core.JdbcTemplate;
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
    private JdbcTemplate jdbcTemplate;
    
    @Autowired
    private BlocksManager manager;
    
    @Autowired
    private OperationsQueue queue;
    
    @Autowired
    private BlocksFormatting formatter;


    @GetMapping(path = "/status", produces = "text/html;charset=UTF-8")
    @ResponseBody
    public String status() {
        return "OK";
    }
    
    
    @PostMapping(path = "/queue/add")
    @ResponseBody
    public String addToQueue(@RequestParam(required = true) String id) {
    	OpBlock block = formatter.parseBootstrapBlock(id);
    	queue.addOperations(block.getOperations());
        return "OK";
    }
    
    @PostMapping(path = "/queue/clear")
    @ResponseBody
    public String addToQueue() {
    	queue.clearOperations();
        return "OK";
    }
    
    @GetMapping(path = "/queue/list", produces = "text/json;charset=UTF-8")
    @ResponseBody
    public String queueList() {
    	OpBlock bl = new OpBlock();
    	for(OpDefinitionBean ob : queue.getOperationsQueue()) {
//    		formatter.calculateOperationHash(ob, false);
    		bl.getOperations().add(ob);	
    	}
    	return formatter.toJson(bl);
    }
    
    
    @PostMapping(path = "/block/create", produces = "text/json;charset=UTF-8")
    @ResponseBody
    public String createBlock() {
    	return manager.createBlock();
    }
    
    
    @GetMapping(path = "/block/content", produces = "text/json;charset=UTF-8")
    @ResponseBody
    public InputStreamResource block(@RequestParam(required = true) String id) {
        return new InputStreamResource(formatter.getBlock(id));
    }
    
    
    @GetMapping(path = "/test", produces = "text/html;charset=UTF-8")
    @ResponseBody
    public InputStreamResource testHarness() {
        return new InputStreamResource(ApiController.class.getResourceAsStream("/test.html"));
    }
    
    @PostMapping(path = "/block/bootstrap", produces = "text/html;charset=UTF-8")
    @ResponseBody
    public String bootstrap() {
    	OpBlock block = formatter.parseBootstrapBlock("1");
    	manager.replicateBlock(block);
        return "OK";
    }
}