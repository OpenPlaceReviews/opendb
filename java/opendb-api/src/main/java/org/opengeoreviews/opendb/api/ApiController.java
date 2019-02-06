package org.opengeoreviews.opendb.api ;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.opengeoreviews.opendb.SecUtils;
import org.opengeoreviews.opendb.ops.OpBlock;
import org.opengeoreviews.opendb.ops.OpDefinitionBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.gson.Gson;

@Controller
@RequestMapping("/api")
public class ApiController {
	
    protected static final Log LOGGER = LogFactory.getLog(ApiController.class);
    
    @Autowired
    private JdbcTemplate jdbcTemplate;
    
    @Autowired
    private OperationsManager manager;
    
    @Autowired
    private OperationsQueue queue;


    @GetMapping(path = "/status", produces = "text/html;charset=UTF-8")
    @ResponseBody
    public String status() {
        return "OK";
    }
    
    
    @GetMapping(path = "/queue/add")
    @ResponseBody
    public String addToQueue(@RequestParam(required = true) String id) {
    	OpBlock block = manager.parseBootstrapBlock(id);
    	queue.addOperations(block.getOperations());
        return "OK";
    }
    
    @GetMapping(path = "/queue/clear")
    @ResponseBody
    public String addToQueue() {
    	queue.clearOperations();
        return "OK";
    }
    
    @GetMapping(path = "/queue/list", produces = "text/json;charset=UTF-8")
    @ResponseBody
    public String queueList() {
    	OpBlock bl = new OpBlock();
    	Gson gson = manager.getGson();
    	for(OpDefinitionBean ob : queue.getOperationsQueue()) {
    		ob.remove(OpDefinitionBean.F_HASH);
    		String hash = SecUtils.calculateSha1(gson.toJson(ob));
    		ob.putStringValue(OpDefinitionBean.F_HASH, hash);
    		bl.getOperations().add(ob);	
    	}
    	
    	return gson.toJson(bl);
    }
    
    
    @GetMapping(path = "/block/content", produces = "text/json;charset=UTF-8")
    @ResponseBody
    public InputStreamResource block(@RequestParam(required = true) String id) {
        return new InputStreamResource(manager.getBlock(id));
    }
    
    @GetMapping(path = "/block/bootstrap", produces = "text/html;charset=UTF-8")
    @ResponseBody
    public String bootstrap() {
    	OpBlock block = manager.parseBootstrapBlock("1");
    	manager.executeBlock(block);
        return "OK";
    }
}