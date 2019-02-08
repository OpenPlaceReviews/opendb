package org.openplacereviews.opendb.api ;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.service.OpenDBValidator;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.OperationsQueue;
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
    private OperationsQueue queue;
    
    @Autowired
    private OpenDBValidator formatter;

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
    public String bootstrap() {
    	OpBlock block = formatter.parseBootstrapBlock("1");
    	manager.replicateBlock(block);
        return "OK";
    }
}