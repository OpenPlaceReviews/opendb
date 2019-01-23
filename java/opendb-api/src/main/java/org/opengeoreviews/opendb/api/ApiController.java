package org.opengeoreviews.opendb.api ;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.opengeoreviews.opendb.ops.OpBlock;
import org.opengeoreviews.opendb.ops.OperationsManager;
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

    @GetMapping(path = "/status", produces = "text/html;charset=UTF-8")
    @ResponseBody
    public String status() {
        return "OK";
    }
    
    
    @GetMapping(path = "/block-content", produces = "text/json;charset=UTF-8")
    @ResponseBody
    public InputStreamResource block(@RequestParam(required = true) String id) {
        return new InputStreamResource(manager.getBlock(id));
    }
    
    
    @GetMapping(path = "/bootstrap", produces = "text/html;charset=UTF-8")
    @ResponseBody
    public String bootstrap() {
    	OpBlock block = manager.parseBootstrapBlock("1");
    	manager.executeBlock(block);
        return "OK";
    }
}