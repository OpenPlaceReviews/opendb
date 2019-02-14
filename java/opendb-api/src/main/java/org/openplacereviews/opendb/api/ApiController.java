package org.openplacereviews.opendb.api ;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.core.io.InputStreamResource;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping("/api")
public class ApiController {
	
    protected static final Log LOGGER = LogFactory.getLog(ApiController.class);
    

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
    
}