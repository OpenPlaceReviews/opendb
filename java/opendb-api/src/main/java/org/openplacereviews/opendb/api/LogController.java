package org.openplacereviews.opendb.api ;

import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.FailedVerificationException;
import org.openplacereviews.opendb.service.LogOperationService;
import org.openplacereviews.opendb.service.LogOperationService.LogEntry;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping("/log")
public class LogController {
	
    protected static final Log LOGGER = LogFactory.getLog(LogController.class);
    
    @Autowired
    private LogOperationService logService;
    
    @Autowired
    private JsonFormatter formatter;

    @PostMapping(path = "/clear")
    @ResponseBody
    public String addToQueue() {
    	logService.clearLogs();
        return "{\"status\":\"OK\"}";
    }
    
    public static class LogResult {

		public Collection<LogEntry> logs;
    	
    }
    
    @GetMapping(path = "/list", produces = "text/json;charset=UTF-8")
    @ResponseBody
	public String queueList() throws FailedVerificationException {
    	LogResult r = new LogResult();
    	r.logs = logService.getLog();
		return formatter.objectToJson(r);
	}
    
}