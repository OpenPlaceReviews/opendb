package org.opengeoreviews.opendb.api ;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

    @GetMapping(path = "/status", produces = "text/html;charset=UTF-8")
    @ResponseBody
    public String status() {
        return "OK";
    }
    
    
    @GetMapping(path = "/block-content", produces = "text/json;charset=UTF-8")
    @ResponseBody
    public InputStreamResource block(@RequestParam(required = true) String id) {
        return new InputStreamResource(getBlock(id));
    }
    
    
    private InputStream getBlock(String id) {
    	return ApiController.class.getResourceAsStream("/bootstrap/ogr-"+id+".json");
    }
    
    @GetMapping(path = "/bootstrap", produces = "text/html;charset=UTF-8")
    @ResponseBody
    public String bootstrap() {
    	InputStream cont = getBlock("1");
    	Map mp = new Gson(). fromJson(new InputStreamReader(cont), Map.class);
    	List operations = (List) mp.get("operations");
    	for(Object opObj : operations) {
    		Map op  = (Map) opObj;
    		if("create_table".equals(op.get("query"))) {
    			String nm = op.get("name").toString();
    			String columns = "";
    			Set<Map.Entry> entrySet = ((Map) op.get("columns")).entrySet();
    			for(Map.Entry e  : entrySet) {
    				if(columns.length() > 0) {
    					columns += ", ";
    				}
    				columns += e.getKey() + " " + e.getValue();
    			}
    			String ddl = String.format("create table %s (%s)", nm, columns);
    			LOGGER.info(ddl);
    			jdbcTemplate.execute(ddl);
    		}
    	}
    	
        return mp.toString();
    }
}