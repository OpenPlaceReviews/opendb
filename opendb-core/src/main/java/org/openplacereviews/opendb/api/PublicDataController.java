package org.openplacereviews.opendb.api;

import org.openplacereviews.opendb.service.PublicDataManager;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping("/data")
public class PublicDataController {

	@Autowired
	private JsonFormatter jsonFormatter;

	@Autowired
	private PublicDataManager dataManager;
	
	

	@GetMapping(path = "/{request}")
	@ResponseBody
	public ResponseEntity<?> processData(@PathVariable("request") String request) {
		String s = "OK - " + request;
		return ResponseEntity.ok().body(s);
	}


}
