package org.openplacereviews.opendb.api;

import javax.servlet.http.HttpServletRequest;

import org.openplacereviews.opendb.service.PublicDataManager;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.HandlerMapping;

@Controller
@RequestMapping("/data")
public class PublicDataController {

	@Autowired
	private JsonFormatter jsonFormatter;

	@Autowired
	private PublicDataManager dataManager;

	@GetMapping(path = "/**")
	@ResponseBody
	public ResponseEntity<?> processData(HttpServletRequest request) {
		String path = (String) request.getAttribute(HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE);
		String fpath = path.substring("/data/".length());
		// alternative method
//		String bestMatchPattern = (String) request.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE);
//		AntPathMatcher apm = new AntPathMatcher();
//		String finalPath = apm.extractPathWithinPattern(bestMatchPattern, path);
		String s = "OK: " + fpath;
		return ResponseEntity.ok().body(s);
	}

}
