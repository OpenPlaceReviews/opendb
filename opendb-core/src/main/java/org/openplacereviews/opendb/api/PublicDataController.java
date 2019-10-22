package org.openplacereviews.opendb.api;

import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.openplacereviews.opendb.service.PublicDataManager;
import org.openplacereviews.opendb.service.PublicDataManager.PublicAPIEndpoint;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.HandlerMapping;

@Controller
@RequestMapping("/api/public")
public class PublicDataController {


	@Autowired
	private PublicDataManager dataManager;

	@GetMapping(path = "/**")
	@ResponseBody
	public ResponseEntity<?> processData(HttpServletRequest request) {
		String path = (String) request.getAttribute(HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE);
		String fpath = path.substring("/api/public/".length());
		Map<String, String[]> params = request.getParameterMap();
		// alternative method
//		String bestMatchPattern = (String) request.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE);
//		AntPathMatcher apm = new AntPathMatcher();
//		String finalPath = apm.extractPathWithinPattern(bestMatchPattern, path);
		int i1 = fpath.indexOf("/");
		String id = fpath;
		String suffix = "";
		if(i1 != -1) {
			id = fpath.substring(0, i1);
			suffix = fpath.substring(i1 + 1);
		}
		
		PublicAPIEndpoint apiEndpoint = dataManager.getEndpoint(id);
		if(apiEndpoint != null) {
			if(suffix.equals("index")) {
				return ResponseEntity.ok(apiEndpoint.getPage(params));
			}
			return ResponseEntity.ok(apiEndpoint.getContent(params));
		}
		return ResponseEntity.notFound().build();
	}

}
