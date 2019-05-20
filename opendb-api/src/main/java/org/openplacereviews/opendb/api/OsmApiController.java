package org.openplacereviews.opendb.api;

import org.openplacereviews.opendb.util.exception.FailedVerificationException;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping("/api")
public class OsmApiController {

//	@PostMapping(path = "/admin-login")
//	@ResponseBody
//	public ResponseEntity<String> serverLogin(@RequestParam(required = true) String name,
//											  @RequestParam(required = true) String pwd, HttpSession session, HttpServletResponse response) {
//		if(OUtils.equals(manager.getServerUser(), name) &&
//				OUtils.equals(manager.getServerPrivateKey(), pwd)) {
//			session.setAttribute(ADMIN_LOGIN_NAME, name);
//			session.setAttribute(ADMIN_LOGIN_PWD, pwd);
//			session.setMaxInactiveInterval(-1);
//			keyPairs.put(name, manager.getServerLoginKeyPair());
//			return ResponseEntity.ok().body("{\"status\":\"OK\"}");
//		}
//		return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body("{\"status\":\"ERROR\"}");
//	}

    @GetMapping(path = "/test-controller", produces = "text/json;charset=UTF-8")
    @ResponseBody
    public String objects(@RequestParam(required = true) String msg) throws FailedVerificationException {
        return "Test controller " + msg;
    }
}
