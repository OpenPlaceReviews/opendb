package org.openplacereviews.opendb.api;

import org.openplacereviews.opendb.service.BotManager;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping("/api/bot")
public class BotController {

	@Autowired
	private JsonFormatter jsonFormatter;

	@Autowired
	private BotManager botManager;

	@GetMapping(path = "", produces = "text/html;charset=UTF-8")
	@ResponseBody
	public String getBots() {
		return jsonFormatter.fullObjectToJson(botManager.getBots().values());
	}

	@GetMapping(path = "start", produces = "text/html;charset=UTF-8")
	@ResponseBody
	public String startBot(@RequestParam String botName) {
		if (botManager.startBot(botName)) {
			return "{\"status\": \"OK\"}";
		} else {
			return "{\"status\": \"ERROR\"}";
		}
	}
	
	@GetMapping(path = "stop", produces = "text/html;charset=UTF-8")
	@ResponseBody
	public String stopBot(@RequestParam String botName) {
		if (botManager.stopBot(botName)) {
			return "{\"status\": \"OK\"}";
		} else {
			return "{\"status\": \"ERROR\"}";
		}
	}

	@GetMapping(path = "stats", produces = "text/json;charset=UTF-8")
	@ResponseBody
	public String getBotStats() {
		// TODO stats formatting
		return jsonFormatter.fullObjectToJson(botManager.getBots());
	}
}
