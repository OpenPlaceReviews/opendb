package org.openplacereviews.opendb.api;

import org.openplacereviews.opendb.service.BotManager;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.List;
import java.util.Map;
import java.util.Set;

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
		Set<List<String>> botsNames = botManager.getAllBots();
		if (botsNames.isEmpty()) {
			return "{}";
		}
		return jsonFormatter.fullObjectToJson(botsNames);
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

	@GetMapping(path = "stats", produces = "text/json;charset=UTF-8")
	@ResponseBody
	public String getBotStats() {
		Map botInfo = botManager.getBotStats();
		if (botInfo.isEmpty()) {
			return "{}";
		}
		return jsonFormatter.fullObjectToJson(botInfo);
	}
}
