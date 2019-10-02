package org.openplacereviews.opendb.api;

import org.openplacereviews.opendb.service.BotManager;
import org.openplacereviews.opendb.service.IOpenDBBot;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.TreeMap;

@Controller
@RequestMapping("/api/bot")
public class BotController {

	@Autowired
	private JsonFormatter jsonFormatter;

	@Autowired
	private BotManager botManager;

	@Autowired
	private JsonFormatter formatter;
	
	public static class BotStats {
		public String taskDescription;
		public String taskName;
		public int taskCount;
		public int total;
		public int progress;
		public boolean isRunning;
		public Map<String, Object> settings;
		public String id;
		public String api;

		public BotStats(String id, IOpenDBBot<?> i) {
			this.api = i.getAPI();
			this.id = id;
			this.taskDescription = i.getTaskDescription();
			this.taskName = i.getTaskName();
			this.taskCount = i.taskCount();
			this.total = i.total();
			this.progress = i.progress();
			this.isRunning = i.isRunning();
		}

	}

	@GetMapping(path = "", produces = "text/html;charset=UTF-8")
	@ResponseBody
	public String getBots() {
		Map<String, BotStats> mp = new TreeMap<>();
		Map<String, IOpenDBBot<?>> bots = botManager.getBots();
		for(String k : bots.keySet()) {
			BotStats bs = new BotStats(k, bots.get(k));
			bs.settings = botManager.getBotConfiguration(k).get();
			mp.put(k, bs);
		}
		return jsonFormatter.fullObjectToJson(mp);
	}

	@PostMapping(path = "start", produces = "text/html;charset=UTF-8")
	@ResponseBody
	public String startBot(@RequestParam String botName) {
		if (botManager.startBot(botName)) {
			return formatter.fullObjectToJson(new MgmtController.ResponseUserBody(MgmtController.ResponseStatus.OK));
		} else {
			return formatter.fullObjectToJson(new MgmtController.ResponseUserBody(MgmtController.ResponseStatus.ERROR));
		}
	}
	
	
	@PostMapping(path = "stop", produces = "text/html;charset=UTF-8")
	@ResponseBody
	public String stopBot(@RequestParam String botName) {
		if (botManager.stopBot(botName)) {
			return formatter.fullObjectToJson(new MgmtController.ResponseUserBody(MgmtController.ResponseStatus.OK));
		} else {
			return formatter.fullObjectToJson(new MgmtController.ResponseUserBody(MgmtController.ResponseStatus.ERROR));
		}
	}
	
	@PostMapping(path = "enable", produces = "text/html;charset=UTF-8")
	@ResponseBody
	public String enableBot(@RequestParam String botName, @RequestParam int interval) {
		if (botManager.enableBot(botName, interval)) {
			return formatter.fullObjectToJson(new MgmtController.ResponseUserBody(MgmtController.ResponseStatus.OK));
		} else {
			return formatter.fullObjectToJson(new MgmtController.ResponseUserBody(MgmtController.ResponseStatus.ERROR));
		}
	}
	
	@PostMapping(path = "disable", produces = "text/html;charset=UTF-8")
	@ResponseBody
	public String disableBot(@RequestParam String botName) {
		if (botManager.disableBot(botName)) {
			return formatter.fullObjectToJson(new MgmtController.ResponseUserBody(MgmtController.ResponseStatus.OK));
		} else {
			return formatter.fullObjectToJson(new MgmtController.ResponseUserBody(MgmtController.ResponseStatus.ERROR));
		}
	}


}
