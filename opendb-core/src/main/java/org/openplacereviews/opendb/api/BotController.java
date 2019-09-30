package org.openplacereviews.opendb.api;

import java.util.Map;
import java.util.TreeMap;

import org.openplacereviews.opendb.service.BotManager;
import org.openplacereviews.opendb.service.IOpenDBBot;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
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
	
	public static class BotStats {
		String taskDescription;
		String taskName;
		int taskCount;
		int total;
		int progress;
		boolean isRunning;

		public BotStats(IOpenDBBot<?> i) {
			this.taskDescription = i.getTaskDescription();
			this.taskName = i.getTaskName();
			this.taskCount = i.taskCount();
			this.total = i.total();
			this.progress = i.progress();
			this.isRunning = i.isRunning();
		}

		public String getTaskDescription() {
			return taskDescription;
		}

		public String getTaskName() {
			return taskName;
		}

		public int getTaskCount() {
			return taskCount;
		}

		public int getTotal() {
			return total;
		}

		public int getProgress() {
			return progress;
		}

		public boolean isRunning() {
			return isRunning;
		}
	}

	@GetMapping(path = "", produces = "text/html;charset=UTF-8")
	@ResponseBody
	public String getBots() {
		Map<String, BotStats> mp = new TreeMap<>();
		Map<String, IOpenDBBot<?>> bs = botManager.getBots();
		for(String k : bs.keySet()) {
			mp.put(k, new BotStats(bs.get(k)));
		}
		return jsonFormatter.fullObjectToJson(mp);
	}

	@PostMapping(path = "start", produces = "text/html;charset=UTF-8")
	@ResponseBody
	public String startBot(@RequestParam String botName) {
		if (botManager.startBot(botName)) {
			return "{\"status\": \"OK\"}";
		} else {
			return "{\"status\": \"ERROR\"}";
		}
	}
	
	@PostMapping(path = "stop", produces = "text/html;charset=UTF-8")
	@ResponseBody
	public String stopBot(@RequestParam String botName) {
		if (botManager.stopBot(botName)) {
			return "{\"status\": \"OK\"}";
		} else {
			return "{\"status\": \"ERROR\"}";
		}
	}


}
