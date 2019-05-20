package org.openplacereviews.opendb.api;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.ops.*;
import org.openplacereviews.opendb.ops.OpBlockChain.ObjectsSearchRequest;
import org.openplacereviews.opendb.scheduled.OpenDBScheduledServices;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.LogOperationService;
import org.openplacereviews.opendb.service.LogOperationService.LogEntry;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.OUtils;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.*;

@Controller
@RequestMapping("/api")
public class ApiController {

	protected static final Log LOGGER = LogFactory.getLog(ApiController.class);

	@Autowired
	private BlocksManager manager;

	@Autowired
	private JsonFormatter formatter;

	@Autowired
	private LogOperationService logService;

	@Autowired
	private OpenDBScheduledServices scheduledServices;

	@GetMapping(path = "/status", produces = "text/html;charset=UTF-8")
	@ResponseBody
	public String status() {
		BlockchainStatus res = new BlockchainStatus();
		OpBlockChain o = manager.getBlockchain();
		while (!o.isNullBlock()) {
			if (o.getSuperBlockHash().equals("")) {
				res.sblocks.add("Q-" + o.getQueueOperations().size());
			} else {
				String shorten = o.getSuperBlockHash();
				while (shorten.indexOf("00") == 0) {
					shorten = shorten.substring(2);
				}
				shorten = shorten.substring(0, 2) + "-" + shorten.substring(2, 10);
				if (o.isDbAccessed()) {
					shorten = "DB-" + shorten;
				}
				res.sblocks.add(shorten);
			}
			o = o.getParent();
		}
		res.orphanedBlocks = manager.getOrphanedBlocks();
		res.serverUser = manager.getServerUser();
		res.status = manager.getCurrentState();
		if (manager.isBlockCreationOn()) {
			res.status += " (blocks every " + scheduledServices.minSecondsInterval + " seconds)";
		} else if (manager.isReplicateOn()) {
			res.status += String.format(" (replicate from %s every %d seconds)	", manager.getReplicateUrl(),
					scheduledServices.replicateInterval);
		}
		return formatter.fullObjectToJson(res);
	}

	@GetMapping(path = "/admin", produces = "text/html;charset=UTF-8")
	@ResponseBody
	public InputStreamResource testHarness() {
		return new InputStreamResource(ApiController.class.getResourceAsStream("/admin.html"));
	}

	@GetMapping(path = "/queue", produces = "text/json;charset=UTF-8")
	@ResponseBody
	public String queueList() throws FailedVerificationException {
		OpBlock bl = new OpBlock();
		for (OpOperation ob : manager.getBlockchain().getQueueOperations()) {
			bl.addOperation(ob);
		}
		return formatter.fullObjectToJson(bl);
	}

	public static class LogResult {
		public Collection<LogEntry> logs;
	}

	@GetMapping(path = "/logs", produces = "text/json;charset=UTF-8")
	@ResponseBody
	public String logsList() throws FailedVerificationException {
		LogResult r = new LogResult();
		r.logs = logService.getLog();
		return formatter.fullObjectToJson(r);
	}

	protected static class BlockchainStatus {
		public String status;
		public String serverUser;
		public Map<String, OpBlock> orphanedBlocks;
		public List<String> sblocks = new ArrayList<String>();
	}

	protected static class BlocksResult {
		public Collection<OpBlock> blocks;
	}

	protected static class ObjectsResult {
		public Collection<OpObject> objects;
	}

	@GetMapping(path = "/blocks", produces = "text/json;charset=UTF-8")
	@ResponseBody
	public String blocksList(@RequestParam(required = false, defaultValue = "50") int depth,
			@RequestParam(required = false) String from) throws FailedVerificationException {
		List<OpBlock> blocks;
		if (from != null) {
			if (OUtils.isEmpty(from)) {
				blocks = new ArrayList<OpBlock>(manager.getBlockchain().getBlockHeaders(-1));
				Collections.reverse(blocks);
			} else {
				OpBlock found = manager.getBlockchain().getBlockHeaderByRawHash(from);
				if (found != null) {
					depth = manager.getBlockchain().getLastBlockId() - found.getBlockId() + 2; // +1 extra
					blocks = new LinkedList<OpBlock>(manager.getBlockchain().getBlockHeaders(depth));
					Collections.reverse(blocks);
					while (!blocks.isEmpty() && !OUtils.equals(blocks.get(0).getRawHash(), from)) {
						blocks.remove(0);
					}
				} else {
					blocks = new ArrayList<OpBlock>(manager.getBlockchain().getBlockHeaders(3));
				}
			}
		} else {
			blocks = manager.getBlockchain().getBlockHeaders(depth);
		}
		return formatter.fullObjectToJson(blocks.toArray(new OpBlock[blocks.size()]));
	}

	@GetMapping(path = "/block-by-hash", produces = "text/json;charset=UTF-8")
	@ResponseBody
	public String getBlockByHash(@RequestParam(required = true) String hash) {
		OpBlock blockHeader = manager.getBlockchain().getFullBlockByRawHash(OpBlockchainRules.getRawHash(hash));
		if (blockHeader == null) {
			return "{}";
		}
		return formatter.fullObjectToJson(blockHeader);
	}
	
	@GetMapping(path = "/block-header-by-id", produces = "text/json;charset=UTF-8")
	@ResponseBody
	public String getBlockHeaderById(@RequestParam(required = true) int blockId) {
		OpBlock blockHeader = manager.getBlockchain().getBlockHeadersById(blockId);
		if (blockHeader == null) {
			return "{}";
		}
		return formatter.fullObjectToJson(blockHeader);
	}

	@GetMapping(path = "/op-by-hash", produces = "text/json;charset=UTF-8")
	@ResponseBody
	public String getOperationByHash(@RequestParam(required = true) String hash) {
		OpOperation op = manager.getBlockchain().getOperationByHash(OpBlockchainRules.getRawHash(hash));
		if (op == null) {
			return "{}";
		}
		return formatter.fullObjectToJson(op);
	}

	@GetMapping(path = "/objects", produces = "text/json;charset=UTF-8")
	@ResponseBody
	public String objects(@RequestParam(required = true) String type,
			@RequestParam(required = false, defaultValue = "100") int limit) throws FailedVerificationException {
		OpBlockChain blc = manager.getBlockchain();
		ObjectsResult res = new ObjectsResult();
		ObjectsSearchRequest r = new ObjectsSearchRequest();
		r.limit = limit;
		blc.getObjects(type, r);
		res.objects = r.result;
		return formatter.fullObjectToJson(res);
	}

	@GetMapping(path = "/object-by-id", produces = "text/json;charset=UTF-8")
	@ResponseBody
	public String objects(@RequestParam(required = true) String type, @RequestParam(required = false) String key,
			@RequestParam(required = false) String key2) throws FailedVerificationException {
		OpBlockChain blc = manager.getBlockchain();
		OpObject obj;
		if (OUtils.isEmpty(key2)) {
			obj = blc.getObjectByName(type, key);
		} else {
			obj = blc.getObjectByName(type, key, key2);
		}
		return formatter.fullObjectToJson(obj);
	}

}