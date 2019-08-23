package org.openplacereviews.opendb.api;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.service.HistoryManager;
import org.openplacereviews.opendb.service.HistoryManager.HistoryObjectRequest;
import org.openplacereviews.opendb.ops.*;
import org.openplacereviews.opendb.ops.OpBlockChain.ObjectsSearchRequest;
import org.openplacereviews.opendb.ops.PerformanceMetrics.PerformanceMetric;
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
	private HistoryManager historyManager;

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
				String hash = o.getSuperBlockHash();
				String sz = hash.substring(0, 8);
				hash = hash.substring(8, 16);
				while (sz.indexOf("00") == 0) {
					sz = sz.substring(2);
				}
				if (o.isDbAccessed()) {
					sz = "DB-" + sz;
				}
				res.sblocks.add(sz + "-"+hash);
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
	
	protected static class MetricResult {
		public String id;
		public int count;
		public int totalSec;
		public int avgMs;
		
	}
	
	protected static class MetricsResult {
		public List<MetricResult> metrics = new ArrayList<ApiController.MetricResult>();
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

	@GetMapping(path = "/metrics", produces = "text/json;charset=UTF-8")
	@ResponseBody
	public String metrics() throws FailedVerificationException {
		MetricsResult ms = new MetricsResult();
		PerformanceMetrics inst = PerformanceMetrics.i();
		for(PerformanceMetric p : new TreeMap<>(inst.getMetrics()).values()) {
			MetricResult r = new MetricResult();
			r.count = p.getInvocations();
			r.id = p.getName();
			r.totalSec = (int) (p.getDuration() / 1e9);
			if(r.count > 0) {
				r.avgMs = (int) (p.getDuration() / 1e6 / r.count);
			}
			ms.metrics.add(r);
		}
		return formatter.fullObjectToJson(ms);
	}
	
	@GetMapping(path = "/objects", produces = "text/json;charset=UTF-8")
	@ResponseBody
	public String objects(@RequestParam(required = true) String type,
			@RequestParam(required = false, defaultValue = "100") int limit) throws FailedVerificationException {
		OpBlockChain blc = manager.getBlockchain();
		ObjectsResult res = new ObjectsResult();
		ObjectsSearchRequest r = new ObjectsSearchRequest();
		r.limit = limit;
		blc.fetchAllObjects(type, r);
		res.objects = r.result;
		return formatter.fullObjectToJson(res);
	}

	@GetMapping(path = "/object-by-id", produces = "text/json;charset=UTF-8")
	@ResponseBody
	public String objects(@RequestParam(required = true) String type, @RequestParam(required = true) String key) throws FailedVerificationException {
		OpBlockChain blc = manager.getBlockchain();
		OpObject obj;
		if (!key.contains(",")) {
			obj = blc.getObjectByName(type, key);
		} else {
			String[] keys = key.split(",");
			obj = blc.getObjectByName(type, keys[0].trim(), keys[1].trim());
		}
		return formatter.fullObjectToJson(obj);
	}

	@GetMapping(path = "/indices-by-type", produces = "text/json;charset=UTF-8")
	@ResponseBody
	public String getIndices(@RequestParam(required = false) String type) {
		Collection<OpIndexColumn> tableIndices = manager.getIndicesForType(type);
		return formatter.fullObjectToJson(tableIndices);
	}

	@GetMapping(path = "/indices", produces = "text/json;charset=UTF-8")
	@ResponseBody
	public String objectsByIndex(@RequestParam(required = true) String type,
								 @RequestParam(required = true) String index,
								 @RequestParam(required = true) String key) {
		OpBlockChain.ObjectsSearchRequest req = new OpBlockChain.ObjectsSearchRequest();
		return formatter.fullObjectToJson(manager.getObjectsByIndex(type, index, req, key));
	}

	@GetMapping(path = "/history", produces = "text/json;charset=UTF-8")
	@ResponseBody
	public String history(@RequestParam(required = true) String type,
						  @RequestParam(required = true) List<String> key,
						  @RequestParam(required = false, defaultValue = "20") int limit,
						  @RequestParam(required = true) String sort) {
		if (key.isEmpty() || !historyManager.isRunning()) {
			return "{}";
		}
		HistoryObjectRequest historyObjectRequest = new HistoryObjectRequest(type, key, limit, sort);
		historyManager.retrieveHistory(historyObjectRequest);

		return formatter.fullObjectToJson(historyObjectRequest.historySearchResult);
	}

}