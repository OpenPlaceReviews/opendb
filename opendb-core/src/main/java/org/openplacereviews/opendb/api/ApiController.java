package org.openplacereviews.opendb.api;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.ops.*;
import org.openplacereviews.opendb.ops.OpBlockChain.ObjectsSearchRequest;
import org.openplacereviews.opendb.ops.PerformanceMetrics.PerformanceMetric;
import org.openplacereviews.opendb.scheduled.OpenDBScheduledServices;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.BlocksManager.BlocksListResult;
import org.openplacereviews.opendb.service.HistoryManager;
import org.openplacereviews.opendb.service.HistoryManager.HistoryObjectRequest;
import org.openplacereviews.opendb.service.LogOperationService;
import org.openplacereviews.opendb.service.LogOperationService.LogEntry;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.OUtils;
import org.openplacereviews.opendb.util.exception.FailedVerificationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpSession;
import java.util.*;

@Controller
@RequestMapping("/api")
public class ApiController {

	protected static final Log LOGGER = LogFactory.getLog(ApiController.class);
	
	public static final int LIMIT_RESULTS = 10000;

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
	public String status(HttpSession session) {
		BlockchainStatus res = new BlockchainStatus();
		OpBlockChain o = manager.getBlockchain();
		while (!o.isNullBlock()) {
			if (o.getSuperBlockHash().equals("")) {
				res.sblocks.add("Q-" + o.getQueueOperations().size());
			} else {
				String hash = o.getSuperBlockHash();
				String sz = hash.substring(0, 8);
				hash = hash.substring(8);
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
		res.amountBlocks = manager.getBlockchain().getDepth();
		res.orphanedBlocks = manager.getOrphanedBlocks();
		res.serverUser = manager.getServerUser();
		res.status = manager.getCurrentState();
		res.statusDescription = manager.getCurrentStateDescription();
		res.loginUser = OpApiController.getServerUser(session);
		if (manager.isBlockCreationOn()) {
			res.status += " (blocks every " + scheduledServices.getMinSecondsInterval() + " seconds)";
		} else if (manager.isReplicateOn()) {
			res.status += String.format(" (replicate from %s every %d seconds)	", manager.getReplicateUrl(),
					scheduledServices.getReplicateInterval());
		} else {
			res.status += " (no blocks)";
		}
		return formatter.fullObjectToJson(res);
	}

	@GetMapping(path = "/admin", produces = "text/html;charset=UTF-8")
	@ResponseBody
	public InputStreamResource testHarness() {
		return new InputStreamResource(ApiController.class.getResourceAsStream("/public/admin.html"));
	}

	@GetMapping(path = "/queue", produces = "text/json;charset=UTF-8")
	@ResponseBody
	public String queueList() throws FailedVerificationException {
		OpBlock bl = new OpBlock();
		for(Iterator<OpOperation> descItr = manager.getBlockchain().getQueueOperations().descendingIterator();descItr.hasNext();) {
			bl.addOperation(descItr.next());
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
		public String statusDescription;
		public String serverUser;
		public String loginUser;
		public Integer amountBlocks;
		public Map<String, OpBlock> orphanedBlocks;
		public List<String> sblocks = new ArrayList<String>();
	}

	

	protected static class ObjectsResult {
		public Collection<OpObject> objects;
		public int count;
	}
	
	protected static class MetricResult {
		public String id;
		public int[] count;
		public int[] totalSec;
		public int[] avgMs;
		
	}
	
	protected static class MetricsResult {
		public List<MetricResult> metrics = new ArrayList<>();
	}

	@GetMapping(path = "/blocks", produces = "text/json;charset=UTF-8")
	@ResponseBody
	public String blocksList(@RequestParam(required = false, defaultValue = "100") int depth,
			@RequestParam(required = false) String from, @RequestParam(required = false) String to) throws FailedVerificationException {
		BlocksListResult br = new BlocksListResult();
		OpBlockChain blc = manager.getBlockchain();
		br.blockDepth = blc.getDepth();
		if (from != null) {
			// used by replication
			if (OUtils.isEmpty(from)) {
				br.blocks = new LinkedList<OpBlock>(blc.getBlockHeaders(-1));
				Collections.reverse(br.blocks);
			} else {
				OpBlock found = blc.getBlockHeaderByRawHash(from);
				if (found != null) {
					depth = blc.getLastBlockId() - found.getBlockId() + 2; // +1 extra
					br.blocks = new LinkedList<OpBlock>(blc.getBlockHeaders(depth));
					Collections.reverse(br.blocks);
					while (!br.blocks.isEmpty() && !OUtils.equals(br.blocks.get(0).getRawHash(), from)) {
						br.blocks.remove(0);
					}
				}
			}
		} else if(!OUtils.isEmpty(to)) {
			OpBlock found = blc.getBlockHeaderByRawHash(to);
			if (found != null) {
				int ldepth = depth + (blc.getLastBlockId() - found.getBlockId()) ;
				br.blocks = new LinkedList<OpBlock>(blc.getBlockHeaders(ldepth));
				while (!br.blocks.isEmpty() && !OUtils.equals(br.blocks.get(0).getRawHash(), to)) {
					br.blocks.remove(0);
				}
				while(br.blocks.size() > depth) {
					br.blocks.removeLast();
				}
			}
		} else {
			br.blocks = new LinkedList<OpBlock>(blc.getBlockHeaders(depth));
			while(br.blocks.size() > depth) {
				br.blocks.removeLast();
			}
		}
		
		return formatter.fullObjectToJson(br);
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
		return formatter.fullObjectToJson(Collections.singletonList(op));
	}

	@GetMapping(path = "/ops-by-id", produces = "text/json;charset=UTF-8")
	@ResponseBody
	public String getOperationByObjectId(@RequestParam(required = true) String id) {
		List<String> keys = Arrays.asList(id.split(","));
		OpBlock opBlock = new OpBlock();
		for (String k : keys) {
			OpOperation op = manager.getBlockchain().getOperationByHash(k);
			opBlock.addOperation(op);
		}
		return formatter.fullObjectToJson(opBlock);
	}

	@GetMapping(path = "/ops-by-block-id",  produces = "text/json;charset=UTF-8")
	@ResponseBody
	public String getOperationsByBlockId(@RequestParam(required = true) int blockId) {
		OpBlock opBlock = manager.getBlockchain().getFullBlockByBlockId(blockId);
		if (opBlock != null) {
			OpBlock bl = new OpBlock();
			for (OpOperation ob : opBlock.getOperations()) {
				bl.addOperation(ob);
			}
			return formatter.fullObjectToJson(bl);
		} else {
			return "{}";
		}
	}

	@GetMapping(path = "/ops-by-block-hash",  produces = "text/json;charset=UTF-8")
	@ResponseBody
	public String getOperationsByBlockHash(@RequestParam(required = true) String hash) {
		OpBlock opBlock = manager.getBlockchain().getFullBlockByRawHash(hash);
		if (opBlock != null) {
			OpBlock bl = new OpBlock();
			for (OpOperation ob : opBlock.getOperations()) {
				bl.addOperation(ob);
			}
			return formatter.fullObjectToJson(bl);
		}

		return "{}";
	}
	
	
	@PostMapping(path = "/metrics-reset", produces = "text/json;charset=UTF-8")
	@ResponseBody
	public String metricsReset(@RequestParam(required = true) int cnt) throws FailedVerificationException {
		PerformanceMetrics.i().reset(cnt);
		return metrics();
	}

	@GetMapping(path = "/metrics", produces = "text/json;charset=UTF-8")
	@ResponseBody
	public String metrics() throws FailedVerificationException {
		MetricsResult ms = new MetricsResult();
		PerformanceMetrics inst = PerformanceMetrics.i();
		TreeMap<String, PerformanceMetric> mp = new TreeMap<>(inst.getMetrics());
		int l = PerformanceMetrics.METRICS_COUNT + 1;
		for (PerformanceMetric p : mp.values()) {
			MetricResult r = new MetricResult();
			r.id = p.getName();
			r.count = new int[l];
			r.totalSec = new int[l];
			r.avgMs = new int[l];
			for (int i = 0; i < l; i++) {
				r.count[i] = p.getInvocations(i);
				r.totalSec[i] = (int) (p.getDuration(i) / 1e9);
				if (r.count[i] > 0) {
					r.avgMs[i] = (int) (p.getDuration(i) / 1e6 / r.count[i]);
				}
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
		if(limit < 0 || limit > LIMIT_RESULTS) {
			limit = LIMIT_RESULTS;
		}
		r.limit = limit;
		blc.fetchAllObjects(type, r);
		res.objects = r.result;
		return formatter.fullObjectToJson(res);
	}
	
	@GetMapping(path = "/objects-count", produces = "text/json;charset=UTF-8")
	@ResponseBody
	public String objects(@RequestParam(required = true) String type) throws FailedVerificationException {
		OpBlockChain blc = manager.getBlockchain();
		ObjectsResult o = new ObjectsResult();
		o.count = blc.countAllObjects(type);
		return formatter.fullObjectToJson(o);
	}

	@GetMapping(path = "/objects-by-id", produces = "text/json;charset=UTF-8")
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
		ObjectsResult res = new ObjectsResult();
		res.objects = obj == null ? Collections.emptyList() : Collections.singletonList(obj);
		return formatter.fullObjectToJson(res);
	}

	@GetMapping(path = "/indices-by-type", produces = "text/json;charset=UTF-8")
	@ResponseBody
	public String getIndices(@RequestParam(required = false) String type) {
		Collection<OpIndexColumn> tableIndices = manager.getIndicesForType(type);
		return formatter.fullObjectToJson(tableIndices);
	}

	@GetMapping(path = "/objects-by-index", produces = "text/json;charset=UTF-8")
	@ResponseBody
	public String objectsByIndex(@RequestParam(required = true) String type,
								 @RequestParam(required = true) String index,
								 @RequestParam(required = true) String key,
								 @RequestParam(required = false, defaultValue = "100") int limit) {
		OpBlockChain.ObjectsSearchRequest req = new OpBlockChain.ObjectsSearchRequest();
		if(limit < 0 || limit > LIMIT_RESULTS) {
			limit = LIMIT_RESULTS;
		}
		req.limit = limit;
		ObjectsResult r = new ObjectsResult();
		OpIndexColumn indexCol = manager.getIndex(type, index);
		if (indexCol != null) {
			manager.getBlockchain().fetchObjectsByIndex(type, indexCol, req, key);
			r.objects = req.result;
		} else {
			throw new UnsupportedOperationException();
		}
		return formatter.fullObjectToJson(r);
	}

	@GetMapping(path = "/history", produces = "text/json;charset=UTF-8")
	@ResponseBody
	public String history(@RequestParam(required = true) String type,
						  @RequestParam(required = false) List<String> key,
						  @RequestParam(required = false, defaultValue = "100") int limit,
						  @RequestParam(required = true) String sort) {
		if (!historyManager.isRunning()) {
			return "{}";
		}
		if(limit < 0 || limit > LIMIT_RESULTS) {
			limit = LIMIT_RESULTS;
		}
		HistoryObjectRequest historyObjectRequest = new HistoryObjectRequest(type, key, limit, sort);
		historyManager.retrieveHistory(historyObjectRequest);

		return formatter.fullObjectToJson(historyObjectRequest.historySearchResult);
	}

	@GetMapping(path = "/index",  produces = "text/json;charset=UTF-8")
	@ResponseBody
	public String getIndexInfo() {
		return formatter.fullObjectToJson(manager.getIndices());
	}

}