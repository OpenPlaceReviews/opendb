package org.openplacereviews.opendb.api;

import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.dto.IpfsStatusDTO;
import org.openplacereviews.opendb.dto.ResourceDTO;
import org.openplacereviews.opendb.service.IPFSFileManager;
import org.openplacereviews.opendb.service.IPFSService;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.exception.ConnectionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.List;

import static org.springframework.http.MediaType.MULTIPART_FORM_DATA_VALUE;

@Controller
@RequestMapping("/api/ipfs")
public class IPFSController {

	protected static final Log LOGGER = LogFactory.getLog(IPFSController.class);

	@Autowired
	private IPFSFileManager externalResourcesManager;

	@Autowired
	private JsonFormatter formatter;
	
	private void checkIPFSRunning() {
		if(!externalResourcesManager.isRunning()) {
			throw new ConnectionException("IPFS service is not running.");
		}
	}

	@PostMapping(value = "/image", consumes = MULTIPART_FORM_DATA_VALUE, produces = "text/html;charset=UTF-8")
	@ResponseBody
	public ResponseEntity<String> uploadImage(
			@RequestPart(name = "file") @Valid @NotNull @NotEmpty MultipartFile file) throws IOException {
		checkIPFSRunning();
		// TODO add to file system and to ipfs
		ResourceDTO imageDTO = ResourceDTO.of(file);
		imageDTO = externalResourcesManager.addFile(imageDTO);

		return ResponseEntity.ok(formatter.imageObjectToJson(imageDTO));
	}

	@ResponseBody
	@GetMapping("status")
	public ResponseEntity<String> loadIpfsStatus() throws IOException, UnirestException {
		// TODO return json with all information
		// TODO add parameter full and display all counts of pending, in blockchain objects etc
		IpfsStatusDTO ipfsStatusDTO ;
		if(!externalResourcesManager.isRunning()) {
			ipfsStatusDTO = new IpfsStatusDTO().setStatus("NOT CONNECTED");
		} else {
			ipfsStatusDTO = externalResourcesManager.getIpfsNodeInfo(); 
		}
		return ResponseEntity.ok(formatter.fullObjectToJson(ipfsStatusDTO));
	}
	

	@GetMapping(value = "/image")
	@ResponseBody
	public ResponseEntity<String> getFile(@RequestParam("cid") String cid) throws IOException {
		checkIPFSRunning();
		try (ByteArrayOutputStream outputStream = (ByteArrayOutputStream) externalResourcesManager.read(cid)) {
			return ResponseEntity.ok(Base64.getEncoder().encodeToString(outputStream.toByteArray()));
		}
	}

	@PutMapping(value = "/image")
	@ResponseBody
	public ResponseEntity<String> pinImage(@RequestParam("cid") String cid) {
		checkIPFSRunning();
		if (!externalResourcesManager.pin(cid)) {
			return ResponseEntity.ok("{\"status\":\"FAILED\"}");
		}

		return ResponseEntity.ok("{\"status\":\"OK\"}");
	}
	

	@DeleteMapping(value = "/image")
	@ResponseBody
	public ResponseEntity<String> unpinImage(@RequestParam("cid") String cid) {
		checkIPFSRunning();
		if(!externalResourcesManager.unpin(cid)) {
			return ResponseEntity.ok("{\"status\":\"FAILED\"}");
		}

		return ResponseEntity.ok("{\"status\":\"OK\"}");
	}

	@GetMapping(value = "/image/tracked")
	@ResponseBody
	public ResponseEntity<String> getTrackedFiles() {
		checkIPFSRunning();
		List<String> tracked = externalResourcesManager.getPinnedResources();
		return ResponseEntity.ok(formatter.fullObjectToJson(tracked));
	}

	@GetMapping(value = "/status")
	@ResponseBody
	public ResponseEntity<String> getStatusCheckingMissingImagesInIPFS() {
		checkIPFSRunning();
		return ResponseEntity.ok(formatter.fullObjectToJson(externalResourcesManager.checkingMissingImagesInIPFS()));
	}

	@PostMapping(value = "/mgmt/ipfs-maintenance")
	@ResponseBody
	public ResponseEntity<String> uploadMissingImagesToIPFS() {
		checkIPFSRunning();
		return ResponseEntity.ok(formatter.fullObjectToJson(externalResourcesManager.uploadMissingResourcesToIPFS()));
	}

	@GetMapping(value = "/mgmt/ipfs-status")
	@ResponseBody
	public ResponseEntity<String> getStatusCheckingMissingImagesInBlockchain() {
		checkIPFSRunning();
		return ResponseEntity.ok(formatter.fullObjectToJson(externalResourcesManager.statusImagesInDB()));
	}

	@PostMapping(value = "/mgmt/clean-deprecated-ipfs")
	@ResponseBody
	public ResponseEntity<String> removeUnactivatedAndTimeoutImages() throws IOException {
		checkIPFSRunning();
		return ResponseEntity.ok(formatter.fullObjectToJson(externalResourcesManager.removeUnusedImageObjectsFromSystemAndUnpinningThem()));
	}

}
