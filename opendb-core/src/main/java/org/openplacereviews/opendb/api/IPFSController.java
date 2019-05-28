package org.openplacereviews.opendb.api;

import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.service.ipfs.IPFSService;
import org.openplacereviews.opendb.service.ipfs.dto.ImageDTO;
import org.openplacereviews.opendb.service.ipfs.dto.IpfsStatusDTO;
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
	private IPFSService ipfsService;

	@Autowired
	private JsonFormatter formatter;

	@PostMapping(value = "/image", consumes = MULTIPART_FORM_DATA_VALUE, produces = "text/html;charset=UTF-8")
	@ResponseBody
	public ResponseEntity<String> uploadImage(
			@RequestPart(name = "file") @Valid @NotNull @NotEmpty MultipartFile file) throws IOException {
		if (!IPFSService.status)
			throw new ConnectionException("IPFS service was not runned!");

		ImageDTO imageDTO = ImageDTO.of(file);
		imageDTO = ipfsService.addFile(imageDTO);

		return ResponseEntity.ok(formatter.imageObjectToJson(imageDTO));
	}

	@GetMapping
	@ResponseBody
	public ResponseEntity<String> loadIpfsStatus() throws IOException, UnirestException {
		IpfsStatusDTO ipfsStatusDTO = IPFSService.status ? ipfsService.getIpfsNodeInfo() : getIpfsStatus();

		return ResponseEntity.ok(formatter.fullObjectToJson(ipfsStatusDTO));
	}

	@GetMapping(value = "/image")
	@ResponseBody
	public ResponseEntity<String> getFile(@RequestParam("cid") String cid) throws IOException {
		if (!IPFSService.status)
			throw new ConnectionException("IPFS service was not runned!");

		try (ByteArrayOutputStream outputStream = (ByteArrayOutputStream) ipfsService.read(cid)) {
			return ResponseEntity.ok(Base64.getEncoder().encodeToString(outputStream.toByteArray()));
		}
	}

	@PutMapping(value = "/image")
	@ResponseBody
	public ResponseEntity<String> pinImage(@RequestParam("cid") String cid) {
		if (!IPFSService.status)
			throw new ConnectionException("IPFS service was not runned!");

		if (!ipfsService.pin(cid)) {
			return ResponseEntity.ok("{\"status\":\"FAILED\"}");
		}

		return ResponseEntity.ok("{\"status\":\"OK\"}");
	}

	@DeleteMapping(value = "/image")
	@ResponseBody
	public ResponseEntity<String> unpinImage(@RequestParam("cid") String cid) {
		if (!IPFSService.status)
			throw new ConnectionException("IPFS service was not runned!");

		if(!ipfsService.unpin(cid)) {
			return ResponseEntity.ok("{\"status\":\"FAILED\"}");
		}

		return ResponseEntity.ok("{\"status\":\"OK\"}");
	}

	@GetMapping(value = "/image/tracked")
	@ResponseBody
	public ResponseEntity<String> getTrackedFiles() {
		if (!IPFSService.status)
			throw new ConnectionException("IPFS service was not runned!");

		List<String> tracked = ipfsService.getTracked();
		return ResponseEntity.ok(formatter.fullObjectToJson(tracked));
	}

	@GetMapping(value = "/status")
	@ResponseBody
	public ResponseEntity<String> getStatusCheckingMissingImagesInIPFS() {
		if (!IPFSService.status)
			throw new ConnectionException("IPFS service was not runned!");

		return ResponseEntity.ok(formatter.fullObjectToJson(ipfsService.checkingMissingImagesInIPFS()));
	}

	@PutMapping(value = "/upload")
	@ResponseBody
	public ResponseEntity<String> loadMissingImagesToIpfs() {
		if (!IPFSService.status)
			throw new ConnectionException("IPFS service was not runned!");

		return ResponseEntity.ok(formatter.fullObjectToJson(ipfsService.uploadMissingImagesToIPFS()));
	}

	@GetMapping(value = "/blc-status")
	@ResponseBody
	public ResponseEntity<String> getStatusCheckingMissingImagesInBlockchain() {
		if (!IPFSService.status)
			throw new ConnectionException("IPFS service was not runned!");

		return ResponseEntity.ok(formatter.fullObjectToJson(ipfsService.statusImagesInDB()));
	}

	@DeleteMapping(value = "/blc-images")
	@ResponseBody
	public ResponseEntity<String> removeUnactivatedAndTimeoutImages() throws IOException {
		if (!IPFSService.status)
			throw new ConnectionException("IPFS service was not runned!");

		return ResponseEntity.ok(formatter.fullObjectToJson(ipfsService.removeUnusedImageObjectsFromSystemAndUnpinningThem()));
	}

	private IpfsStatusDTO getIpfsStatus() {
		return new IpfsStatusDTO().setStatus("NOT CONNECTED");
	}

}
