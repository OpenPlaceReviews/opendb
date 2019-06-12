package org.openplacereviews.opendb.api;

import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.dto.IpfsStatusDTO;
import org.openplacereviews.opendb.dto.ResourceDTO;
import org.openplacereviews.opendb.service.IPFSFileManager;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.exception.ConnectionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.IOException;

import static org.springframework.http.MediaType.APPLICATION_OCTET_STREAM;
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
		if(!externalResourcesManager.isIPFSRunning()) {
			throw new ConnectionException("IPFS service is not running.");
		}
	}

	@PostMapping(value = "/image", consumes = MULTIPART_FORM_DATA_VALUE, produces = "text/html;charset=UTF-8")
	@ResponseBody
	public ResponseEntity<String> uploadImage(
			@RequestPart(name = "file") @Valid @NotNull @NotEmpty MultipartFile file) throws IOException {
		checkIPFSRunning();
		ResourceDTO resourceDTO = ResourceDTO.of(file);
		resourceDTO = externalResourcesManager.addFile(resourceDTO);
		return ResponseEntity.ok(formatter.imageObjectToJson(resourceDTO));
	}


	@GetMapping(value = "/image")
	@ResponseBody
	public ResponseEntity<FileSystemResource> getFile(@RequestParam("hash") String hash, 
			@RequestParam(value="ext", required=false) String ext) throws IOException {
		checkIPFSRunning();
		File file = externalResourcesManager.getFileByHash(hash, ext);

		HttpHeaders httpHeaders = new HttpHeaders();
		httpHeaders.add("Content-Disposition", "attachment; filename=" + file.getName());
		httpHeaders.add("Content-Length", String.valueOf(file.length()));

		return ResponseEntity.ok()
				.contentType(APPLICATION_OCTET_STREAM)
				.headers(httpHeaders)
				.body(new FileSystemResource(file));
	}
	
	@ResponseBody
	@GetMapping("status")
	public ResponseEntity<String> loadIpfsStatus(@RequestParam(value = "full", required = false) boolean full)
			throws IOException, UnirestException {
		IpfsStatusDTO ipfsStatusDTO;
		if (!externalResourcesManager.isIPFSRunning()) {
			ipfsStatusDTO = new IpfsStatusDTO().setStatus("NOT CONNECTED");
		} else {
			ipfsStatusDTO = externalResourcesManager.getCurrentStatus(full);
		}
		return ResponseEntity.ok(formatter.fullObjectToJson(ipfsStatusDTO));
	}
	

	@PostMapping(value = "/mgmt/ipfs-maintenance")
	@ResponseBody
	public ResponseEntity<String> uploadMissingImagesToIPFS() {
		checkIPFSRunning();
		return ResponseEntity.ok(formatter.fullObjectToJson(externalResourcesManager.uploadMissingResourcesToIPFS()));
	}

	@PostMapping(value = "/mgmt/clean-deprecated-ipfs")
	@ResponseBody
	public ResponseEntity<String> removeUnactivatedAndTimeoutImages() throws IOException {
		checkIPFSRunning();
		return ResponseEntity.ok(formatter.fullObjectToJson(externalResourcesManager.removeUnusedImageObjectsFromSystemAndUnpinningThem()));
	}

}
