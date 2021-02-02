package org.openplacereviews.opendb.api;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.service.IPFSFileManager;
import org.openplacereviews.opendb.service.IPFSFileManager.IpfsStatusDTO;
import org.openplacereviews.opendb.service.IPFSService;
import org.openplacereviews.opendb.service.IPFSService.ResourceDTO;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.OUtils;
import org.openplacereviews.opendb.util.exception.ConnectionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.FileOutputStream;
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
	private IPFSService ipfsService;
	
	@Autowired
	private JsonFormatter formatter;

	private void checkIPFSRunning() {
		if (!externalResourcesManager.isIPFSRunning()) {
			throw new ConnectionException("IPFS service is not running.");
		}
	}

	@PostMapping(value = "/image", consumes = MULTIPART_FORM_DATA_VALUE, produces = "text/html;charset=UTF-8")
	@ResponseBody
	public ResponseEntity<String> uploadImage(@RequestPart(name = "file") @Valid @NotNull @NotEmpty MultipartFile file)
			throws IOException {
		checkIPFSRunning();
		ResourceDTO resourceDTO = ResourceDTO.of(file);
		resourceDTO = externalResourcesManager.addFile(resourceDTO);
		return ResponseEntity.ok(formatter.fullObjectToJson(resourceDTO));
	}
	
	@GetMapping(value = "/image-ipfs")
	@ResponseBody
	public void getFile(HttpServletResponse response,
			@RequestParam(value = "cid") String cid, 
			@RequestParam(value = "ext", required = false, defaultValue = ".jpeg") String ext) throws IOException {
		checkIPFSRunning();
		response.setHeader("Content-Disposition", "attachment; filename=" + cid + ext);
		response.setContentType(APPLICATION_OCTET_STREAM.getType());
		ipfsService.read(cid, response.getOutputStream());
	}

	@GetMapping(value = "/image")
	@ResponseBody
	public ResponseEntity<FileSystemResource> getFile(
			@RequestParam(value = "cid") String cid,
			@RequestParam(value = "hash") String hash,
			@RequestParam(value = "ext", required = false, defaultValue = ".jpeg") String ext) throws IOException {
		File file = externalResourcesManager.getFileByHash(hash, ext);
		if (!file.exists() && !OUtils.isEmpty(cid)) {
			checkIPFSRunning();
			ipfsService.read(cid, new FileOutputStream(file));
		}
		HttpHeaders httpHeaders = new HttpHeaders();
		httpHeaders.add("Content-Disposition", "attachment; filename=" + file.getName());
		httpHeaders.add("Content-Length", String.valueOf(file.length()));

		return ResponseEntity.ok().contentType(APPLICATION_OCTET_STREAM).headers(httpHeaders)
				.body(new FileSystemResource(file));
	}

	@ResponseBody
	@GetMapping("status")
	public ResponseEntity<String> loadIpfsStatus(@RequestParam(value = "full", required = false) boolean full)
			throws IOException {
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
		return ResponseEntity.ok(formatter
				.fullObjectToJson(externalResourcesManager.removeUnusedImageObjectsFromSystemAndUnpinningThem()));
	}

}
