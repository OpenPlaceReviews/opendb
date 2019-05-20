package org.openplacereviews.opendb.api;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.service.ipfs.IPFSService;
import org.openplacereviews.opendb.service.ipfs.storage.ImageDTO;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.io.IOException;

import static org.springframework.http.MediaType.MULTIPART_FORM_DATA_VALUE;

@Controller
@RequestMapping("/api/image")
public class IPFSController {

	protected static final Log LOGGER = LogFactory.getLog(IPFSController.class);

	@Autowired
	private IPFSService ipfsService;

	@Autowired
	private JsonFormatter formatter;

	@PostMapping(consumes = MULTIPART_FORM_DATA_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<String> uploadImage(
			@RequestPart(name = "file") @Valid @NotNull MultipartFile file) throws IOException {
		ImageDTO imageDTO = ImageDTO.of(file);
		imageDTO = ipfsService.addFile(imageDTO);

		return ResponseEntity.ok(formatter.imageObjectToJson(imageDTO));
	}

	@GetMapping
	@ResponseBody
	public FileSystemResource getFile(@RequestParam("cid") String cid) {
		return null;
	}

}
