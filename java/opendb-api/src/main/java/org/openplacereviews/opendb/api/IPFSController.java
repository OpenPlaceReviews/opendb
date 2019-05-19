package org.openplacereviews.opendb.api;

import org.openplacereviews.opendb.service.ipfs.IPFSService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import static org.springframework.http.MediaType.MULTIPART_FORM_DATA_VALUE;

@Controller
@RequestMapping("/api/image")
public class IPFSController {

	@Autowired
	private IPFSService ipfsService;

	@PostMapping(consumes = MULTIPART_FORM_DATA_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public String uploadImage(
			@RequestPart(name = "file") @Valid @NotNull MultipartFile file) {
		return null;
	}

}
