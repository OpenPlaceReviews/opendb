package org.openplacereviews.opendb.api;

import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.service.ipfs.IPFSService;
import org.openplacereviews.opendb.service.ipfs.dto.ImageDTO;
import org.openplacereviews.opendb.service.ipfs.dto.IpfsStatus;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.exception.ConnectionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
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

	@Value("${ipfs.run:false}")
	public boolean ipfsServiceIsRunning;

	@PostMapping(value = "/image", consumes = MULTIPART_FORM_DATA_VALUE, produces = "text/html;charset=UTF-8")
	@ResponseBody
	public ResponseEntity<String> uploadImage(
			@RequestPart(name = "file") @Valid @NotNull MultipartFile file) throws IOException {
		if (!ipfsServiceIsRunning)
			throw new ConnectionException("IPFS service was not runned!");

		ImageDTO imageDTO = ImageDTO.of(file);
		imageDTO = ipfsService.addFile(imageDTO);

		return ResponseEntity.ok(formatter.imageObjectToJson(imageDTO));
	}

	@GetMapping
	@ResponseBody
	public ResponseEntity<String> loadIpfsStatus() throws IOException, UnirestException {
		IpfsStatus ipfsStatus = ipfsServiceIsRunning ? ipfsService.getIpfsStatus() : getIpfsStatus();

		return ResponseEntity.ok(formatter.fullObjectToJson(ipfsStatus));
	}

	@GetMapping(value = "/image")
	@ResponseBody
	public void getFile(@RequestParam("cid") String cid, HttpServletResponse response) throws IOException {
		if (!ipfsServiceIsRunning)
			throw new ConnectionException("IPFS service was not runned!");

		ByteArrayOutputStream outputStream = (ByteArrayOutputStream) ipfsService.read(cid);

		outputStream.writeTo(response.getOutputStream());
	}

	@PostMapping(value = "/image")
	@ResponseBody
	public ResponseEntity pinImage(@RequestParam("cid") String cid) {
		if (!ipfsServiceIsRunning)
			throw new ConnectionException("IPFS service was not runned!");

		if (!ipfsService.pin(cid)) {
			return ResponseEntity.ok("{\"status\":\"FAILED\"}");
		}

		return ResponseEntity.ok("{\"status\":\"OK\"}");
	}

	@DeleteMapping(value = "/image")
	@ResponseBody
	public ResponseEntity unpinImage(@RequestParam("cid") String cid) {
		if (!ipfsServiceIsRunning)
			throw new ConnectionException("IPFS service was not runned!");

		if(!ipfsService.unpin(cid)) {
			return ResponseEntity.ok("{\"status\":\"FAILED\"}");
		}

		return ResponseEntity.ok("{\"status\":\"OK\"}");
	}

	@GetMapping(value = "/image/tracked")
	@ResponseBody
	public ResponseEntity getTrackedFiles() {
		if (!ipfsServiceIsRunning)
			throw new ConnectionException("IPFS service was not runned!");

		List<String> tracked = ipfsService.getTracked();
		return ResponseEntity.ok(formatter.fullObjectToJson(tracked));
	}

	private IpfsStatus getIpfsStatus() {
		return new IpfsStatus().setStatus("NOT CONNECTED");
	}

}
