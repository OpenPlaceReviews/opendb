package org.openplacereviews.opendb.ipfs;

import io.ipfs.api.NamedStreamable;
import org.apache.commons.io.IOUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.openplacereviews.opendb.dto.ResourceDTO;
import org.openplacereviews.opendb.service.IPFSFileManager;
import org.openplacereviews.opendb.service.IPFSService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class IPFSTest {

	@Autowired
	private IPFSService ipfsService;

	@Ignore
	@Test
	public void connectionMultiAddress() throws Exception {
		String multiaddress = "/ip4/127.0.0.1/tcp/" + 5001;
		IPFSFileManager ipfsFileManager = new IPFSFileManager();
		ipfsFileManager.init();

		File file = new File(
				getClass().getClassLoader().getResource("1.jpg").getFile()
		);

		NamedStreamable.FileWrapper file1 = new NamedStreamable.FileWrapper(file);
	}


	@Ignore
	@Test
	public void testFileManager() throws IOException {
		IPFSFileManager ipfsFileManager = new IPFSFileManager();
		ipfsFileManager.init();

		File file = new File(
				getClass().getClassLoader().getResource("1.jpg").getFile()
		);

		FileInputStream input = new FileInputStream(file);
		MultipartFile multipartFile = new MockMultipartFile("file",
				file.getName(), "text/plain", IOUtils.toByteArray(input));

		ipfsFileManager.addFile(new ResourceDTO());
	}
}
