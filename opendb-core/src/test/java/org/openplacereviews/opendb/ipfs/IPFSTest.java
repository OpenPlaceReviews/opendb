package org.openplacereviews.opendb.ipfs;

import io.ipfs.api.IPFS;
import io.ipfs.api.MerkleNode;
import io.ipfs.api.NamedStreamable;
import io.ipfs.multihash.Multihash;
import org.apache.commons.io.IOUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.openplacereviews.opendb.service.ipfs.IPFSService;
import org.openplacereviews.opendb.service.ipfs.file.IPFSFileManager;
import org.openplacereviews.opendb.service.ipfs.pinning.PinningService;
import org.openplacereviews.opendb.service.ipfs.dto.ImageDTO;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.web.multipart.MultipartFile;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.util.List;
import java.util.Set;

public class IPFSTest {

	@Ignore
	@Test
	public void connectionMultiAddress() throws Exception {
		String multiaddress = "/ip4/127.0.0.1/tcp/" + 5001;

		IPFS ipfs = new IPFS("/ip4/127.0.0.1/tcp/5001");

		File file = new File(
				getClass().getClassLoader().getResource("1.jpg").getFile()
		);

		NamedStreamable.FileWrapper file1 = new NamedStreamable.FileWrapper(file);

		MerkleNode merkleNode = ipfs.add(file1).get(0);

		Multihash filePointer = Multihash.fromBase58(merkleNode.hash.toString());
		byte[] fileContents = ipfs.cat(filePointer);

		ByteArrayInputStream bis = new ByteArrayInputStream(fileContents);
		BufferedImage bImage2 = ImageIO.read(bis);
		ImageIO.write(bImage2, "jpg", new File("output.jpg") );

		ipfs.pin.rm(filePointer);

		IPFSService ipfsService = new IPFSService();
		ipfsService.connect("/ip4/127.0.0.1/tcp/5001");

		String cid = ipfsService.write(file1.getInputStream());

		List<String> list = ipfsService.getTracked();
		Set<PinningService> set = ipfsService.getReplicaSet();
		OutputStream outputStream = ipfsService.read(cid);

		ipfsService.unpin(cid);
	}

	@Ignore
	@Test
	public void IPFS() throws IOException {
		IPFSService ipfsService = new IPFSService();
		ipfsService.connect("/ip4/127.0.0.1/tcp/5001");
		//ipfsService.addReplica(IPFSClusterPinningService.connect("",0,""));
		File file = new File(
				getClass().getClassLoader().getResource("1.jpg").getFile()
		);

		NamedStreamable.FileWrapper file1 = new NamedStreamable.FileWrapper(file);
		String cid = ipfsService.write(file1.getInputStream());

		System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!-> " + cid);

		ipfsService.pin(cid);

		List<String> set = ipfsService.getTracked();

		ipfsService.unpin(cid);

		set = ipfsService.getTracked();
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

		ipfsFileManager.addFileToStorage(new ImageDTO());
	}
}
