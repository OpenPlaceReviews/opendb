package org.openplacereviews.opendb.ipfs;

import com.google.gson.Gson;
import io.ipfs.api.IPFS;
import io.ipfs.api.MerkleNode;
import io.ipfs.api.NamedStreamable;
import io.ipfs.multihash.Multihash;
import org.apache.commons.io.IOUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.openplacereviews.opendb.service.ipfs.IPFSService;
import org.openplacereviews.opendb.service.ipfs.dto.ImageDTO;
import org.openplacereviews.opendb.service.ipfs.pinning.IPFSFileManager;
import org.openplacereviews.opendb.service.ipfs.pinning.PinningService;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.web.multipart.MultipartFile;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

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

	@Ignore
	@Test
	public void testJson() {
		String json = "{\n" +
				"\t\t\"type\"  : \"sys.role\",\n" +
				"\t\t\"new\" : [{\n" +
				"\t\t\t\"id\" : [\"none2\"],\n" +
				"\t\t\t\"comment\" : \"2Owner role is a super role that nobody could change it\",\n" +
				"\t\t\t\"owner_role\" : \"none\",\n" +
				"\t\t\t\"picture\" :  {\"type\":\"#image\",\"hash\":\"sha256:2cfcfbce76b5c305f3939755c96b9e12283cc49f1104f92dcb82925234304c43\",\"extension\":\"jpg\",\"cid\":\"QmWyPQ7ebGAWdyjPoRXvAJEYsYiPoR8urWC65gtsS381vp\"},\n" +
				"\t\t\t\"super_roles\": []\n" +
				"\t\t}],\n" +
				"\t\t\"details\": { \"profile-picture\":{\"type\":\"#image\",\"hash\":\"sha256:2cfcfbce76b5c305f3939755c96b9e12283cc49f1104f92dcb82925234304c43\",\"extension\":\"jpg\",\"cid\":\"QmWyPQ7ebGAWdyjPoRXvAJEYsYiPoR8urWC65gtsS381vp\"}}\n" +
				"\t}";

		TreeMap treeMap = new Gson().fromJson(json, TreeMap.class);

		List<ImageDTO> imageDTOList = new ArrayList<>();
		//getObject(treeMap, imageDTOList);
	}
}
