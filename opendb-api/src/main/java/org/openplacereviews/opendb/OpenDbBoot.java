package org.openplacereviews.opendb;


import org.openplacereviews.opendb.service.BlocksManager;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;
import java.util.List;

@SpringBootApplication
public class OpenDbBoot extends OpenDBServer {

	public static void main(String[] args) {
		System.setProperty("spring.devtools.restart.enabled", "false");
		SpringApplication.run(OpenDbBoot.class, args);
	}

	public void preStartApplication() {
		List<String> bootstrapList =
				Arrays.asList("opr-0-test-user", BlocksManager.BOOT_STD_OPS_DEFINTIONS, BlocksManager.BOOT_STD_ROLES,
						BlocksManager.BOOT_OPR_TEST_GRANT, BlocksManager.BOOT_STD_VALIDATION);
		blocksManager.setBootstrapList(bootstrapList);
	}
}
