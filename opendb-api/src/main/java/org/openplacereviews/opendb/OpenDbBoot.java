package org.openplacereviews.opendb;


import java.util.Arrays;
import java.util.List;

import org.openplacereviews.opendb.service.BlocksManager;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@SpringBootApplication
public class OpenDbBoot extends OpenDBServer {

	public static void main(String[] args) {
		System.setProperty("spring.devtools.restart.enabled", "false");
		SpringApplication.run(OpenDbBoot.class, args);
	}

	public void preStartApplication() {
		List<String> bootstrapList =
				Arrays.asList("opr-0-test-user", BlocksManager.BOOT_STD_OPS_DEFINTIONS, BlocksManager.BOOT_STD_ROLES,
						"opr-0-test-grant", BlocksManager.BOOT_STD_VALIDATION);
		blocksManager.setBootstrapList(bootstrapList);
	}
}
