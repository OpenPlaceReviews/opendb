package org.openplacereviews.opendb;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.api.ApiController;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.OperationsQueueManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableJpaAuditing
public class OpenDBServer  {
	protected static final Log LOGGER = LogFactory.getLog(ApiController.class);
	@Autowired
	BlocksManager blocksManager;
	
	@Autowired
	OperationsQueueManager queueManager;
	
	public static void main(String[] args) {
		System.setProperty("spring.devtools.restart.enabled", "false");
		SpringApplication.run(OpenDBServer.class, args);
	}

	@Bean
	public CommandLineRunner commandLineRunner(ApplicationContext ctx) {
		return args -> {
			System.out.println("Application has started");
			blocksManager.init();
			System.out.println("Blocks are initialized");
			queueManager.init();
			System.out.println("Queue is initialized");
		};
	}
}
