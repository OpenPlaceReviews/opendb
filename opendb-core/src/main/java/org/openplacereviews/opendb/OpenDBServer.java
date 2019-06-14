package org.openplacereviews.opendb;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.DBConsensusManager;
import org.openplacereviews.opendb.service.IPFSFileManager;
import org.openplacereviews.opendb.service.LogOperationService;
import org.openplacereviews.opendb.util.DBConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.servlet.MultipartConfigFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;

import javax.servlet.MultipartConfigElement;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

@EnableScheduling
@EnableJpaAuditing
@EnableConfigurationProperties
public class OpenDBServer {

	protected static final Log LOGGER = LogFactory.getLog(OpenDBServer.class);
	@Value("${spring.servlet.multipart.max-file-size}")
	public String maxUploadSize;
	@Value("${spring.servlet.multipart.max-request-size}")
	public String maxRequestSize;
	@Autowired
	BlocksManager blocksManager;
	@Autowired
	DBConsensusManager dbDataManager;
	@Autowired
	LogOperationService logOperationService;
	@Autowired
	IPFSFileManager externalResourcesService;
	@Autowired
	private JdbcTemplate jdbcTemplate;

	public static void main(String[] args) {
		System.setProperty("spring.devtools.restart.enabled", "false");
		SpringApplication.run(OpenDBServer.class, args);
	}

	public void preStartApplication() {
		List<String> bootstrapList =
				Arrays.asList("opr-0-test-user", BlocksManager.BOOT_STD_OPS_DEFINTIONS, BlocksManager.BOOT_STD_ROLES,
						BlocksManager.BOOT_OPR_TEST_GRANT, BlocksManager.BOOT_STD_VALIDATION);
		blocksManager.setBootstrapList(bootstrapList);
	}

	public MetadataDb loadMetadata() {
		MetadataDb d = new MetadataDb();
		try {
			DatabaseMetaData mt = jdbcTemplate.getDataSource().getConnection().getMetaData();
			ResultSet rs = mt.getColumns(null, DBConstants.SCHEMA_NAME, null, null);
			while (rs.next()) {
				String tName = rs.getString("TABLE_NAME");
				if (!d.tablesSpec.containsKey(tName)) {
					d.tablesSpec.put(tName, new ArrayList<OpenDBServer.MetadataColumnSpec>());
				}
				List<MetadataColumnSpec> cols = d.tablesSpec.get(tName);
				MetadataColumnSpec spec = new MetadataColumnSpec();
				spec.columnName = rs.getString("COLUMN_NAME");
				spec.sqlType = rs.getInt("DATA_TYPE");
				spec.dataType = rs.getString("TYPE_NAME");
				spec.columnSize = rs.getInt("COLUMN_SIZE");
				cols.add(spec);
			}
		} catch (SQLException e) {
			throw new IllegalStateException("Can't read db metadata", e);
		}
		return d;
	}

	@Bean
	public CommandLineRunner commandLineRunner(ApplicationContext ctx) {
		return args -> {
			try {
				LOGGER.info("Application starting...");
				preStartApplication();
				MetadataDb metadataDB = loadMetadata();
				OpBlockChain blockchain = dbDataManager.init(metadataDB);
				blocksManager.init(metadataDB, blockchain);
				externalResourcesService.init();
				LOGGER.info("Application has started");
			} catch (RuntimeException e) {
				LOGGER.error(e.getMessage(), e);
				throw e;
			}
		};
	}

	@Bean
	public MultipartConfigElement multipartConfigElement() {
		MultipartConfigFactory factory = new MultipartConfigFactory();
		factory.setMaxFileSize(maxUploadSize);
		factory.setMaxRequestSize(maxRequestSize);
		return factory.createMultipartConfig();
	}

	public static class MetadataDb {
		public Map<String, List<MetadataColumnSpec>> tablesSpec = new TreeMap<String, List<MetadataColumnSpec>>();
	}

	public static class MetadataColumnSpec {
		public String name;
		public String columnName;
		public int sqlType;
		public String dataType;
		public int columnSize;

		@Override
		public String toString() {
			return "Column [name=" + name + ", columnName=" + columnName + ", sqlType=" + sqlType
					+ ", dataType=" + dataType + ", columnSize=" + columnSize + "]";
		}
	}

}
