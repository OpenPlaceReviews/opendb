package org.openplacereviews.opendb;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.service.BlocksManager;
import org.openplacereviews.opendb.service.DBDataManager;
import org.openplacereviews.opendb.service.LogOperationService;
import org.openplacereviews.opendb.service.OperationsRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableJpaAuditing
public class OpenDBServer  {
	protected static final Log LOGGER = LogFactory.getLog(OpenDBServer.class);
	
	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	@Autowired
	BlocksManager blocksManager;
	
	@Autowired
	DBDataManager dbDataManager;
	
	@Autowired
	LogOperationService logOperationService;
	
	@Autowired
	OperationsRegistry operationRegistry;
	
	public static void main(String[] args) {
		System.setProperty("spring.devtools.restart.enabled", "false");
		SpringApplication.run(OpenDBServer.class, args);
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
	
	public MetadataDb loadMetadata() {
		MetadataDb d = new MetadataDb();
		try {
			DatabaseMetaData mt = jdbcTemplate.getDataSource().getConnection().getMetaData();
			ResultSet rs = mt.getColumns(null, DBConstants.SCHEMA_NAME, null, null);
			while(rs.next()) {
				String tName = rs.getString("TABLE_NAME");
				if(!d.tablesSpec.containsKey(tName)) {
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
				System.out.println("Application starting...");
				MetadataDb metadataDB = loadMetadata();
				dbDataManager.init(metadataDB);
				logOperationService.init(metadataDB);
				operationRegistry.init(metadataDB);
				blocksManager.init(metadataDB);
				System.out.println("Application has started");
			} catch (RuntimeException e) {
				LOGGER.error(e.getMessage(), e);
				throw e;
			}
		};
	}
}
