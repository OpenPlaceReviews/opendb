package org.openplacereviews.opendb.service;

import static org.openplacereviews.opendb.ops.de.ColumnDef.IndexType.GIN;
import static org.openplacereviews.opendb.ops.de.ColumnDef.IndexType.GIST;
import static org.openplacereviews.opendb.ops.de.ColumnDef.IndexType.INDEXED;
import static org.openplacereviews.opendb.ops.de.ColumnDef.IndexType.NOT_INDEXED;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.openplacereviews.opendb.OpenDBServer.MetadataColumnSpec;
import org.openplacereviews.opendb.OpenDBServer.MetadataDb;
import org.openplacereviews.opendb.ops.de.ColumnDef;
import org.openplacereviews.opendb.ops.de.ColumnDef.IndexType;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

public class DBSchemaHelper {

	
	Map<String, List<ColumnDef>> schema = new HashMap<String, List<ColumnDef>>();
	private String settingsTableName;
	
	public DBSchemaHelper(String settingsTable) {
		this.settingsTableName = settingsTable;
		registerColumn(settingsTableName, "key", "text PRIMARY KEY", INDEXED);
		registerColumn(settingsTableName, "value", "text", NOT_INDEXED);
		registerColumn(settingsTableName, "content", "jsonb", NOT_INDEXED);
	}
	

	public void registerColumn(String tableName, String colName, String colType, IndexType basicIndexType) {
		ColumnDef cd = new ColumnDef(tableName, colName, colType, basicIndexType);
		registerColumn(tableName, cd);
	}
	
	public void initSettingsTable(MetadataDb metadataDB, JdbcTemplate jdbcTemplate) {
		createTable(metadataDB, jdbcTemplate, settingsTableName, schema.get(settingsTableName));		
	}
	
	public void createTablesIfNeeded(MetadataDb metadataDB, JdbcTemplate jdbcTemplate) {
		for (String tableName : schema.keySet()) {
			if(tableName.equals(settingsTableName))  {
				 continue;
			}
			List<ColumnDef> cls = schema.get(tableName);
			createTable(metadataDB, jdbcTemplate, tableName, cls);
		}		
	}

	public ColumnDef registerColumn(String tableName, ColumnDef cd) {
		List<ColumnDef> lst = schema.get(tableName);
		if (lst == null) {
			lst = new ArrayList<ColumnDef>();
			schema.put(tableName, lst);
		}

		lst.add(cd);
		return cd;
	}
	
	
	public void createTable(MetadataDb metadataDB, JdbcTemplate jdbcTemplate, String tableName, List<ColumnDef> cls) {
		List<MetadataColumnSpec> list = metadataDB.tablesSpec.get(tableName);
		if (list == null) {
			StringBuilder clb = new StringBuilder();
			List<String> indx = new ArrayList<String>();
			for (ColumnDef c : cls) {
				if (clb.length() > 0) {
					clb.append(", ");
				}
				clb.append(c.getColName()).append(" ").append(c.getColType());
				if(c.getIndex() != NOT_INDEXED) {
					indx.add(generateIndexQuery(c));
				}
			}
			String createTable = String.format("create table %s (%s)", tableName, clb.toString());
			jdbcTemplate.execute(createTable);
			for (String ind : indx) {
				jdbcTemplate.execute(ind);
			}
		} else {
			for (ColumnDef c : cls) {
				boolean found = false;
				for (MetadataColumnSpec m : list) {
					if (c.getColName().equals(m.columnName)) {
						found = true;
						break;
						
					}
				}
				if (!found) {
					alterTableNewColumn(jdbcTemplate, c);
				}
			}
		}
	}

	public void alterTableNewColumn(JdbcTemplate jdbcTemplate, ColumnDef c) {
		String alterTable = String.format("alter table %s add column %s %s", c.getTableName(), 
				c.getColName(), c.getColType());
		jdbcTemplate.execute(alterTable);
		if(c.getIndex() != NOT_INDEXED) {
			jdbcTemplate.execute(generateIndexQuery(c));
		}
	}
	
	

	public boolean setSetting(JdbcTemplate jdbcTemplate, String key, String v) {
		return jdbcTemplate.update("insert into  " + settingsTableName + "(key,value) values (?, ?) "
				+ " ON CONFLICT (key) DO UPDATE SET value = ? ", key, v, v) != 0;
	}
	
	public int getIntSetting(JdbcTemplate jdbcTemplate, String key) {
		String s = getSetting(jdbcTemplate, key);
		if(s == null) {
			return 0;
		}
		return Integer.parseInt(s);
	}

	public int removeSetting(JdbcTemplate jdbcTemplate, String key) {
		return jdbcTemplate.update("DELETE FROM " + settingsTableName + " WHERE key = ?", key);
	}

	public String getSetting(JdbcTemplate jdbcTemplate, String key) {
		String s = null;
		try {
			s = jdbcTemplate.query("select value from " + settingsTableName + " where key = ?", new ResultSetExtractor<String>() {
				@Override
				public String extractData(ResultSet rs) throws SQLException, DataAccessException {
					boolean next = rs.next();
					if (next) {
						return rs.getString(1);
					}
					return null;
				}
			}, key);
		} catch (DataAccessException e) {
		}
		return s;
	}
	
	protected String generateIndexName(IndexType indexType, String tableName, String colName) {
		switch (indexType) {
			case INDEXED: {
				return String.format("%s_%s_ind", tableName, colName);
			}
			case GIN: {
				return String.format("%s_%s_gin_ind", tableName, colName);
			}
			case GIST: {
				return String.format("%s_%s_gist_ind", tableName, colName);
			}
			default: {
				throw new UnsupportedOperationException();
			}
		}
	}

	private String generateIndexQuery(ColumnDef c) {
		String indName = generateIndexName(c.getIndex(), c.getTableName(), c.getColName());
		if (c.getIndex() == INDEXED) {
			return String.format("create index %s on %s (%s);\n", indName,
					c.getTableName(), c.getColName());
		} else if (c.getIndex() == GIN) {
			return String.format("create index %s on %s using gin (%s);\n", indName,
					c.getTableName(), c.getColName());
		} else if (c.getIndex() == GIST) {
			return String.format("create index %s on %s using gist (tsvector(%s));\n", indName,
					c.getTableName(), c.getColName());
		}
		return null;
	}
	

	// Query / insert values
	// select encode(b::bytea, 'hex') from test where b like (E'\\x39')::bytea||'%';
	// insert into test(b) values (decode('39556d070fd95f54b554010207d42605a8d0adfbb3b8b8e134df7df0689d78ab', 'hex'));
	// UPDATE blocks SET superblocks = array_remove(superblocks,
	// decode('39556d070fd95f54b554010207d42605a8d0adfbb3b8b8e134df7df0689d78ab', 'hex'));

	public static void main(String[] args) {
		DBSchemaHelper db = new DBSchemaHelper("opendb_settings");
		for (String tableName : db.schema.keySet()) {
			List<ColumnDef> cls = db.schema.get(tableName);
			StringBuilder clb = new StringBuilder();
			StringBuilder indx = new StringBuilder();
			for (ColumnDef c : cls) {
				if (clb.length() > 0) {
					clb.append(", ");
				}
				clb.append(c.getColName()).append(" ").append(c.getColType());
				if (c.getIndex() == INDEXED) {
					indx.append(String.format("create index %s_%s_ind on %s (%s);\n", c.getTableName(), c.getColName(),
							c.getTableName(), c.getColName()));
				}
			}
			System.out.println(String.format("create table %s (%s);", tableName, clb.toString()));
			System.out.println(indx.toString());
		}

	}



	
}
