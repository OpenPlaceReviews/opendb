package org.openplacereviews.opendb.ops;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openplacereviews.opendb.ops.OpBlockChain.SearchType;
import org.openplacereviews.opendb.ops.de.ColumnDef;
import org.openplacereviews.opendb.util.JsonObjectUtils;
import org.openplacereviews.opendb.util.OUtils;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

public class OpIndexColumn {

	protected static final Log LOGGER = LogFactory.getLog(OpIndexColumn.class);

	private final String indexId;
	private final String opType;
	private final ColumnDef columnDef;
	private List<List<String>> fieldsExpression = Collections.emptyList();
	
	
	public OpIndexColumn(String opType, String indexId, ColumnDef columnDef) {
		this.opType = opType;
		this.indexId = indexId;
		this.columnDef = columnDef;
	}
	
	public String getOpType() {
		return opType;
	}
	
	public String getIndexId() {
		return indexId;
	}
	
	public void setFieldsExpression(Collection<String> fieldsExpression) {
		List<List<String>> nf = new ArrayList<>();
		if (fieldsExpression != null) {
			for (String o : fieldsExpression) {
				String[] split = o.split("\\.");
				List<String> fld = new ArrayList<String>();
				for(String s : split) {
					fld.add(s);
				}
				nf.add(fld);
			}
		}
		this.fieldsExpression = nf;
	}
	
	public ColumnDef getColumnDef() {
		return columnDef;
	}
	
	public Object evalDBValue(OpObject opObject, JdbcTemplate jdbcTemplate) {
		List<Object> array = null;
		for (List<String> f : fieldsExpression) {
			array = JsonObjectUtils.getIndexObjectByField(opObject.getRawOtherFields(), f, null);
		}
		if(array != null) {
			Iterator<Object> it = array.iterator();
			while(it.hasNext()) {
				Object o = it.next();
				if(o == null) {
					it.remove();
				}
			}
		}
		if(array == null || array.size() == 0) {
			return null;
		}
		if(columnDef.isArray()) {
			return generateArrayObject(jdbcTemplate, array.toArray(new Object[array.size()]));
		} else {
			return array.get(0);
		}
	}

	private String getColumnType() {
		int indexOf = columnDef.getColType().indexOf("[");
		String columnType = columnDef.getColType();
		if (indexOf != -1) {
			columnType = columnDef.getColType().substring(0, indexOf);
		}

		return columnType;
	}

	public Object generateArrayObject(JdbcTemplate jdbcTemplate, Object[] object) {
		try {
			Connection connection = jdbcTemplate.getDataSource().getConnection();
			Array array = connection.createArrayOf(getColumnType(), object);
			connection.close();
			return array;
		} catch (SQLException e) {
			LOGGER.error("Error while creating sql array", e);
			return null;
		}
	}

	public boolean accept(OpObject opObject, SearchType searchType, Object[] argsToSearch) {
		List<Object> array = null;
		for (List<String> f : fieldsExpression) {
			array = JsonObjectUtils.getIndexObjectByField(opObject.getRawOtherFields(), f, null);
		}
		if (array != null && argsToSearch.length > 0) {
			for (Object s : array) {
				if(searchType == SearchType.STRING_EQUALS) {
					if(OUtils.equalsStringValue(s, argsToSearch[0])) {
						return true;
					}
				}
			}
		}
		return false;
	}
	
	

	
}