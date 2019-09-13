package org.openplacereviews.opendb.dto;

import java.io.Serializable;
import java.util.List;

public class RequestIndexBody implements Serializable {

	public String tableName;
	public String colName;
	public String colType;
	public String index;
	public String[] types;
	public String sqlMapping;
	public Integer cacheRuntimeMax;
	public Integer cacheDbIndex;
	public List<String> field;

	public RequestIndexBody() {
	}

}