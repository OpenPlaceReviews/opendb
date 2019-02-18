package org.openplacereviews.opendb.service;

import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.openplacereviews.opendb.OUtils;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.service.DBDataManager.SqlColumnType;
import org.postgresql.util.PGobject;
import org.springframework.jdbc.core.JdbcTemplate;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public abstract class SimpleExprEvaluator {
	
	public abstract Object execute(SqlColumnType type, JsonObject obj);
	
	public static class EvaluationContext {
		JsonObject ctx;
		JdbcTemplate jdbc;
		Object[] args;
		
		public EvaluationContext(JdbcTemplate jdbc, JsonObject ctx) {
			this.jdbc = jdbc;
			this.ctx = ctx;
		}
		
		public void setArgs(Object... args) {
			this.args = args;
		}
	}
	
	
	static {
		registerFunction("find_by_id", FindByIdFunction.class);
		registerFunction("first", FindByIdFunction.class);
		registerFunction("second", FindByIdFunction.class);
		
	}
	
	private static void registerFunction(String string, Class<FindByIdFunction> class1) {
		// TODO Auto-generated method stub
		
	}
	
	protected static class FindByIdFunction extends SimpleExprEvaluator {

		@Override
		public Object execute(SqlColumnType type, JsonObject obj) {
			// TODO Auto-generated method stub
			return null;
		}
		
	}
	
	
	protected static class ConstantExprEvaluator extends SimpleExprEvaluator {

		private Object o;

		public Object execute(SqlColumnType type, JsonObject obj) {
			if (o == null) {
				return null;
			}
			if (type == SqlColumnType.INT) {
				return o;
			}
			return o.toString();
		}
	}

	protected static class FieldAccessEvaluator extends SimpleExprEvaluator {
		List<String> fieldAccess = new ArrayList<String>();

		public Object execute(SqlColumnType type, JsonObject obj) {
			JsonElement o = obj;
			for (String f : fieldAccess) {
				o = o.getAsJsonObject().get(f);
				if (o == null) {
					break;
				}
			}
			if (o == null) {
				return null;
			}
			if (type == SqlColumnType.INT) {
				return o.getAsInt();
			}
			if (type == SqlColumnType.TIMESTAMP) {
				try {
					return OpBlock.dateFormat.parse(o.getAsString());
				} catch (ParseException e) {
					throw new IllegalArgumentException(e);
				}
			}
			if (type == SqlColumnType.JSONB) {
				PGobject jsonObject = new PGobject();
				jsonObject.setType("json");
				try {
					jsonObject.setValue(o.toString());
				} catch (SQLException e) {
					throw new IllegalArgumentException(e);
				}
				return jsonObject;
			}
			return o.toString();
		}
	}
	
	public static SimpleExprEvaluator prepareMappingExpression(String value) {
		FieldAccessEvaluator  s = new FieldAccessEvaluator();
		if(value.equals("this")) {
			return new FieldAccessEvaluator();
		} else if (value.startsWith(".") || value.startsWith("this.")) {
			String[] fields = value.substring(value.indexOf('.') + 1).split("\\.");
			for (String f : fields) {
				if(!OUtils.isValidJavaIdentifier(f)) {
					throw new UnsupportedOperationException(String.format("Invalid field access '%s' in expression '%s'", f, value));
				}
				s.fieldAccess.add(f);
			}
		} else {
			throw new UnsupportedOperationException();
		}
		return s;
	}

}
