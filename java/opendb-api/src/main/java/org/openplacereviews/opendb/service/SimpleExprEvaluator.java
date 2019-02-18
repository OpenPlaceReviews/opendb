package org.openplacereviews.opendb.service;

import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.openplacereviews.opendb.expr.OpenDBExprLexer;
import org.openplacereviews.opendb.expr.OpenDBExprParser;
import org.openplacereviews.opendb.expr.OpenDBExprParser.ExpressionContext;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.service.DBDataManager.SqlColumnType;
import org.postgresql.util.PGobject;
import org.springframework.jdbc.core.JdbcTemplate;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class SimpleExprEvaluator {
	
	private ExpressionContext ectx;
	
	
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
		
	private SimpleExprEvaluator(ExpressionContext ectx) {
		this.ectx = ectx;
		
	}
	
	
	public Object execute(SqlColumnType type, EvaluationContext obj) {
		ParseTree child = ectx.getChild(0);
		if(child instanceof TerminalNode){
			TerminalNode t = ((TerminalNode)child);
			if(t.getSymbol().getType()  == OpenDBExprParser.INT) {
				return Integer.parseInt(t.getText());
			} else if(t.getSymbol().getType()  == OpenDBExprParser.STRING_LITERAL1) {
				return t.getText().substring(1, t.getText().length() - 1).replace("\\\'", "\'");
			} else if(t.getSymbol().getType()  == OpenDBExprParser.STRING_LITERAL2) {
				return t.getText().substring(1, t.getText().length() - 1).replace("\\\"", "\"");
			}
			throw new UnsupportedOperationException("Terminal node is not supported");
		}
		
		System.out.println(ectx);
		return null;
		
	}
	
	public Object execute(SqlColumnType type, JsonObject obj) {
		List<String> fieldAccess = new ArrayList<String>();
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

	
	
	
	
	static {
//		registerFunction("find_by_id", FindByIdFunction.class);
//		registerFunction("first", FindByIdFunction.class);
//		registerFunction("second", FindByIdFunction.class);
//		
	}

	
	public static void main(String[] args) {
		Gson gson = new Gson();
		JsonElement obj = gson.fromJson("{'a':1}", JsonElement.class);
		EvaluationContext ectx = new EvaluationContext(null, obj.getAsJsonObject());
		System.out.println(parseMappingExpression("1").execute(null,  ectx));
		System.out.println(parseMappingExpression("'1\"\\\''").execute(null,  ectx));
		System.out.println(parseMappingExpression("\"1\\\"\'\"").execute(null,  ectx));
	}

	public static class ThrowingErrorListener extends BaseErrorListener {


		private String value;

		public ThrowingErrorListener(String value) {
			this.value = value;
		}

		@Override
		public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine,
				String msg, RecognitionException e) throws ParseCancellationException {
			throw new ParseCancellationException( 
					String.format("Error parsing expression '%s' %d:%d %s", value, line, charPositionInLine, msg));
		}
	}
	
	public static SimpleExprEvaluator parseMappingExpression(String value) {
		OpenDBExprLexer lexer = new OpenDBExprLexer(new ANTLRInputStream(value));
		ThrowingErrorListener twt = new ThrowingErrorListener(value);
		lexer.removeErrorListeners();
		lexer.addErrorListener(twt);
		OpenDBExprParser parser = new OpenDBExprParser(new CommonTokenStream(lexer));
		parser.removeErrorListeners();
		parser.addErrorListener(twt);
		ExpressionContext ectx = parser.expression();
		
		SimpleExprEvaluator ev = new SimpleExprEvaluator(ectx);
//		if(value.equals("this")) {
//			return new FieldAccessEvaluator();
//		} else if (value.startsWith(".") || value.startsWith("this.")) {
//			String[] fields = value.substring(value.indexOf('.') + 1).split("\\.");
//			for (String f : fields) {
//				if(!OUtils.isValidJavaIdentifier(f)) {
//					throw new UnsupportedOperationException(String.format("Invalid field access '%s' in expression '%s'", f, value));
//				}
//				s.fieldAccess.add(f);
//			}
//		} else {
//			throw new UnsupportedOperationException();
//		}
		return ev;
	}

}
