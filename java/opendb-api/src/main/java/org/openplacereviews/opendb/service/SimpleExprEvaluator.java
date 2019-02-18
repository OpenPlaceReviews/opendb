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
import org.openplacereviews.opendb.expr.OpenDBExprParser.FieldAccessContext;
import org.openplacereviews.opendb.expr.OpenDBExprParser.MethodCallContext;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.service.DBDataManager.SqlColumnType;
import org.postgresql.util.PGobject;
import org.springframework.jdbc.core.JdbcTemplate;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class SimpleExprEvaluator {

	public static final String FUNCTION_DB_FIND_BY_ID = "db.find_by_id";
	public static final String FUNCTION_STR_FIRST = "str.first";
	public static final String FUNCTION_STR_SECOND = "str.second";
	public static final String FUNCTION_M_PLUS = "m.plus";

	private ExpressionContext ectx;

	public static class EvaluationContext {
		JsonObject ctx;
		JdbcTemplate jdbc;

		public EvaluationContext(JdbcTemplate jdbc, JsonObject ctx) {
			this.jdbc = jdbc;
			this.ctx = ctx;
		}

	}

	private SimpleExprEvaluator(ExpressionContext ectx) {
		this.ectx = ectx;

	}

	public Object execute(SqlColumnType type, EvaluationContext obj) {
		return executeExpr(ectx, obj);
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
		return ev;
	}

	private Object callFunction(String functionName, List<Object> args) {
		switch (functionName) {
//		case FUNCTION_DB_FIND_BY_ID:
//
//			break;
		case FUNCTION_M_PLUS:
			long l1 = getLongArgument(functionName, args, 0);
			long l2 = getLongArgument(functionName, args, 1);
			return l1 + l2;
		case FUNCTION_STR_FIRST:
		case FUNCTION_STR_SECOND:
			String ffs = getStringArgument(functionName, args, 0);
			if (ffs != null) {
				int indexOf = ffs.indexOf(':');
				if (indexOf != -1) {
					return functionName.equals(FUNCTION_STR_FIRST) ? ffs.substring(0, indexOf) : ffs
							.substring(indexOf + 1);
				}
			}
			return ffs;
		default:
			break;
		}
		throw new UnsupportedOperationException(String.format("Unsupported function '%s'", functionName));
	}

	private String getStringArgument(String functionName, List<Object> args, int i) {
		if (i >= args.size()) {
			throw new UnsupportedOperationException(String.format("Not enough arguments for function '%s'",
					functionName));
		}
		Object o = args.get(i);
		return o == null ? null : o.toString();
	}

	private long getLongArgument(String functionName, List<Object> args, int i) {
		if (i >= args.size()) {
			throw new UnsupportedOperationException(String.format("Not enough arguments for function '%s'",
					functionName));
		}
		Object o = args.get(i);
		return o == null ? 0 : Long.parseLong(o.toString());
	}

	private Object executeExpr(ExpressionContext ectx, EvaluationContext obj) {
		ParseTree child = ectx.getChild(0);
		if (child instanceof TerminalNode) {
			TerminalNode t = ((TerminalNode) child);
			if (t.getSymbol().getType() == OpenDBExprParser.INT) {
				return Long.parseLong(t.getText());
			} else if (t.getSymbol().getType() == OpenDBExprParser.STRING_LITERAL1) {
				return t.getText().substring(1, t.getText().length() - 1).replace("\\\'", "\'");
			} else if (t.getSymbol().getType() == OpenDBExprParser.STRING_LITERAL2) {
				return t.getText().substring(1, t.getText().length() - 1).replace("\\\"", "\"");
			}
			throw new UnsupportedOperationException("Terminal node is not supported");
		}
		if (child instanceof FieldAccessContext) {
			FieldAccessContext mcc = ((FieldAccessContext) child);
			List<String> fieldAccess = new ArrayList<String>();
			for (int i = 0; i < mcc.getChildCount(); i++) {
				TerminalNode pt = (TerminalNode) mcc.getChild(i);
				if (pt.getSymbol().getType() == OpenDBExprLexer.NAME) {
					fieldAccess.add(pt.getSymbol().getText());
				}
			}
			JsonElement o = obj.ctx;
			for (String f : fieldAccess) {
				if (o == null) {
					break;
				}
				o = o.getAsJsonObject().get(f);

			}
			return o;
		}
		if (child instanceof MethodCallContext) {
			MethodCallContext mcc = ((MethodCallContext) child);
			String functionName = "";
			boolean functionNameComplete = false;
			List<Object> args = new ArrayList<Object>();
			for (int i = 0; i < mcc.getChildCount(); i++) {
				ParseTree pt = mcc.getChild(i);
				if (pt instanceof TerminalNode) {
					int tp = ((TerminalNode)pt).getSymbol().getType();
					if(tp == OpenDBExprLexer.OPENB) {
						functionNameComplete = true;
					} else if (!functionNameComplete) {
						functionName += pt.getText();
					}
				} else {
					args.add(executeExpr((ExpressionContext) pt, obj));
				}
			}
			return callFunction(functionName, args);
		}
		throw new UnsupportedOperationException("Unsupported parser operation: %s" + child.getText());
	}

	public Object evaluateForJson(SqlColumnType type, JsonObject obj) {
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

	public static class ThrowingErrorListener extends BaseErrorListener {

		private String value;

		public ThrowingErrorListener(String value) {
			this.value = value;
		}

		@Override
		public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine,
				String msg, RecognitionException e) throws ParseCancellationException {
			throw new ParseCancellationException(String.format("Error parsing expression '%s' %d:%d %s", value, line,
					charPositionInLine, msg));
		}
	}



}
