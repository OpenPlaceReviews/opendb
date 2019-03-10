package org.openplacereviews.opendb.util;

import java.sql.Date;
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
import org.openplacereviews.opendb.OUtils;
import org.openplacereviews.opendb.expr.OpenDBExprLexer;
import org.openplacereviews.opendb.expr.OpenDBExprParser;
import org.openplacereviews.opendb.expr.OpenDBExprParser.ExpressionContext;
import org.openplacereviews.opendb.expr.OpenDBExprParser.MethodCallContext;
import org.openplacereviews.opendb.ops.OpBlock;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.service.DBDataManager.SqlColumnType;
import org.postgresql.util.PGobject;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class SimpleExprEvaluator {

	public static final String FUNCTION_DB_FIND_UNIQUE = "db:find_unique";
	public static final String FUNCTION_STR_FIRST = "str:first";
	public static final String FUNCTION_STR_SECOND = "str:second";
	public static final String FUNCTION_M_PLUS = "m:plus";
	public static final String FUNCTION_STD_EQ = "std:eq";
	public static final String FUNCTION_STD_NEQ = "std:neq";
	public static final String FUNCTION_STD_LEQ = "std:leq";
	public static final String FUNCTION_STD_LE = "std:le";
	public static final String FUNCTION_STD_SIZE = "std:size";
	public static final String FUNCTION_BLC_FIND = "blc:find";
	
	// TODOO
	// op:op_signatures
	// auth:has_sig_all_roles
	// auth:has_sig_role
	// auth:has_sig_user
	// auth:has_sig_any_user(op:op_signatures(db:find_op(signup)))
	

	private ExpressionContext ectx;

	public static class EvaluationContext {
		JsonObject ctx;
		OpBlockChain op;

		public EvaluationContext(OpBlockChain blockchain, JsonObject ctx, 
				JsonElement deleted, JsonObject refs) {
			this.op = blockchain;
			this.ctx = ctx;
			this.ctx.add("ref", refs);
			this.ctx.add("old", deleted);
		}

	}

	private SimpleExprEvaluator(ExpressionContext ectx) {
		this.ectx = ectx;

	}

	public Object evaluateObject(EvaluationContext obj) {
		return eval(ectx, obj);
	}
	
	public boolean evaluteBoolean(EvaluationContext ctx) {
		Object obj = evaluateObject(ctx);
		if(obj == null || (obj instanceof Number && ((Number) obj).intValue() == 0)) {
			return false;
		}
		return true;
	}
	
	public Object evaluateObject(SqlColumnType type, EvaluationContext obj) {
		Object o = eval(ectx, obj);
		if (o == null) {
			return null;
		}
		if (type == SqlColumnType.INT) {
			if(o instanceof JsonPrimitive) {
				return ((JsonPrimitive) o).getAsLong();
			}
			if(o instanceof Number) {
				return ((Number) o).longValue();
			}
			return Long.parseLong(o.toString());
		}
		if (type == SqlColumnType.TIMESTAMP) {
			String s = o.toString();
			if(o instanceof JsonPrimitive) {
				if(((JsonPrimitive) o).isNumber()) {
					return new Date(((Number) o).longValue());
				}
				s = ((JsonPrimitive) o).getAsString();
			}
			if(o instanceof Number) {
				return new Date(((Number) o).longValue());
			}
			try {
				return OpBlock.dateFormat.parse(s);
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
		if(o instanceof JsonPrimitive) {
			if(((JsonPrimitive) o).isString()) {
				return  ((JsonPrimitive) o).getAsString();
			}
		}
		return o.toString();
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

	private Object callFunction(String functionName, List<Object> args, EvaluationContext ctx) {
		Number n1, n2;
		Object obj1, obj2;
		switch (functionName) {
		case FUNCTION_M_PLUS:
			long l1 = getLongArgument(functionName, args, 0);
			long l2 = getLongArgument(functionName, args, 1);
			return l1 + l2;
		case FUNCTION_BLC_FIND:
			if(args.size() > 3) {
				throw new UnsupportedOperationException("blc:find Not supported multiple args yet");
			} else if(args.size() == 3) {
				return ctx.op.getObjectByName(getStringArgument(functionName, args, 0), 
						getStringArgument(functionName, args, 1), getStringArgument(functionName, args, 2));
			} else if(args.size() == 2) {
				return ctx.op.getObjectByName(getStringArgument(functionName, args, 0), 
						getStringArgument(functionName, args, 1));
			} else {
				throw new UnsupportedOperationException("blc:find not enough arguments");
			}
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
		case FUNCTION_STD_EQ:
			obj1 = getObjArgument(functionName, args, 0);
			obj2 = getObjArgument(functionName, args, 1);
			return OUtils.equals(obj1, obj2) ? 1 : 0;
		case FUNCTION_STD_NEQ:
			obj1 = getObjArgument(functionName, args, 0);
			obj2 = getObjArgument(functionName, args, 1);
			return OUtils.equals(obj1, obj2) ? 0 : 1;
		case FUNCTION_STD_LEQ:
			n1 = (Number) getObjArgument(functionName, args, 0);
			n2 = (Number) getObjArgument(functionName, args, 0);
			return n1.doubleValue() <= n2.doubleValue() ? 1 : 0;
		case FUNCTION_STD_LE:
			n1 = (Number) getObjArgument(functionName, args, 0);
			n2 = (Number) getObjArgument(functionName, args, 0);
			return n1.doubleValue() < n2.doubleValue() ? 1 : 0;
		case FUNCTION_STD_SIZE:
			Object ob = getObjArgument(functionName, args, 0);
			if(ob instanceof JsonArray) {
				return ((JsonArray) ob).size();
			} else if(ob instanceof JsonObject) {
				return ((JsonObject) ob).size();
			}
			return 1;
		default:
			break;
		}
		throw new UnsupportedOperationException(String.format("Unsupported function '%s'", functionName));
	}

	

	private String getStringArgument(String functionName, List<Object> args, int i) {
		validateSize(functionName, args, i);
		Object o = args.get(i);
		return o == null ? null : o.toString();
	}
	
	
	private Object getObjArgument(String functionName, List<Object> args, int i) {
		validateSize(functionName, args, i);
		 Object obj= args.get(i);
		 if(obj instanceof JsonArray) {
			 if(((JsonArray) obj).size() == 1) {
				 obj = ((JsonArray) obj).get(0);
			 }
		 }
		 if(obj instanceof JsonPrimitive) {
			 if(((JsonPrimitive) obj).isNumber()) {
				 return ((JsonPrimitive) obj).getAsNumber();
			 } else {
				 return ((JsonPrimitive) obj).getAsString();
			 }
		 }
		 return obj;
	}

	private void validateSize(String functionName, List<Object> args, int i) {
		if (i >= args.size()) {
			throw new UnsupportedOperationException(String.format("Not enough arguments for function '%s'",
					functionName));
		}
	}

	private long getLongArgument(String functionName, List<Object> args, int i) {
		Object o = getObjArgument(functionName, args, i);
		return o == null ? 0 : Long.parseLong(o.toString());
	}
	
	

	private Object eval(ExpressionContext expr, EvaluationContext ctx) {
		ParseTree child = expr.getChild(0);
		if (child instanceof TerminalNode) {
			TerminalNode t = ((TerminalNode) child);
			if (t.getSymbol().getType() == OpenDBExprParser.INT) {
				return Long.parseLong(t.getText());
			} else if (t.getSymbol().getType() == OpenDBExprParser.THIS) {
				return ctx.ctx;
			} else if (t.getSymbol().getType() == OpenDBExprParser.DOT) {
				Object obj = ctx.ctx;
//				System.out.println(expr.getChild(0).getText());
//				System.out.println(expr.getText());
				for(int i = 1; i < expr.getChildCount(); i++) {
					obj = unwrap(((JsonObject)obj).get(expr.getChild(i).getText()));
				}
				return obj;
//				return unwrap(ctx.ctx.get(expr.getChild(1).getText()));
			} else if (t.getSymbol().getType() == OpenDBExprParser.STRING_LITERAL1) {
				return t.getText().substring(1, t.getText().length() - 1).replace("\\\'", "\'");
			} else if (t.getSymbol().getType() == OpenDBExprParser.STRING_LITERAL2) {
				return t.getText().substring(1, t.getText().length() - 1).replace("\\\"", "\"");
			}
			throw new UnsupportedOperationException("Terminal node is not supported");
		}
		if (child instanceof ExpressionContext && ((TerminalNode)expr.getChild(1)).getSymbol().getType() == OpenDBExprLexer.DOT ) {
			ExpressionContext fc = ((ExpressionContext) child);
			String field = expr.getChild(2).getText();
			Object eval = eval(fc, ctx);
			if(eval instanceof JsonObject) {
				return unwrap(((JsonObject) eval).get(field));
			}
			return null;
		}
		if (child instanceof MethodCallContext) {
			MethodCallContext mcc = ((MethodCallContext) child);
			String functionName = mcc.getChild(0).getText();
			List<Object> args = new ArrayList<Object>();
			System.out.println(functionName + " ");
			for (int i = 0; i < mcc.getChildCount(); i++) {
				ParseTree pt = mcc.getChild(i);
				System.out.println(" -> " + pt.getText());
				if(pt instanceof ExpressionContext){
					Object obj = eval((ExpressionContext) pt, ctx);
					System.out.println(obj);
					args.add(obj);
				}
			}
			return callFunction(functionName, args, ctx);
		}
		throw new UnsupportedOperationException("Unsupported parser operation: %s" + child.getText());
	}


	private Object unwrap(JsonElement j) {
		if(j instanceof JsonPrimitive) {
			if(((JsonPrimitive) j).isBoolean()) {
				return ((JsonPrimitive) j).getAsBoolean();
			}
			if(((JsonPrimitive) j).isString()) {
				return ((JsonPrimitive) j).getAsString();
			}
			if(((JsonPrimitive) j).isNumber()) {
				return ((JsonPrimitive) j).getAsNumber();
			}
		}
		return j;
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
