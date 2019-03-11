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

	public static final String FUNCTION_STR_FIRST = "str:first";
	public static final String FUNCTION_STR_SECOND = "str:second";
	public static final String FUNCTION_M_PLUS = "m:plus";
	public static final String FUNCTION_M_MULT = "m:mult";
	public static final String FUNCTION_M_DIV = "m:div";
	public static final String FUNCTION_M_MINUS = "m:minus";
	
	public static final String FUNCTION_STD_EQ = "std:eq";
	public static final String FUNCTION_STD_NEQ = "std:neq";
	public static final String FUNCTION_STD_LEQ = "std:leq";
	public static final String FUNCTION_STD_LE = "std:le";
	public static final String FUNCTION_STD_SIZE = "std:size";
	
	public static final String FUNCTION_BLC_FIND = "blc:find";
	
	public static final String FUNCTION_AUTH_HAS_SIG_ROLES = "auth:has_sig_roles";
	public static final String FUNCTION_AUTH_HAS_SIG_USER = "auth:has_sig_user";
	
	
	public static boolean TRACE_EXPRESSIONS = false;
	
	
	private ExpressionContext ectx;

	public static class EvaluationContext {
		private JsonElement ctx;
		private OpBlockChain op;
		private int exprNested;

		public EvaluationContext(OpBlockChain blockchain, JsonObject ctx, 
				JsonElement deleted, JsonObject refs) {
			ctx.add("ref", refs);
			ctx.add("old", deleted);
			this.op = blockchain;
			this.ctx = ctx;
		}

	}

	private SimpleExprEvaluator(ExpressionContext ectx) {
		this.ectx = ectx;

	}

	public Object evaluateObject(EvaluationContext obj) {
		return eval(ectx, obj);
	}
	
	public boolean evaluateBoolean(EvaluationContext ctx) {
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
	
	public static SimpleExprEvaluator parseExpression(String value) throws RecognitionException {
		OpenDBExprLexer lexer = new OpenDBExprLexer(new ANTLRInputStream(value));
		ThrowingErrorListener twt = new ThrowingErrorListener(value);
		lexer.removeErrorListeners();
		lexer.addErrorListener(twt);
		OpenDBExprParser parser = new OpenDBExprParser(new CommonTokenStream(lexer));
		parser.removeErrorListeners();
		parser.addErrorListener(twt);
		ExpressionContext ectx = parser.expression();
		return new SimpleExprEvaluator(ectx);
	}

	private Object callFunction(String functionName, List<Object> args, EvaluationContext ctx) {
		Number n1, n2;
		Object obj1, obj2;
		switch (functionName) {
		case FUNCTION_M_MULT:
			n1 = (Number) getObjArgument(functionName, args, 0);
			n2 = (Number) getObjArgument(functionName, args, 1);
			if(n1.doubleValue() == Math.ceil(n1.doubleValue()) && 
					n2.doubleValue() == Math.ceil(n2.doubleValue())) {
				return n1.longValue() * n2.longValue();
			}
			return n1.doubleValue() * n2.doubleValue();
		case FUNCTION_M_DIV:
			n1 = (Number) getObjArgument(functionName, args, 0);
			n2 = (Number) getObjArgument(functionName, args, 1);
			if(n1.doubleValue() == Math.ceil(n1.doubleValue()) && 
					n2.doubleValue() == Math.ceil(n2.doubleValue())) {
				if(n2.longValue() == 0) {
					if(n1.longValue() == 0) {
						return Double.NaN;
					} else if(n1.longValue() > 0) {
						return Double.POSITIVE_INFINITY;
					} else {
						return Double.NEGATIVE_INFINITY;
					}
				}
				return n1.longValue() / n2.longValue();
			}
			return n1.doubleValue() / n2.doubleValue();
		case FUNCTION_M_PLUS:
			n1 = (Number) getObjArgument(functionName, args, 0);
			n2 = (Number) getObjArgument(functionName, args, 1);
			if(n1.doubleValue() == Math.ceil(n1.doubleValue()) && 
					n2.doubleValue() == Math.ceil(n2.doubleValue())) {
				return n1.longValue() + n2.longValue();
			}
			return n1.doubleValue() + n2.doubleValue();
		case FUNCTION_M_MINUS:
			n1 = (Number) getObjArgument(functionName, args, 0);
			n2 = (Number) getObjArgument(functionName, args, 1);
			if(n1.doubleValue() == Math.ceil(n1.doubleValue()) && 
					n2.doubleValue() == Math.ceil(n2.doubleValue())) {
				return n1.longValue() - n2.longValue();
			}
			return n1.doubleValue() - n2.doubleValue();
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
			if(obj1 instanceof Number && obj2 instanceof Number) {
				n1 = (Number) obj1;
				n2 = (Number) obj2;
				if(n1.doubleValue() == Math.ceil(n1.doubleValue()) && 
						n2.doubleValue() == Math.ceil(n2.doubleValue())) {
					return n1.longValue() == n2.longValue() ? 1 : 0;
				}	
				return n1.doubleValue() == n2.doubleValue() ? 1 : 0;
			}
			return OUtils.equals(obj1, obj2) ? 1 : 0;
		case FUNCTION_STD_NEQ:
			obj1 = getObjArgument(functionName, args, 0);
			obj2 = getObjArgument(functionName, args, 1);
			if(obj1 instanceof Number && obj2 instanceof Number) {
				n1 = (Number) obj1;
				n2 = (Number) obj2;
				if(n1.doubleValue() == Math.ceil(n1.doubleValue()) && 
						n2.doubleValue() == Math.ceil(n2.doubleValue())) {
					return n1.longValue() == n2.longValue() ? 0 : 1;
				}	
				return n1.doubleValue() == n2.doubleValue() ? 0 : 1;
			}
			return OUtils.equals(obj1, obj2) ? 0 : 1;
		case FUNCTION_STD_LEQ:
			n1 = (Number) getObjArgument(functionName, args, 0);
			n2 = (Number) getObjArgument(functionName, args, 1);
			if(n1.doubleValue() == Math.ceil(n1.doubleValue()) && 
					n2.doubleValue() == Math.ceil(n2.doubleValue())) {
				return n1.longValue() <= n2.longValue() ? 1 : 0;		
			}
			return n1.doubleValue() <= n2.doubleValue() ? 1 : 0;
		case FUNCTION_STD_LE:
			n1 = (Number) getObjArgument(functionName, args, 0);
			n2 = (Number) getObjArgument(functionName, args, 1);
			if(n1.doubleValue() == Math.ceil(n1.doubleValue()) && 
					n2.doubleValue() == Math.ceil(n2.doubleValue())) {
				return n1.longValue() < n2.longValue();
			}
			return n1.doubleValue() < n2.doubleValue() ? 1 : 0;
		case FUNCTION_STD_SIZE:
			Object ob = getObjArgument(functionName, args, 0, false);
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
		Object o = getObjArgument(functionName, args, i);
		return o == null ? null : o.toString();
	}
	
	private Object getObjArgument(String functionName, List<Object> args, int i) {
		return getObjArgument(functionName, args, i, true);
	}
	
	private Object getObjArgument(String functionName, List<Object> args, int i, boolean expandSingleArray) {
		validateSize(functionName, args, i);
		 Object obj= args.get(i);
		 if(obj instanceof JsonArray && expandSingleArray) {
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


	private Object eval(ExpressionContext expr, EvaluationContext ctx) {
		ParseTree child = expr.getChild(0);
		if (child instanceof TerminalNode) {
			TerminalNode t = ((TerminalNode) child);
			if (t.getSymbol().getType() == OpenDBExprParser.INT) {
				return Long.parseLong(t.getText());
			} else if (t.getSymbol().getType() == OpenDBExprParser.THIS) {
				return ctx.ctx;
			} else if (t.getSymbol().getType() == OpenDBExprParser.DOT) {
				String field = expr.getChild(1).getText();
				return getField(ctx.ctx, field);
				
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
			return getField(eval, field);
		}
		if (child instanceof MethodCallContext) {
			MethodCallContext mcc = ((MethodCallContext) child);
			String functionName = mcc.getChild(0).getText();
			List<Object> args = new ArrayList<Object>();
			StringBuilder traceExpr = null;
			if(TRACE_EXPRESSIONS) {
				traceExpr = new StringBuilder();
				traceExpr.append(space(ctx.exprNested)).append(functionName);
			}
			for (int i = 0; i < mcc.getChildCount(); i++) {
				ParseTree pt = mcc.getChild(i);
				if(pt instanceof ExpressionContext){
					Object obj = eval((ExpressionContext) pt, ctx);
					if(TRACE_EXPRESSIONS) {
						traceExpr.append("[ '").append(pt.getText()).append("'");
						traceExpr.append(" -> '").append(obj).append("']");
					}
					args.add(obj);
				}
			}
			ctx.exprNested++;
			Object funcRes = callFunction(functionName, args, ctx);
			if(TRACE_EXPRESSIONS) {
				System.out.println("EXPR:  " + traceExpr.toString() + " = " + funcRes);
			}
			ctx.exprNested--;
			return funcRes;
		}
		throw new UnsupportedOperationException("Unsupported parser operation: %s" + child.getText());
	}


	private Object getField(Object obj, String field) {
		if(obj instanceof JsonArray) {
			JsonArray ar = (JsonArray) obj;
			try {
				int nt = Integer.parseInt(field);
				if(nt < ar.size() && nt >= 0) {
					return unwrap(ar.get(nt));
				}
				return null;
			} catch (NumberFormatException e) {
			}
			if(ar.size() > 0 && ar.get(0) instanceof JsonObject) {
				return unwrap(((JsonObject) ar.get(0)).get(field));
			}
			return null;
		} else if(obj instanceof JsonObject) {
			return unwrap(((JsonObject) obj).get(field));
		}
		return null;
	}

	private String space(int exprNested) {
		String s = "";
		for(int i = 0; i < exprNested; i++) {
			s+= "  ";
		}
		return s;
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
