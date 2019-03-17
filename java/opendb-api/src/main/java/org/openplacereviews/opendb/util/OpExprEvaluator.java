package org.openplacereviews.opendb.util;

import java.sql.Date;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

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
import org.openplacereviews.opendb.ops.OpBlockchainRules;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.service.DBDataManager.SqlColumnType;
import org.postgresql.util.PGobject;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class OpExprEvaluator {

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

	public static final String FUNCTION_SET_IN = "set:in";
	public static final String FUNCTION_SET_ALL = "set:all";
	public static final String FUNCTION_SET_MINUS = "set:minus";

	public static final String FUNCTION_AUTH_HAS_SIG_ROLES = "auth:has_sig_roles";

	public static final String FUNCTION_BLC_FIND = "blc:find";

	public static boolean TRACE_EXPRESSIONS = false;

	private ExpressionContext ectx;

	public static class EvaluationContext {
		private JsonElement ctx;
		private OpBlockChain op;
		private int exprNested;

		public EvaluationContext(OpBlockChain blockchain, JsonObject ctx, JsonElement deleted, JsonObject refs) {
			ctx.add("ref", refs);
			ctx.add("old", deleted);
			this.op = blockchain;
			this.ctx = ctx;
		}

	}

	private OpExprEvaluator(ExpressionContext ectx) {
		this.ectx = ectx;

	}

	public Object evaluateObject(EvaluationContext obj) {
		return eval(ectx, obj);
	}

	public boolean evaluateBoolean(EvaluationContext ctx) {
		Object obj = evaluateObject(ctx);
		if (obj == null || (obj instanceof Number && ((Number) obj).intValue() == 0)) {
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
			if (o instanceof JsonPrimitive) {
				return ((JsonPrimitive) o).getAsLong();
			}
			if (o instanceof Number) {
				return ((Number) o).longValue();
			}
			return Long.parseLong(o.toString());
		}
		if (type == SqlColumnType.TIMESTAMP) {
			String s = o.toString();
			if (o instanceof JsonPrimitive) {
				if (((JsonPrimitive) o).isNumber()) {
					return new Date(((Number) o).longValue());
				}
				s = ((JsonPrimitive) o).getAsString();
			}
			if (o instanceof Number) {
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
		if (o instanceof JsonPrimitive) {
			if (((JsonPrimitive) o).isString()) {
				return ((JsonPrimitive) o).getAsString();
			}
		}
		return o.toString();
	}

	public static OpExprEvaluator parseExpression(String value) throws RecognitionException {
		OpenDBExprLexer lexer = new OpenDBExprLexer(new ANTLRInputStream(value));
		ThrowingErrorListener twt = new ThrowingErrorListener(value);
		lexer.removeErrorListeners();
		lexer.addErrorListener(twt);
		OpenDBExprParser parser = new OpenDBExprParser(new CommonTokenStream(lexer));
		parser.removeErrorListeners();
		parser.addErrorListener(twt);
		ExpressionContext ectx = parser.expression();
		return new OpExprEvaluator(ectx);
	}

	private Object callFunction(String functionName, List<Object> args, EvaluationContext ctx) {
		Number n1, n2;
		Object obj1, obj2;
		switch (functionName) {
		case FUNCTION_M_MULT:
			n1 = (Number) getObjArgument(functionName, args, 0);
			n2 = (Number) getObjArgument(functionName, args, 1);
			if (n1.doubleValue() == Math.ceil(n1.doubleValue()) && n2.doubleValue() == Math.ceil(n2.doubleValue())) {
				return n1.longValue() * n2.longValue();
			}
			return n1.doubleValue() * n2.doubleValue();
		case FUNCTION_M_DIV:
			n1 = (Number) getObjArgument(functionName, args, 0);
			n2 = (Number) getObjArgument(functionName, args, 1);
			if (n1.doubleValue() == Math.ceil(n1.doubleValue()) && n2.doubleValue() == Math.ceil(n2.doubleValue())) {
				if (n2.longValue() == 0) {
					if (n1.longValue() == 0) {
						return Double.NaN;
					} else if (n1.longValue() > 0) {
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
			if (n1.doubleValue() == Math.ceil(n1.doubleValue()) && n2.doubleValue() == Math.ceil(n2.doubleValue())) {
				return n1.longValue() + n2.longValue();
			}
			return n1.doubleValue() + n2.doubleValue();
		case FUNCTION_M_MINUS:
			n1 = (Number) getObjArgument(functionName, args, 0);
			n2 = (Number) getObjArgument(functionName, args, 1);
			if (n1.doubleValue() == Math.ceil(n1.doubleValue()) && n2.doubleValue() == Math.ceil(n2.doubleValue())) {
				return n1.longValue() - n2.longValue();
			}
			return n1.doubleValue() - n2.doubleValue();
		case FUNCTION_BLC_FIND:
			if (args.size() > 3) {
				throw new UnsupportedOperationException("blc:find Not supported multiple args yet");
			} else if (args.size() == 3) {
				return ctx.op.getObjectByName(getStringArgument(functionName, args, 0),
						getStringArgument(functionName, args, 1), getStringArgument(functionName, args, 2));
			} else if (args.size() == 2) {
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
			return objEquals(obj1, obj2);
		case FUNCTION_STD_NEQ:
			obj1 = getObjArgument(functionName, args, 0);
			obj2 = getObjArgument(functionName, args, 1);
			int r = objEquals(obj1, obj2);
			return r == 0 ? 1 : 0;
		case FUNCTION_STD_LEQ:
			n1 = (Number) getObjArgument(functionName, args, 0);
			n2 = (Number) getObjArgument(functionName, args, 1);
			if (n1.doubleValue() == Math.ceil(n1.doubleValue()) && n2.doubleValue() == Math.ceil(n2.doubleValue())) {
				return n1.longValue() <= n2.longValue() ? 1 : 0;
			}
			return n1.doubleValue() <= n2.doubleValue() ? 1 : 0;
		case FUNCTION_STD_LE:
			n1 = (Number) getObjArgument(functionName, args, 0);
			n2 = (Number) getObjArgument(functionName, args, 1);
			if (n1.doubleValue() == Math.ceil(n1.doubleValue()) && n2.doubleValue() == Math.ceil(n2.doubleValue())) {
				return n1.longValue() < n2.longValue();
			}
			return n1.doubleValue() < n2.doubleValue() ? 1 : 0;
		case FUNCTION_STD_SIZE:
			Object ob = getObjArgument(functionName, args, 0, false);
			if (ob instanceof JsonArray) {
				return ((JsonArray) ob).size();
			} else if (ob instanceof JsonObject) {
				return ((JsonObject) ob).size();
			}
			return 1;
		case FUNCTION_AUTH_HAS_SIG_ROLES:
			Object opSigned = getObjArgument(functionName, args, 0, false);
			Object checkRoles = getObjArgument(functionName, args, 1, false);
			List<String> signedBy, roles;
			if (opSigned instanceof JsonObject) {
				JsonElement elm = ((JsonObject) opSigned).get(OpOperation.F_SIGNED_BY);
				signedBy = getStringsList(elm);
			} else {
				return 0;
			}
			if(checkRoles instanceof JsonElement) {
				roles = getStringsList((JsonElement) checkRoles);
			} else if(checkRoles instanceof String) {
				roles = Collections.singletonList(checkRoles.toString());
			} else {
				return 0;
			}
			for(String rl : roles) {
				boolean oneSigHasRole = false;
				for(String sign : signedBy) {
					if(checkSignaturesHasRole(sign, rl, ctx)) {
						oneSigHasRole = true;
						break;
					}
				}
				if(!oneSigHasRole) {
					return 0;
				}
			}

			return 1;
		case FUNCTION_SET_MINUS:
			obj1 = getObjArgument(functionName, args, 0, false);
			obj2 = getObjArgument(functionName, args, 1, false);
			Set<String> obj1Set = new TreeSet<String>();
			if (isJsonMapObj(obj1)) {
				obj1Set.addAll(((JsonObject) obj1).keySet());
			} else if (isJsonArrayObj(obj1)) {
				JsonArray j1 = ((JsonArray) obj1);
				for (int i = 0; i < j1.size(); i++) {
					obj1Set.add(toStringPrimitive(j1.get(i)));
				}
			} else {
				obj1Set.add(toStringPrimitive(obj1));
			}

			if (isJsonMapObj(obj2)) {
				obj1Set.removeAll(((JsonObject) obj1).keySet());
			} else if (isJsonArrayObj(obj1)) {
				JsonArray j2 = ((JsonArray) obj2);
				for (int i = 0; i < j2.size(); i++) {
					obj1Set.remove(toStringPrimitive(j2.get(i)));
				}
			} else {
				obj1Set.remove(toStringPrimitive(obj2));
			}

			JsonArray ar = new JsonArray(obj1Set.size());
			for (String s : obj1Set) {
				ar.add(s);
			}
			return ar;
		case FUNCTION_SET_ALL:
			JsonArray arrayRes = new JsonArray();
			for (Object o : args) {
				if (o instanceof JsonElement) {
					arrayRes.add((JsonElement) o);
				} else if (o instanceof Number) {
					arrayRes.add((Number) o);
				} else {
					arrayRes.add((String) o);
				}
			}
			return arrayRes;
		case FUNCTION_SET_IN:
			obj1 = getObjArgument(functionName, args, 0, false);
			obj2 = getObjArgument(functionName, args, 1, false);
			if (!isJsonArrayObj(obj1) && !isJsonMapObj(obj1)) {
				if (obj2 instanceof JsonArray) {
					JsonArray j2 = ((JsonArray) obj2);
					for (int i = 0; i < j2.size(); i++) {
						if (objEquals(obj1, j2.get(i)) != 0) {
							return 1;
						}
					}
				} else if (obj2 instanceof JsonObject) {
					JsonObject j2 = ((JsonObject) obj2);
					for (String key : j2.keySet()) {
						if (objEquals(obj1, key) != 0) {
							return 1;
						}
					}
				}
				return 0;
			} else {
				if (obj2 instanceof JsonArray) {
					JsonArray j2 = ((JsonArray) obj2);
					for (int i = 0; i < j2.size(); i++) {
						if (objEquals(obj1, j2.get(i)) != 0) {
							return 1;
						}
					}
				}
				return objEquals(obj1, obj2);
			}
		default:
			break;
		}
		throw new UnsupportedOperationException(String.format("Unsupported function '%s'", functionName));
	}

	private boolean checkSignaturesHasRole(String sign, String rl, EvaluationContext ctx) {
		OpObject grantObj = ctx.op.getObjectByName(OpBlockchainRules.OP_GRANT, sign);
		if(grantObj == null) {
			int indexOf = sign.indexOf(':');
			if (indexOf != -1) {
				grantObj = ctx.op.getObjectByName(OpBlockchainRules.OP_GRANT, sign.substring(0, indexOf));
			}
		}
		if (grantObj != null) {
			List<String> rls = grantObj.getStringList("roles");
			// TODO here we need to check super roles
			for (String r : rls) {
				if (OUtils.equals(r, rl)) {
					return true;
				}
			}
		}
		return false;
	}

	private List<String> getStringsList(JsonElement elm) {
		List<String> lst = new ArrayList<String>();
		if (elm.isJsonPrimitive()) {
			if (((JsonPrimitive) elm).isString()) {
				lst.add(((JsonPrimitive) elm).getAsString());
			}
		} else if (elm.isJsonArray()) {
			JsonArray jsar = (JsonArray) elm;
			for (int i = 0; i < jsar.size(); i++) {
				lst.add(jsar.get(i).getAsString());
			}
		}
		return lst;
	}

	private String toStringPrimitive(Object o) {
		return o == null ? "" : o.toString();
	}

	private int objEquals(Object obj1, Object obj2) {
		Number n1;
		Number n2;
		if (obj1 instanceof Number && obj2 instanceof Number) {
			n1 = (Number) obj1;
			n2 = (Number) obj2;
			if (n1.doubleValue() == Math.ceil(n1.doubleValue()) && n2.doubleValue() == Math.ceil(n2.doubleValue())) {
				return n1.longValue() == n2.longValue() ? 1 : 0;
			}
			return n1.doubleValue() == n2.doubleValue() ? 1 : 0;
		}
		if (obj1 instanceof JsonPrimitive) {
			obj1 = ((JsonPrimitive) obj1).getAsString();
		}
		if (obj2 instanceof JsonPrimitive) {
			obj2 = ((JsonPrimitive) obj2).getAsString();
		}
		return OUtils.equals(obj1, obj2) ? 1 : 0;
	}

	public boolean isJsonMapObj(Object o) {
		return o instanceof JsonObject;
	}

	public boolean isJsonArrayObj(Object o) {
		return o instanceof JsonArray;
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
		Object obj = args.get(i);
		if (obj instanceof JsonArray && expandSingleArray) {
			if (((JsonArray) obj).size() == 1) {
				obj = ((JsonArray) obj).get(0);
			}
		}
		if (obj instanceof JsonPrimitive) {
			if (((JsonPrimitive) obj).isNumber()) {
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
		if (child instanceof ExpressionContext
				&& ((TerminalNode) expr.getChild(1)).getSymbol().getType() == OpenDBExprLexer.DOT) {
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
			if (TRACE_EXPRESSIONS) {
				traceExpr = new StringBuilder();
				traceExpr.append(space(ctx.exprNested)).append(functionName);
			}
			for (int i = 0; i < mcc.getChildCount(); i++) {
				ParseTree pt = mcc.getChild(i);
				if (pt instanceof ExpressionContext) {
					Object obj = eval((ExpressionContext) pt, ctx);
					if (TRACE_EXPRESSIONS) {
						traceExpr.append("[ '").append(pt.getText()).append("'");
						traceExpr.append(" -> '").append(obj).append("']");
					}
					args.add(obj);
				}
			}
			ctx.exprNested++;
			Object funcRes = callFunction(functionName, args, ctx);
			if (TRACE_EXPRESSIONS) {
				System.out.println("EXPR:  " + traceExpr.toString() + " = " + funcRes);
			}
			ctx.exprNested--;
			return funcRes;
		}
		throw new UnsupportedOperationException("Unsupported parser operation: %s" + child.getText());
	}

	private Object getField(Object obj, String field) {
		if (obj instanceof JsonArray) {
			JsonArray ar = (JsonArray) obj;
			try {
				int nt = Integer.parseInt(field);
				if (nt < ar.size() && nt >= 0) {
					return unwrap(ar.get(nt));
				}
				return null;
			} catch (NumberFormatException e) {
			}
			if (ar.size() > 0 && ar.get(0) instanceof JsonObject) {
				return unwrap(((JsonObject) ar.get(0)).get(field));
			}
			return null;
		} else if (obj instanceof JsonObject) {
			return unwrap(((JsonObject) obj).get(field));
		} else if ("0".equals(field)) {
			return obj;
		}
		return null;
	}

	private String space(int exprNested) {
		String s = "";
		for (int i = 0; i < exprNested; i++) {
			s += "  ";
		}
		return s;
	}

	private Object unwrap(JsonElement j) {
		if (j instanceof JsonPrimitive) {
			if (((JsonPrimitive) j).isBoolean()) {
				return ((JsonPrimitive) j).getAsBoolean();
			}
			if (((JsonPrimitive) j).isString()) {
				return ((JsonPrimitive) j).getAsString();
			}
			if (((JsonPrimitive) j).isNumber()) {
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
