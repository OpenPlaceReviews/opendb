package org.openplacereviews.opendb.util;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.openplacereviews.opendb.expr.OpenDBExprLexer;
import org.openplacereviews.opendb.expr.OpenDBExprParser;
import org.openplacereviews.opendb.expr.OpenDBExprParser.ExpressionContext;
import org.openplacereviews.opendb.expr.OpenDBExprParser.MethodCallContext;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpBlockchainRules;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.OpOperation;

import java.util.*;

import static org.openplacereviews.opendb.ops.OpObject.F_CHANGE;
import static org.openplacereviews.opendb.ops.OpOperation.*;

public class OpExprEvaluator {

	public static final String FUNCTION_STR_FIRST = "str:first";
	public static final String FUNCTION_STR_SECOND = "str:second";
	public static final String FUNCTION_STR_ALL = "str:all";
	public static final String FUNCTION_STR_COMBINE = "str:combine";
	public static final String FUNCTION_STR_CONCAT = "str:concat";

	public static final String FUNCTION_M_PLUS = "m:plus";
	public static final String FUNCTION_M_MULT = "m:mult";
	public static final String FUNCTION_M_DIV = "m:div";
	public static final String FUNCTION_M_MINUS = "m:minus";
	public static final String FUNCTION_M_FIELDS_INT_SUM =  "m:fields_int_sum";

	public static final String FUNCTION_STD_EQ = "std:eq";
	public static final String FUNCTION_STD_NEQ = "std:neq";
	public static final String FUNCTION_STD_LEQ = "std:leq";
	public static final String FUNCTION_STD_LE = "std:le";
	public static final String FUNCTION_STD_SIZE = "std:size";
	public static final String FUNCTION_STD_OR = "std:or";
	public static final String FUNCTION_STD_AND = "std:and";

	public static final String FUNCTION_SET_IN = "set:in";
	public static final String FUNCTION_SET_ALL = "set:all";
	public static final String FUNCTION_SET_MINUS = "set:minus";
	public static final String FUNCTION_SET_CONTAINS_KEY = "set:contains_key";

	public static final String FUNCTION_AUTH_HAS_SIG_ROLES = "auth:has_sig_roles";
	
	public static final String FUNCTION_OP_OPERATION_TYPE = "op:op_type";
	public static final String FUNCTION_OP_GET_OBJECT_BY_FIELD = "op:obj_get";
	public static final String FUNCTION_OP_FIELDS_CHANGED = "op:fields_changed";

	
	public static final String F_NEW = "new";
	public static final String F_OLD = "old";
	public static final String F_REF = "ref";

	public static boolean TRACE_EXPRESSIONS = false;

	private ExpressionContext ectx;

	public static class EvaluationContext {
		private JsonElement ctx;
		private OpBlockChain blc;
		private int exprNested;

		public EvaluationContext(OpBlockChain blockchain, JsonObject ctx, JsonElement createdElement, JsonElement deleted, JsonObject refs) {
			ctx.add(F_REF, refs);
			ctx.add(F_OLD, deleted);
			ctx.add(F_NEW, createdElement);
			this.blc = blockchain;
			this.ctx = ctx;
		}

	}

	public OpExprEvaluator(ExpressionContext ectx) {
		this.ectx = ectx;
	}

	public ExpressionContext getEctx() {
		return this.ectx;
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

	protected Object callFunction(String functionName, List<Object> args, EvaluationContext ctx) {
		Number n1, n2;
		Object obj1, obj2;
		JsonObject object;
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
		case FUNCTION_STR_FIRST:
		case FUNCTION_STR_SECOND:
		case FUNCTION_STR_ALL:
			String ffs = getStringArgument(functionName, args, 0);
			if (ffs != null) {
				int indexOf = ffs.indexOf(':');
				if (indexOf != -1) {
					return functionName.equals(FUNCTION_STR_ALL) ? ffs : functionName.equals(FUNCTION_STR_FIRST) ? ffs.substring(0, indexOf) : ffs
							.substring(indexOf + 1);
				}
			}
			return ffs;
		case FUNCTION_STR_CONCAT: {
			String res = getStringObject(getObjArgument(functionName, args, 0, false));
			for(int i = 1 ; i <args.size(); i++) {
				res += getStringObject(getObjArgument(functionName, args, i, false));
			}
			return res;
		}
		case FUNCTION_STR_COMBINE:
			obj1 = getObjArgument(functionName, args, 0, false);
			String s1 = getStringArgument(functionName, args, 1);
			String res = "";
			if (!isJsonArrayObj(obj1)) {
				return obj1;
			} else {
				JsonArray ar1 = (JsonArray) obj1;
				for(int i = 0; i < ar1.size(); i++) {
					if(i > 0) {
						res += s1;
					}
					JsonElement e1 = ar1.get(i);
					res += e1 != null && e1.isJsonPrimitive() ? 
							((JsonPrimitive)e1).getAsString() : toStringPrimitive(e1); 
				}
			}
			return res;
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
				return n1.longValue() < n2.longValue() ? 1 : 0;
			}
			return n1.doubleValue() < n2.doubleValue() ? 1 : 0;
		case FUNCTION_STD_SIZE:
			Object ob = getObjArgument(functionName, args, 0, false);
			if (ob instanceof JsonArray) {
				return ((JsonArray) ob).size();
			} else if (ob instanceof JsonObject) {
				return ((JsonObject) ob).size();
			}
			return ob == null ? 0 : 1;
		case FUNCTION_STD_AND:
			for (Object o : args) {
				if (o == null) {
					return 0;
				} else if (o instanceof Number) {
					if (((Number) o).doubleValue() == 0) {
						return 0;
					}
				} else if (o instanceof String) {
					if (((String) o).length() == 0) {
						return 0;
					}
				}
			}
			return 1;
		case FUNCTION_STD_OR:
			for (Object o : args) {
				if (o == null) {
					continue;
				} else if (o instanceof Number) {
					if (((Number) o).doubleValue() != 0) {
						return 1;
					}
				} else if (o instanceof String) {
					if (((String) o).length() > 0) {
						return 1;
					}
				}
			}
			return 0;
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
		case FUNCTION_OP_FIELDS_CHANGED:
			obj1 = getObjArgument(functionName, args, 0, false);
			if (!(obj1 instanceof JsonObject)) {
				throw new UnsupportedOperationException(FUNCTION_OP_FIELDS_CHANGED + " support only JsonObject");
			}
			object = ((JsonObject) obj1);
			if (object.get(F_EDIT) == null) {
				throw new UnsupportedOperationException(FUNCTION_OP_FIELDS_CHANGED + " must to contains edit list");
			}

			JsonArray objList = (JsonArray) object.get(F_EDIT);
			JsonArray arrayChangedFields = new JsonArray();
			for (JsonElement o : objList) {
				JsonObject changedMap =  o.getAsJsonObject().get(F_CHANGE).getAsJsonObject();

				for (Map.Entry<String, JsonElement> e : changedMap.entrySet()) {
					String fieldExpr = e.getKey();
					Object op = e.getValue();
					if (op instanceof JsonObject) {
						for(Map.Entry<String, JsonElement> ee : ((JsonObject) op).entrySet()) {
							if(ee.getKey().equals(OpBlockChain.OP_CHANGE_APPEND)) {
								arrayChangedFields.add(fieldExpr);
							} else if(ee.getKey().equals(OpBlockChain.OP_CHANGE_SET)) {
								arrayChangedFields.add(fieldExpr);
							} else {
								throw new UnsupportedOperationException();
							}
						}
					} else {
						if(OpBlockChain.OP_CHANGE_INCREMENT.equals(op)) {
							arrayChangedFields.add(fieldExpr);
						} else if(OpBlockChain.OP_CHANGE_DELETE.equals(op)) {
							arrayChangedFields.add(fieldExpr);
						} else {
							throw new UnsupportedOperationException();
						}
					}
				}
			}
			return arrayChangedFields;
		case FUNCTION_OP_OPERATION_TYPE:
			obj1 = getObjArgument(functionName, args, 0, false);
			if (!(obj1 instanceof JsonObject)) {
				throw new UnsupportedOperationException(FUNCTION_OP_OPERATION_TYPE + " support only JsonObject");
			}
			object = ((JsonObject) obj1);
			if (object.get(F_EDIT) != null) {
				return F_EDIT;
			}
			if (object.get(F_CREATE) != null) {
				return F_CREATE;
			}
			if (object.get(F_DELETE) != null) {
				return F_DELETE;
			}
			throw new UnsupportedOperationException(FUNCTION_OP_OPERATION_TYPE + " op doesn't have any ops type");

		case FUNCTION_OP_GET_OBJECT_BY_FIELD:
			obj1 = getObjArgument(functionName, args, 0, false);
			Object obj = obj1;
			for (int i = 1; i < args.size(); i++) {
				if (!(obj instanceof JsonObject)) {
					throw new UnsupportedOperationException(FUNCTION_OP_GET_OBJECT_BY_FIELD + " support only JsonObject");
				}
				obj = getField(obj, getStringObject(args.get(i)));
			}
			return obj;
		case FUNCTION_M_FIELDS_INT_SUM:
			obj1 = getObjArgument(functionName, args, 0, false);
			obj2 = getObjArgument(functionName, args, 1, false);
			if (!(obj1 instanceof JsonObject)) {
				throw new UnsupportedOperationException(FUNCTION_M_FIELDS_INT_SUM + " support only JsonObject");
			}
			int sum = 0;
			object = ((JsonObject) obj1).get(getStringObject(obj2)).getAsJsonObject();
			for (Map.Entry<String, JsonElement> e : object.entrySet()) {
				sum += e.getValue().getAsInt();
			}
			return sum;
		case FUNCTION_SET_CONTAINS_KEY:
			obj1 = getObjArgument(functionName, args, 0, false);
			obj2 = getObjArgument(functionName, args, 1, false);
			Set<String> refKey = ((JsonObject) obj1).keySet();
			if (!refKey.contains(String.valueOf(obj2))) {
				return 0;
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
			} else if (isJsonArrayObj(obj2)) {
				JsonArray j2 = ((JsonArray) obj2);
				for (int i = 0; i < j2.size(); i++) {
					obj1Set.remove(toStringPrimitive(j2.get(i)));
				}
			} else {
				obj1Set.remove(toStringPrimitive(obj2));
			}

			JsonArray ar = new JsonArray(obj1Set.size());
			for (String s : obj1Set) {
				if (!s.equals("")) {
					ar.add(s);
				}
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
					if (o != null) {
						arrayRes.add((String) o);
					}
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


	private String getStringObject(Object obj2) {
		StringBuilder str = new StringBuilder();
		if (obj2 instanceof String) {
			str = new StringBuilder((String) obj2);
		} else if (isJsonArrayObj(obj2)) {
			JsonArray array = (JsonArray) obj2;
			for (int i = 0; i < array.size(); i++) {
				if (str.toString().equals("")) {
					str.append(toStringPrimitive(array.get(i).getAsString()));
				} else {
					str.append(":").append(toStringPrimitive(array.get(i).getAsString()));
				}
			}
		}
		return str.toString();
	}

	private boolean checkSignaturesHasRole(String sign, String roleToCheck, EvaluationContext ctx) {
		OpObject grantObj = ctx.blc.getObjectByName(OpBlockchainRules.OP_GRANT, sign);
		if(grantObj == null) {
			int indexOf = sign.indexOf(':');
			if (indexOf != -1) {
				grantObj = ctx.blc.getObjectByName(OpBlockchainRules.OP_GRANT, sign.substring(0, indexOf));
			}
		}
		if (grantObj != null) {
			Map<String, Set<String>> roleToChildRoles = ctx.blc.getRules().getRoles(ctx.blc);
			List<String> grantedRoles = grantObj.getStringList("roles");
			for (String grantedRole : grantedRoles) {
				if (OUtils.equals(grantedRole, roleToCheck)) {
					return true;
				}
				Set<String> derivedRoles = roleToChildRoles.get(grantedRole);
				if(derivedRoles != null && derivedRoles.contains(roleToCheck)) {
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
		Number n1 = null;
		Number n2 = null;
		if (obj1 instanceof Number) {
			n1 = (Number) obj1;
		}
		if (obj2 instanceof Number) {
			n2 = (Number) obj2;
		}
		if (obj1 instanceof JsonPrimitive && ((JsonPrimitive) obj1).isNumber()) {
			n1 = ((JsonPrimitive) obj1).getAsNumber();
		}
		if (obj2 instanceof JsonPrimitive && ((JsonPrimitive) obj2).isNumber()) {
			n2 = ((JsonPrimitive) obj2).getAsNumber();
		}
		
		if (n1 != null && n2 != null) {
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

	protected String getStringArgument(String functionName, List<Object> args, int i) {
		Object o = getObjArgument(functionName, args, i);
		return o == null ? null : o.toString();
	}

	private Object getObjArgument(String functionName, List<Object> args, int i) {
		return getObjArgument(functionName, args, i, true);
	}

	protected Object getObjArgument(String functionName, List<Object> args, int i, boolean expandSingleArray) {
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

	protected Object eval(ExpressionContext expr, EvaluationContext ctx) {
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
			Object funcRes;
			try {
				funcRes = callFunction(functionName, args, ctx);
			} catch (UnsupportedDataTypeException e) {
				throw new UnsupportedOperationException(e);
			}

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
