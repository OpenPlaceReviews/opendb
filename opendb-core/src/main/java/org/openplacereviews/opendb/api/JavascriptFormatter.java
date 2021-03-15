package org.openplacereviews.opendb.api;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.util.OUtils;

public class JavascriptFormatter {

	public static String genSysOperationJsFunction(StringBuilder s, OpObject op) {
		String idName = op == null ? "sys_op_operation" : "op_" + op.getId().get(0).replace('.', '_');
		String className = OUtils.capitalizeFirstLetter(idName);
		s.append("class " + className + (op == null ? " " : " extends Sys_op_operation ") + "{\n");
		if (op == null) {
			genJSFunction(s, "Abstract", "getBaseName");
			genJSFunction(s, Collections.singletonList("return this.getBaseName(); "), "getName");
			genJSFunction(s, Collections.singletonList("return this.getBaseName() + 's'; "), "getPluralName");
			genJSFunction(s, Collections.singletonList("return ''; "), "getIcon");
			genJSFunction(s, Collections.singletonList("return this.getBaseName() + ' ' + obj.id; "), "getObjName", "obj");
			genJSFunction(s, Collections.singletonList("return this.getIcon(); "), "getObjectIcon", "obj");
			genJSFunction(s, Collections.singletonList("return this.getBaseName() + ' ' + obj.id + (obj.userdetails ? ' details: ' + JSON.stringify(obj.userdetails) : ''); "), "getObjDescription", "obj");
			genJSFunction(s, Collections.singletonList("return this.getBaseName(); "), "getOpName", "op");
			genJSFunction(s, Collections.singletonList("return this.getBaseName(); "), "getOpDescription", "op");
		} else {
			s.append("    static key() { return '" + op.getId().get(0) + "'; } \n ");
			String idPart = op.getId().get(0);
			idPart = OUtils.capitalizeFirstLetter(idPart.substring(idPart.lastIndexOf('.') + 1));
			genJSFunction(s, idPart, "getBaseName");
			genJSFunction(s, op.getFieldByExpr("description.name"), "getName");
			genJSFunction(s, op.getFieldByExpr("description.plural-name"), "getPluralName");
			genJSFunction(s, op.getFieldByExpr("description.operation-icon"), "getIcon");
			genJSFunction(s, op.getFieldByExpr("description.object-name-format"), "getObjName", "obj");
			genJSFunction(s, op.getFieldByExpr("description.object-icon"), "getObjectIcon", "obj");
			genJSFunction(s, op.getFieldByExpr("description.object-description-format"), "getObjDescription", "obj");
			genJSFunction(s, op.getFieldByExpr("description.operation-name-format"), "getOpName", "op");
			genJSFunction(s, op.getFieldByExpr("description.operation-format"), "getOpDescription", "op");

		}

		s.append("}");
		return className;
	}

	private static void genJSFunction(StringBuilder s, Object val, String funcName, String... params) {
		if (val == null) {
			return;
		}
		String tab = "    ";
		s.append("\n").append(tab).append(funcName + "(");
		for (int i = 0; i < params.length; i++) {
			if (i > 0) {
				s.append(", ");
			}
			s.append(params[i]);
		}
		s.append(") { \n");
		if (val instanceof String) {
			s.append(tab).append("\treturn '" + val + "';\n");
		} else if (val instanceof List) {
			Iterator<?> it = ((List<?>) (val)).iterator();
			while (it.hasNext()) {
				s.append(tab).append(tab).append(it.next().toString()).append("\n");
			}
		} else {
			s.append(tab).append("\treturn 'TODO ?????';\n");
		}
		s.append(tab).append("}\n");

	}

}
