package org.openplacereviews.opendb.api;

import static org.openplacereviews.opendb.ops.OpObject.F_CHANGE;
import static org.openplacereviews.opendb.ops.OpObject.F_CURRENT;
import static org.openplacereviews.opendb.ops.OpObject.F_ID;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.openplacereviews.opendb.api.ApiController.ObjectsResult;
import org.openplacereviews.opendb.ops.OpBlockChain;
import org.openplacereviews.opendb.ops.OpObject;
import org.openplacereviews.opendb.ops.OpOperation;
import org.openplacereviews.opendb.util.JsonFormatter;
import org.openplacereviews.opendb.util.OUtils;

import com.google.gson.JsonSyntaxException;

public class PatchReplicatedIncorrectObjects {
	public static final String F_SOURCE = "source";
	public static final String F_OLD_OSM_IDS = "old-osm-ids";
	public static final String F_OSM = "osm";
	public static final String F_TAGS = "tags";
	public static final String F_OSM_TAG = "osm_tag";
	public static final String F_OSM_VALUE = "osm_value";

	public static void main(String[] args) throws JsonSyntaxException, IOException {
		
//		scanCompareObjects("https://openplacereviews.org/", "https://r2.openplacereviews.org/", 12705, 12800);
		compareObjects(DOUBLE_CHECK_OBJECTS_TO_PATCH, "https://openplacereviews.org/", "https://r2.openplacereviews.org/");
		compareObjects(DISCOVERED_OBJECTS_TO_PATCH, "https://openplacereviews.org/", "https://r2.openplacereviews.org/");
//		generateEditTouchPatchOperation("https://openplacereviews.org/", DOUBLE_CHECK_OBJECTS_TO_PATCH, "version", 1);
		
		
//		generateEditTouchPatchOperation("https://openplacereviews.org/", DISCOVERED_OBJECTS_TO_PATCH, "source.osm[0].changeset", null);
//		generateEditSwapPatchOperation("https://r2.openplacereviews.org", DISCOVERED_OBJECTS_TO_PATCH);
	}
	
	private static void compareObjects(String[][] ids, String host1, String host2 )
			throws JsonSyntaxException, IOException {
		JsonFormatter fmt = new JsonFormatter();
		for (String[] objId : ids) {
			OpObject obj1 = loadObject(host1, fmt, objId);
			OpObject obj2 = loadObject(host2, fmt, objId);
//			System.out.println(fmt.objToJson(obj1));
//			System.out.println(fmt.objToJson(obj2));
			boolean equal = fmt.objToJson(obj1).equals(fmt.objToJson(obj2));
			if (equal) {
				System.out.println(Arrays.toString(objId) + " - OK. ");
			} else {
				OpObject editObject = new OpObject();
				editObject.putObjectValue(F_ID, obj1.getId());
				
				Map<String, Object> changeTagMap = new TreeMap<>();
				Map<String, Object> currentTagMap = new TreeMap<>();
				generateDiff(editObject, "", changeTagMap, currentTagMap, obj1.getAllFields(), 
						obj2.getAllFields());
				System.out.println(changeTagMap);
				System.out.println(Arrays.toString(objId) + " - FAILED. ");
			}
		}
	}

	@SuppressWarnings("unchecked")
	protected static void scanCompareObjects(String host1, String host2, int blockStart, int blockEnd)
			throws JsonSyntaxException, IOException {
		JsonFormatter fmt = new JsonFormatter();
		List<String> failedObjects = new ArrayList<>();
		for (int blockId = blockStart; blockId <= blockEnd; blockId++) {
			System.out.println("\n\n\n>>> BLOCK " + blockId);
			URL u = new URL(host1 + "api/objects-from-edit-op-by-block-id?blockId=" + blockId);
//			URL u = new URL(host1 + "api/edited-objects-by-block?type=opr.place&blockId=" + blockId);
			List<String> objs = fmt.fromJson(new InputStreamReader(u.openStream()), List.class);
			for (String objId : objs) {
				OpObject obj1 = loadObject(host1, fmt, objId.split(","));
				OpObject obj2 = loadObject(host2, fmt, objId.split(","));
				boolean equal = fmt.objToJson(obj1).equals(fmt.objToJson(obj2));
				if (equal) {
					System.out.println(objId + " - OK. ");
				} else {
					failedObjects.add(objId);
					System.out.println(objId + " - FAILED. ");
					System.err.println("!!!! " + objId + " - FAILED. ");
				}
			}
		}
		System.out.println("FAILED: " + failedObjects);
		// URL u1 = new URL("https://r2.openplacereviews.org/api/objects-by-id?type=opr.place&key=" + id[0] + "," +id[1]);
		// URL u2 = new URL("https://r2.openplacereviews.org/api/objects-by-id?type=opr.place&key=" + id[0] + "," + id[1]);

	}
	
	protected static void generateEditTouchPatchOperation(String host, String[][] ids, String touchField, Object def)
			throws MalformedURLException, IOException {
		JsonFormatter fmt = new JsonFormatter();
		OpOperation op = null;
		for (String[] id : ids) {
			OpObject obj = loadObject(host, fmt, id);
			if (op == null) {
				op = new OpOperation();
				op.setType("opr.place");
			}
			OpObjectDiffBuilder bld = new OpObjectDiffBuilder(obj);
			if (obj.getFieldByExpr(touchField) == null && def != null) {
				bld.setNewTag(touchField, def);
			} else {
				bld.setNewTag(touchField, obj.getFieldByExpr(touchField));
			}
			bld.add(op);
		}
		System.out.println(fmt.fullObjectToJson(op));
	}

	@SuppressWarnings("unchecked")
	protected static void generateEditSwapPatchOperation(String host, String[][] ids) throws MalformedURLException, IOException {
		JsonFormatter fmt = new JsonFormatter();
		OpOperation op = null;
		for (String[] id : ids) {
			OpObject obj = loadObject(host, fmt, id);
			if (op == null) {
				op = new OpOperation();
				op.setType("opr.place");
			}
			
			List<Map<String, Object>> osms = obj.getField(null, F_SOURCE, F_OSM);
			if (osms.size() > 2) {
				// 3 Manually patched
				//57V4WV,ejpw65 + 
				//855C82,4jvkkd +
				//86FVW5,lbktwz +
				//8F9Q37,wb8iyn +
				//8FVGJP,qclld9 +
				//9F22RF,afuaay +
				System.out.println(id[0] + "," + id[1] + " !!!!! " + osms.size());
				continue;
			}
			OpObject editObject = new OpObject();
			editObject.putObjectValue(F_ID, obj.getId());
			
			Map<String, Object> changeTagMap = new TreeMap<>();
			Map<String, Object> currentTagMap = new TreeMap<>();
			
			String field = F_SOURCE + "." + F_OSM + "[" + 0 + "].";
			Map<String, Object> osm1 = new TreeMap<String, Object>(osms.get(0));
			Map<String, Object> osm2 = new TreeMap<String, Object>(osms.get(1));
			generateDiff(editObject, field + F_TAGS + ".", changeTagMap, currentTagMap,
					(Map<String, Object>) osm1.remove(F_TAGS), (Map<String, Object>) osm2.remove(F_TAGS));
			generateDiff(editObject, field, changeTagMap, currentTagMap, osm1, osm2);
			
			
			field = F_SOURCE + "." + F_OSM + "[" + 1 + "].";
			osm1 = new TreeMap<String, Object>(osms.get(0));
			osm2 = new TreeMap<String, Object>(osms.get(1));
			generateDiff(editObject, field + F_TAGS + ".", changeTagMap, currentTagMap,
					(Map<String, Object>) osm2.remove(F_TAGS), (Map<String, Object>) osm1.remove(F_TAGS));
			generateDiff(editObject, field, changeTagMap, currentTagMap, osm2, osm1);
			
			if (!changeTagMap.isEmpty()) {
				editObject.putObjectValue(F_CHANGE, changeTagMap);
				editObject.putObjectValue(F_CURRENT, currentTagMap);
				op.addEdited(editObject);
			}			
			if (op.getEdited().size() > 30) {
				System.out.println(fmt.fullObjectToJson(op));
				op = new OpOperation();
				op.setType("opr.place");
			}
		}
		System.out.println(fmt.fullObjectToJson(op));
	}

	private static OpObject loadObject(String host, JsonFormatter fmt, String[] id) throws MalformedURLException, IOException {
		URL u = new URL(host + "api/objects-by-id?type=opr.place&key=" + id[0] + "," + id[1]);
		ObjectsResult objs = fmt.fromJson(new InputStreamReader(u.openStream()), ObjectsResult.class);
		if (objs.objects.isEmpty()) {
			return null;
		}
		OpObject obj = objs.objects.iterator().next();
		return obj;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected static void generateDiff(OpObject editObject, String field, Map<String, Object> change,
			Map<String, Object> current, Map<String, Object> oldM, Map<String, Object> newM) {
		TreeSet<String> removedTags = new TreeSet<>(oldM.keySet());
		removedTags.removeAll(newM.keySet());
		for (String removedTag : removedTags) {
			change.put(field + addQuotes(removedTag), OpBlockChain.OP_CHANGE_DELETE);
			current.put(field + addQuotes(removedTag), oldM.get(removedTag));
		}
		for (String tag : newM.keySet()) {
			Object fOld = oldM.get(tag);
			Object fNew = newM.get(tag);
			if (fOld instanceof Map && fNew instanceof Map) {
				generateDiff(editObject, field + tag + ".", change, current, (Map) fOld, (Map) fNew);
			} else if(fOld instanceof List && fNew instanceof List) {
				List<Map<String, Object>> lOld = (List<Map<String, Object>>) fOld;
				List<Map<String, Object>> lNew = (List<Map<String, Object>>) fNew;
				int i = 0;
				for(; i < lOld.size() && i < lNew.size(); i++ ) {
					if(lOld.get(i) instanceof Map && lNew.get(i) instanceof Map) {
						generateDiff(editObject, field.substring(0, field.length() - 1) + 
								"["+i+"].", change, current, lOld.get(i), lNew.get(i));
					} else if (!OUtils.equals(lOld.get(i), lNew.get(i))) {
						throw new UnsupportedOperationException();
					}
				}
				for (; i < lOld.size(); ) {
					throw new UnsupportedOperationException();
				}
				for (; i < lNew.size(); ) {
					throw new UnsupportedOperationException();
				}
				
			} else if (!OUtils.equals(fOld, fNew)) {
				change.put(field + addQuotes(tag), set(fNew));
				if (fOld != null) {
					current.put(field + addQuotes(tag), fOld);
				}
			}
		}
	}
	
	private static String addQuotes(String field) {
		if (field.contains(".") || field.contains("[") || field.contains("]")) {
			field = field.replaceAll("\\[", "\\\\[");
			field = field.replaceAll("\\]", "\\\\]");
			field = "{" + field + "}";
		}

		return field;
	}
	private static Object diffSet(Object vl) {
		Map<String, Object> set = new TreeMap<String, Object>();
		set.put(OpBlockChain.OP_CHANGE_SET, vl);
		return set;
	}
	
	public static Object set(Object vl) {
		Map<String, Object> set = new TreeMap<String, Object>();
		set.put(OpBlockChain.OP_CHANGE_SET, vl);
		return set;
	}
	
	
	public static class OpObjectDiffBuilder {

		final OpObject obj;
		final TreeMap<String, Object> changeTagMap = new TreeMap<>();
		final TreeMap<String, Object> currentTagMap = new TreeMap<>();

		public OpObjectDiffBuilder(OpObject obj) {
			this.obj = obj;
		}

		public void setNewTag(String tag, Object value) {
			// not suitable for increment
			Object o = obj.getFieldByExpr(tag);
//			if (!OUtils.equals(o, value)) {
				changeTagMap.put(tag, diffSet(value));
				if (o != null) {
					currentTagMap.put(tag, o);
				}
//			}
		}

		public void add(OpOperation op) {
			OpObject editObj = new OpObject();
			for (String s : obj.getId()) {
				editObj.addOrSetStringValue(OpObject.F_ID, s);
			}
			editObj.putObjectValue(OpObject.F_CHANGE, changeTagMap);
			editObj.putObjectValue(OpObject.F_CURRENT, currentTagMap);
			op.addEdited(editObj);
		}
	}
	
	public static String[][] DOUBLE_CHECK_OBJECTS_TO_PATCH = new String[][] {
		{"4RJ768","b2pr1l"},
		{"76H3X2","uqbg6o"},
		{"76VVWF","mxxkqs"},
		{"87C4WJ","gcy45v"},
		{"87G8Q2","akhleh"},
		{"8CGV6W","2lmmhu"},
		{"8CMV67","zczljq"},
		{"8FF5H9","facwtr"},
		{"8FRP4X","yigaoc"},
		{"8FV29M","6unsca"},
		{"8FXR5J","5wki0b"},
		{"8GG4JX","umfw24"},
		{"8GG4JX","wnvabs"},
		{"8GG4JX","zqgpgg"},
		{"8H46J2","i5so3d"},
		{"8Q336F","gsi6ce"},
		{"9F25J3","zdrmpw"},
		{"9F2PV3","427sui"},
		{"9GPF4G","66zbs8"}
	};
	
	public static String[][] DISCOVERED_OBJECTS_TO_PATCH = new String[][] {
		{"47G9PQ","biuobd"},
		{"47G9PQ","obc0sf"},
		{"47G9PQ","rffxyf"},
		{"48Q37H","e7hvzs"},
		{"4RJ768","c9zchv"},
		{"4RJ768","fqazsa"},
		{"4VCMFV","1iqoyb"},
		{"57V4WV","ejpw65"},
		{"584HC9","whzvhx"},
		{"588PX9","hj9fcz"},
		{"589P74","pv9oog"},
		{"5F9VC3","uwywvo"},
		{"66WXGM","rttxid"},
		{"67X24J","elydnj"},
		{"6QW7XG","sassdu"},
		{"75HW2P","a90mcf"},
		{"765GPR","nsoa6i"},
		{"765GPR","xz2wcs"},
		{"849VQH","ke13nm"},
		{"84CWHG","ijk7xp"},
		{"84CWHG","ydx2ar"},
		{"84VVGJ","sjepuf"},
		{"8559CX","ghvp1d"},
		{"8559CX","zoxq0z"},
		{"855C82","4jvkkd"},
		{"855CC2","ur7xer"},
		{"85FQC4","jvoahj"},
		{"85FQG4","8dg6zh"},
		{"862QCM","zy6nzx"},
		{"862QVQ","lhcplb"},
		{"862QVR","yugkdv"},
		{"86FMWX","ugsxps"},
		{"86FMXX","sfcxih"},
		{"86FVW5","lbktwz"},
		{"86G3HF","genwhd"},
		{"86MM7J","4lsjus"},
		{"86X4RV","fe51iv"},
		{"87C4WJ","jzovqv"},
		{"87G6JG","mggmv1"},
		{"87JFX3","5uyzwo"},
		{"87M824","9glozv"},
		{"87M824","gtem3r"},
		{"87MFGH","8e3o1u"},
		{"87P2Q4","eche0o"},
		{"87P84R","j5fbji"},
		{"87P96F","bcnvbf"},
		{"87Q5P9","goncor"},
		{"8CMPC4","glf5rj"},
		{"8CMWMW","gpdu3j"},
		{"8CPWPQ","mb7t2u"},
		{"8F9Q37","wb8iyn"},
		{"8FF4MH","0en9gd"},
		{"8FF4MH","64bixe"},
		{"8FF4MH","asgqae"},
		{"8FF4MH","u6okur"},
		{"8FH494","5kyvlk"},
		{"8FH5Q2","5jptj9"},
		{"8FHJVF","sthbio"},
		{"8FJ47X","nwp3gk"},
		{"8FJW7V","wpm7tj"},
		{"8FMHQ7","akfhif"},
		{"8FMHQ7","dldtmk"},
		{"8FPGJW","ugxped"},
		{"8FQGGH","kp4lms"},
		{"8FR86Q","4lqpo4"},
		{"8FR86Q","fwm0fn"},
		{"8FR9JM","bwhkny"},
		{"8FR9W9","cibtfj"},
		{"8FRHPX","mlki0o"},
		{"8FRRFX","tezhuw"},
		{"8FVFC8","slru4k"},
		{"8FVGG8","3vm7za"},
		{"8FVGJP","qclld9"},
		{"8FVJJ8","njmzrr"},
		{"8FVJV4","yqmjls"},
		{"8FVMR3","azu9xb"},
		{"8FVQ3C","52degk"},
		{"8FVXF3","ttulln"},
		{"8FVXG3","tgppye"},
		{"8FW4V9","dh3l57"},
		{"8FW97P","ejhxaq"},
		{"8FW97P","whsjnx"},
		{"8FWFG3","gvweyq"},
		{"8FWFG3","k0ji1y"},
		{"8FWFQ5","tocur6"},
		{"8FWMP9","gslide"},
		{"8FWPR8","9u0oh7"},
		{"8FWR59","2jrru1"},
		{"8FWV6F","b5uges"},
		{"8FXGVH","nqpd0x"},
		{"8FXHQ5","qr6kjo"},
		{"8G6MP4","vx9v8y"},
		{"8GC26J","ill4qq"},
		{"8GG4HX","x9njoh"},
		{"8GG4JX","hqlvj8"},
		{"8GG4JX","zqgpgg"},
		{"8GHC2X","k3bgcf"},
		{"8GHC2X","osmwhy"},
		{"8GHC2X","zwyeds"},
		{"8GJ2M7","ssqswo"},
		{"8GM2W6","4cf2ww"},
		{"8GP8C3","gd1qnj"},
		{"8H4HJM","aumsob"},
		{"8PFRV9","jqerqc"},
		{"8PFRV9","qpmzue"},
		{"8Q2274","gohpdd"},
		{"8Q7XGC","yckmxj"},
		{"8Q7XHP","lxlvbw"},
		{"8Q7XPQ","wsu2cn"},
		{"8Q88P8","nmwsjw"},
		{"8RH2QP","hfnfto"},
		{"9C2XRV","ajplbf"},
		{"9C3RFR","fsrcbg"},
		{"9C3VMQ","oueyyo"},
		{"9C3VPQ","aclpzp"},
		{"9C3VR9","8rclbw"},
		{"9C4WC3","54d3de"},
		{"9C4WC3","i8f9yz"},
		{"9C4WC3","jxlva5"},
		{"9C4WCF","ovbtwn"},
		{"9C4WH2","05puq2"},
		{"9C4WH2","jr7ljm"},
		{"9C5M9P","qqdvp5"},
		{"9C5M9P","yyeugk"},
		{"9C5R9R","atutqd"},
		{"9C5R9R","oziwny"},
		{"9C5R9R","zojnhe"},
		{"9C5VC4","weqgga"},
		{"9C5WJ4","whakvk"},
		{"9C7RWR","ghrfui"},
		{"9C8PCG","ov8rbj"},
		{"9F22RF","afuaay"},
		{"9F22RF","z2qqof"},
		{"9F235J","iccy7b"},
		{"9F25J3","dzuxzh"},
		{"9F26CC","jedhh7"},
		{"9F28Q3","zkpwic"},
		{"9F2GMW","xxqjmk"},
		{"9F2HWH","8ryb4l"},
		{"9F2HWH","kk2p4t"},
		{"9F2P3C","1au82b"},
		{"9F2VC4","6zuadb"},
		{"9F2VF5","asekcq"},
		{"9F2VF8","lzgcw4"},
		{"9F35PQ","8eqpbj"},
		{"9F366X","zlq94h"},
		{"9F383V","8m0alx"},
		{"9F383V","qfkid3"},
		{"9F383V","xqrbcv"},
		{"9F3855","hvqcch"},
		{"9F3952","ajz1xt"},
		{"9F3CPP","oqbwwj"},
		{"9F3MGM","owkuwm"},
		{"9F43H7","8wi6jq"},
		{"9F4MFC","gccxgb"},
		{"9F5893","2wbpq3"},
		{"9F5FHX","asumzx"},
		{"9F5FHX","fcwsje"},
		{"9F6C8J","cckwff"},
		{"9F6G48","1o5ok0"},
		{"9F6G48","ovgbrj"},
		{"9F6W9J","kucgxp"},
		{"9F7JPH","cr6wkx"},
		{"9F7JWR","0pfm8i"},
		{"9F7MQ4","wrlcd0"},
		{"9FFGWQ","9xq65z"},
		{"9G233M","sudc8n"},
		{"9G2GCG","l1vpfp"},
		{"9G2GCG","y75v6n"},
		{"9G4362","55ie2b"},
		{"9G4363","tennli"},
		{"9G752H","9hqsxg"},
		{"9GFGP3","fsf1k9"},
		{"9H5C7X","yepzog"},
		{"9H7FQ4","gr95on"},
		{"9RJQH9","mbhxq8"}};
}
