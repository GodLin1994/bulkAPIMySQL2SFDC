package com.cg.zcript;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import com.cg.zcript.FileUtil;

public class DataChunkConverter {

	private Map<String, String> targetField2sourceField = new LinkedHashMap<>();
	private Map<String, Function<String, String>> targetField2customFunction = new HashMap<>();
	private Map<String, Function<Map<String, String>, String>> targetField2customFunctionWithContext = new HashMap<>();
	public static final String OPERATION_SKIP_LINE = "OPERATION_SKIP_LINE";
	public static final String OPERATION_USE_SOURCE_FIELD_CONTEXT = "OPERATION_USE_SOURCE_FIELD_CONTEXT";

	public static void main(String[] args) throws FileNotFoundException, IOException, ParserConfigurationException, SAXException {
//		try (InputStream is = new FileInputStream("./src/data/crm021Data_20180425135959.csv")){
//			List<List<String>> lists = CSVHelper.readListsFromCSV(is);
//			ListsConverter lc = new ListsConverter();
//			lc.map("Name", m->m.getOrDefault("ebeln", "")+"/"+m.getOrDefault("ebelp", ""));
//			lc.map("Remark__c", "text1Zh");
//			lc.map("Sales_Order__c", "ebelnVbap");
//			lc.map("Sales_Order_Line_Item__c", "ebelpVbap");
//			lc.map("Account_Name__c", "name1Kna1");
//			lc.map("Contract_Number__c", "zarchive");
//			lc.map("Sales_Office__c", "vkbur");
//			lc.map("Urgency__c", "prioUrg");
//			lc.map("Employee_Number__c", "pernr", v->Integer.valueOf(v)+"");
//			lc.map("SalesName__c", "pernrName");
//			System.out.println(lc.convert(lists));
//		}

		List<List<String>> sourceLists = new ArrayList<>();
		sourceLists.add(Arrays.asList(new String[]{"source1", "source2", "source3", "source4"}));
		sourceLists.add(Arrays.asList(new String[]{"a", "甲", "A", "001"}));
		sourceLists.add(Arrays.asList(new String[]{"b", "", "B", "002"}));
		sourceLists.add(Arrays.asList(new String[]{"c", "丙", "C", "003"}));
		sourceLists.add(Arrays.asList(new String[]{"d", "丁", "D", "004"}));
		sourceLists.add(Arrays.asList(new String[]{"e", "戊", "E", "005"}));
		
		DataChunkConverter lc = new DataChunkConverter();
		System.out.println("sourceData:"+sourceLists);
		lc.map("target1", "source1")//普通mapping
		  .map("target2", "source4", v->Integer.valueOf(v)+"")//mapping后用lumbda表达式定制化value
		  .map("target3", m->m.get("source3")+"/"+m.get("source2"))//targetField对应多个sourceField组合
		  .map("target4", "source1", v->"c".equals(v)?DataChunkConverter.OPERATION_SKIP_LINE:v)//跳行处理
		  .map("target5", m->"constant");//常量
		
		System.out.println("targetData1:"+lc.listsToLists(sourceLists));
		System.out.println("targetData2:"+lc.listsToMaps(sourceLists));
		
		DataChunkConverter dcc2 = new DataChunkConverter();
		dcc2.map("target10", "target1");//普通mapping
		dcc2.map("target20", "target2", v->v+"123");//mapping后用lumbda表达式定制化value
		dcc2.map("target30", m->m.get("target3")+"/"+m.get("target4"));//targetField对应多个sourceField组合
		dcc2.map("target40", "target1", v->"e".equals(v)?DataChunkConverter.OPERATION_SKIP_LINE:v);//跳行处理
		dcc2.map("target50", m->"constant中文");//常量
		System.out.println("targetData3:"+dcc2.mapsToMaps(lc.listsToMaps(sourceLists)));

		

		String jsonStr = FileUtil.getStringFromFileLocation("./src/string/payload.txt");
		System.out.println("jsonStr:"+jsonStr);
		Gson gson = new Gson();
		List<Map<String, String>> maps = gson.fromJson(jsonStr, new TypeToken<List<Map<String, String>>>(){}.getType());
//		System.out.println(maps);
		System.out.println(DataChunkConverter.sameMapsToLists(maps));
		
		DataChunkConverter dcc21 = new DataChunkConverter();
		dcc21.map("target1", "originalProvisioningName");//普通mapping
		dcc21.map("target2", "unit", v->v+"123");//mapping后用lumbda表达式定制化value
		dcc21.map("target3", m->m.get("addon")+"/"+m.get("name"));//targetField对应多个sourceField组合
		dcc21.map("target4", "originalProvisioningName", v->"ResellerToCustomerSupport".equals(v)?DataChunkConverter.OPERATION_SKIP_LINE:v);//跳行处理
		dcc21.map("target5", m->"constant中文");//常量
		System.out.println(dcc21.mapsToMaps(maps));
		
		DataChunkConverter lc2 = new DataChunkConverter();
		lc2.map("target1", "Auart");//普通mapping
		lc2.map("target2", "Matnr", v->Long.valueOf(v)+"123");//mapping后用lumbda表达式定制化value
		lc2.map("target3", sourceField2value->sourceField2value.get("Atwrt")+"/"+sourceField2value.get("Lkotr"));//targetField对应多个sourceField组合
		lc2.map("target4", "Atnam", v->"SC_1168_00_1100".equals(v)?DataChunkConverter.OPERATION_SKIP_LINE:v);//跳行处理
		lc2.map("target5", m->"constant");//常量

		System.out.println("========xml==================================");
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = dbf.newDocumentBuilder();
		try (InputStream is = DataChunkConverter.class.getResourceAsStream("crm015response.xml")){
			Document d = db.parse(is);
//			String soap = "http://schemas.xmlsoap.org/soap/envelope/";
//			String ns2 = "http://soap.ws.crm.kion.com/";
//			NodeList childNodes = d.getDocumentElement().getElementsByTagNameNS(soap, "Body");
			Node childNode = d.getDocumentElement().getFirstChild().getNextSibling();
			NodeList childNodes2 = childNode.getFirstChild().getNextSibling().getFirstChild().getNextSibling().getChildNodes();
			Node DocRt = childNodes2.item(27);
			System.out.println(lc2.mapsToLists(DataChunkConverter.sameXmlNodeListToMaps(DocRt.getChildNodes())));
		}
		
		
		
		
		
		
		
		
		
	}

	/**
	 * 建立从源字段到目标字段的映射关系
	 * @param targetField 目标字段名
	 * @param customFunctionWithSourceFieldContext 含有源字段数据上下文map的定制化的转换函数
	 * @return a reference to this object.
	 */
	public DataChunkConverter map(String targetField, Function<Map<String, String>, String> customFunctionWithSourceFieldContext){
		targetField2sourceField.put(targetField, OPERATION_USE_SOURCE_FIELD_CONTEXT);
		targetField2customFunctionWithContext.put(targetField, customFunctionWithSourceFieldContext);
		return this;
	}
	
	/**
	 * 建立从源字段到目标字段的映射关系
	 * @param targetField 目标字段名
	 * @param sourceField 源字段名
	 * @param customFunction 定制化的转换函数
	 * @return a reference to this object.
	 */
	public DataChunkConverter map(String targetField, String sourceField, Function<String, String> customFunction){
		targetField2sourceField.put(targetField, sourceField);
		if (customFunction!=null){
			targetField2customFunction.put(targetField, customFunction);
		}
		return this;
	}
	
	/**
	 * 建立从源字段到目标字段的映射关系
	 * @param targetField 目标字段名
	 * @param sourceField 源字段名
	 * @return a reference to this object.
	 */
	public DataChunkConverter map(String targetField, String sourceField){
		return map(targetField, sourceField, null);
	}
	
	private static int getStackTraceIndexSuggested(Throwable t){
		int stackTraceIndexSuggested;
		if (t.getStackTrace().length>=3){
			//定位到lumbda表达式的定义中
			stackTraceIndexSuggested = t.getStackTrace().length-3;
		}else{
			stackTraceIndexSuggested = 0;
		}
		return stackTraceIndexSuggested;
	}

	/**
	 * 将源数据按映射关系转换为目标数据
	 * @param sourceLists 源数据（第一行为列名的二维list）
	 * @return 目标数据（第一行为列名的二维list）
	 */
	public List<List<String>> listsToLists(List<List<String>> sourceLists){
//		if (targetField2sourceField.isEmpty()){
//			//没有配置转换map，进行默认转换
//			//构造sourceField到targetField的DataChunkConverter map
//			if (sourceLists==null||sourceLists.isEmpty()) {
//				//表头信息缺失
//				return null;
//			}
//			DataChunkConverter dcc = new DataChunkConverter();
//			for (String sourceField:sourceLists.get(0)) {
//				dcc.map(sourceField, sourceField);
//			}
//			return dcc.convertListsToLists(sourceLists);
//		}
		
		List<List<String>> targetLists = new ArrayList<>();
		//处理header
		if (sourceLists==null||sourceLists.isEmpty()) {
			return null;
		}
		List<String> headers = sourceLists.get(0);
		List<String> targetFields = new ArrayList<>();
		Map<String, Integer> sourceField2index = new HashMap<>();
		for (int i=0;i<headers.size();i++){
			String header = headers.get(i);
			sourceField2index.put(header, i);
		}
		for (String targetField:targetField2sourceField.keySet()){
			targetFields.add(targetField);
		}
		targetLists.add(targetFields);
		//处理data
		i:for (int i = 1; i < sourceLists.size(); i++) {
			List<String> list = sourceLists.get(i);
			try {
				List<String> targetList = new ArrayList<>();
				for (Map.Entry<String, String> entry : targetField2sourceField.entrySet()) {
					String targetField = entry.getKey();
					String sourceField = entry.getValue();
					String value;
					if (OPERATION_USE_SOURCE_FIELD_CONTEXT.equals(sourceField)){
						//使用来源字段上下文
						Map<String, String> sourceField2value = new HashMap<>();
						for (String header:headers){
							int index = sourceField2index.get(header);
							sourceField2value.put(header, list.get(index));
						}
						value = targetField2customFunctionWithContext.get(targetField).apply(sourceField2value);
					} else {
						//普通
						int index = sourceField2index.get(sourceField);
						value = list.get(index);
						// 定制化部分
						if (targetField2customFunction.containsKey(targetField)){
							value = targetField2customFunction.get(targetField).apply(value);
						}
					}
					if (OPERATION_SKIP_LINE.equals(value)){
						continue i;
					}
					targetList.add(value);
				}
				targetLists.add(targetList);
			}catch (Exception e){
				System.out.println(String.format("行%s转换失败%s/原因%s/位置 %s", i, list, e.toString(), e.getStackTrace()[getStackTraceIndexSuggested(e)]));
			}
		}
		return targetLists;
	}
	
	public List<List<String>> mapsToLists(List<Map<String, String>> maps) {
		List<List<String>> retLists = new ArrayList<>();
		// 处理header
		List<String> retHeaders = new ArrayList<>();
		for (String targetField : targetField2sourceField.keySet()) {
			retHeaders.add(targetField);
		}
		retLists.add(retHeaders);
		if (maps == null) {
			return retLists;
		}
		// 处理data
		i: for (int i = 0; i < maps.size(); i++) {
			Map<String, String> map = maps.get(i);
			try {
				List<String> retList = new ArrayList<>();
				// 具体处理数据
				for (Map.Entry<String, String> entry : targetField2sourceField.entrySet()) {
					String targetHeader = entry.getKey();
					String sourceHeader = entry.getValue();
					String value;
					if (OPERATION_USE_SOURCE_FIELD_CONTEXT.equals(sourceHeader)) {
						// 使用外部字段上下文
						value = targetField2customFunctionWithContext.get(targetHeader).apply(map);
					} else {
						// 普通
						value = map.get(sourceHeader);
						// 定制化部分
						if (targetField2customFunction.containsKey(targetHeader)) {
							value = targetField2customFunction.get(targetHeader).apply(value);
						}
					}
					if (OPERATION_SKIP_LINE.equals(value)) {
						// 跳行
						continue i;
					}
					retList.add(value);
				}
				retLists.add(retList);
			} catch (Exception e) {
				System.out.println(String.format("行%s转换失败%s/原因%s/位置 %s", i, map, e.toString(), e.getStackTrace()[getStackTraceIndexSuggested(e)]));
			}
		}
		return retLists;
	}
	
	public List<Map<String, String>> listsToMaps(List<List<String>> lists) {
		List<Map<String, String>> maps = new ArrayList<>();
		if (lists==null||lists.isEmpty()) {
			return maps;
		}
		//处理header
		List<String> headers = lists.get(0);
		Map<String, Integer> sourceField2index = new HashMap<>();
		for (int i=0;i<headers.size();i++){
			String header = headers.get(i);
			sourceField2index.put(header, i);
		}
		//处理data
		i:for (int i = 1; i < lists.size(); i++) {
			List<String> list = lists.get(i);
			try {
				Map<String, String> map = new LinkedHashMap<>();
				for (Map.Entry<String, String> entry : targetField2sourceField.entrySet()) {
					String targetField = entry.getKey();
					String sourceField = entry.getValue();
					String value;
					if (OPERATION_USE_SOURCE_FIELD_CONTEXT.equals(sourceField)){
						//使用来源字段上下文
						Map<String, String> sourceField2value = new HashMap<>();
						for (String header:headers){
							int index = sourceField2index.get(header);
							sourceField2value.put(header, list.get(index));
						}
						value = targetField2customFunctionWithContext.get(targetField).apply(sourceField2value);
					} else {
						//普通
						int index = sourceField2index.get(sourceField);
						value = list.get(index);
						// 定制化部分
						if (targetField2customFunction.containsKey(targetField)){
							value = targetField2customFunction.get(targetField).apply(value);
						}
					}
					if (OPERATION_SKIP_LINE.equals(value)){
						continue i;
					}
					map.put(targetField, value);
				}
				maps.add(map);
			}catch (Exception e){
				System.out.println(String.format("行%s转换失败%s/原因%s/位置 %s", i, list, e.toString(), e.getStackTrace()[getStackTraceIndexSuggested(e)]));
			}
		}
		return maps;
	}

	public List<Map<String, String>> mapsToMaps(List<Map<String, String>> maps) {
//		if (targetField2sourceField.isEmpty()){
//			//没有配置转换map，进行默认转换
//			//构造sourceField到targetField的DataChunkConverter map
//			if (sourceMaps==null||sourceMaps.isEmpty()) {
//				//TODO 表头信息缺失
//				return null;
//			}
//			DataChunkConverter dcc = new DataChunkConverter();
//			for (String sourceField:sourceMaps.get(0).keySet()) {
//				dcc.map(sourceField, sourceField);
//			}
//			return dcc.mapsToMaps(sourceMaps);
//		}
		
		//将maps默认转换为lists
		List<List<String>> lists = DataChunkConverter.sameMapsToLists(maps);
		//将lists转换为maps
		return listsToMaps(lists);
	}

	public static List<Map<String, String>> sameListsToMaps(List<List<String>> lists) {
		//进行默认转换
		//构造sourceField到targetField的DataChunkConverter map
		if (lists==null||lists.isEmpty()) {
			//表头信息缺失
			return null;
		}
		DataChunkConverter dcc = new DataChunkConverter();
		for (String sourceField:lists.get(0)) {
			dcc.map(sourceField, sourceField);
		}
		return dcc.listsToMaps(lists);
	}
	
	public static List<List<String>> sameMapsToLists(List<Map<String, String>> maps){
		//进行默认转换
		//构造sourceField到targetField的DataChunkConverter map
		if (maps==null||maps.isEmpty()) {
			//TODO 表头信息缺失
			return null;
		}
		DataChunkConverter dcc = new DataChunkConverter();
		for (String sourceField:maps.get(0).keySet()) {
			dcc.map(sourceField, sourceField);
		}
		return dcc.mapsToLists(maps);
	}
	
	public static List<Map<String, String>> sameJsonStrToMaps(String jsonStr){
		Gson gson = new Gson();
		List<Map<String, String>> maps = gson.fromJson(jsonStr, new TypeToken<List<Map<String, String>>>(){}.getType());
		return maps;
	}
	
	public static List<Map<String, String>> sameXmlNodeListToMaps(NodeList nodes){
		List<Map<String, String>> maps = new ArrayList<>();
		if (nodes == null) {
			return maps;
		}
		// 处理data
		for (int i = 0; i < nodes.getLength(); i++) {
			Node node = nodes.item(i);
			try {
				if (node.getNodeType() == Node.TEXT_NODE) {
					continue;
				}
				Map<String, String> map = new HashMap<>();
				NodeList fields = node.getChildNodes();
				for (int j = 0; j < fields.getLength(); j++) {
					Node field = fields.item(j);
					if (field.getNodeType() == Node.TEXT_NODE) {
						continue;
					}
					map.put(field.getNodeName(), field.getTextContent());
				}
				maps.add(map);
			} catch (Exception e) {
				System.out.println(String.format("转换失败: 行%s/内容%s/原因%s/位置%s", i, node, e.toString(), e.getStackTrace()[getStackTraceIndexSuggested(e)]));
			}
		}
		return maps;
	}

}








































