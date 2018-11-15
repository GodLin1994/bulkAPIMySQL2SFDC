package com.cg.zcript;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import com.sforce.async.AsyncApiException;
import com.sforce.ws.ConnectionException;

import com.cg.zcript.DataChunkConverter;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.cg.zcript.CSVHelper;

public class Main {

	public static void main(String[] args) {
		CloseableHttpClient httpCilent = HttpClients.createDefault();
		HttpGet httpGet = new HttpGet("http://prmtest2.dahuatech.com/prm-os-srv-crm/queryDDPXSList?lastUpd=20170830122825&rowKey=1-ZJ07060002&step=10");
		try {
			String srtResult = "";
			HttpResponse httpResponse = httpCilent.execute(httpGet);
			if(httpResponse.getStatusLine().getStatusCode() == 200){
				srtResult = EntityUtils.toString(httpResponse.getEntity());//获得返回的结果
				
                System.out.println(srtResult);
                List<List<String>> listReadyToBeConvert = JSONToListString(srtResult);
                System.out.println(listReadyToBeConvert);
                DataChunkConverter lc = new DataChunkConverter();
                lc.map("Freight_Coefficient__c","FREIGHT");
                lc.map("Tariff_Coefficient__c","TARIFF");
                lc.map("Subcompany__c","SUB_COMPANY");
                List<List<String>> listsSf = lc.listsToLists(listReadyToBeConvert);
                listsSf.get(0).add("Product__c");
                for(int i=1;i<listsSf.size();i++) {
                	listsSf.get(i).add("a26p0000000aKgH");
                }
                BulkApiHelper bah = new BulkApiHelper(BulkApiHelper.DEFAULT_TEST_ENDPOINT, Hardcode.username, Hardcode.password);
                System.out.println("listsSf:"+listsSf);
                bah.insert("Price_Coefficient__c", listsSf);
			}
		}catch(Exception e) {
			System.out.println(e);
		}
		
//		try (InputStream is = new FileInputStream("./src/data/crm021Data_20180425135959.csv")){
//			List<List<String>> lists = CSVHelper.readListsFromCSV(is);
//			//convert lists to SF BulkAPI CSV format
//			DataChunkConverter lc = new DataChunkConverter();
//			//普通mapping
//			lc.map("Remark__c", "text1Zh");
//			//mapping后用lumbda表达式定制化value
//			lc.map("Employee_Number__c", "pernr", v->Integer.valueOf(v)+"");
//			//targetField对应多个sourceField组合
//			lc.map("Name", m->m.getOrDefault("ebeln", "")+"/"+m.getOrDefault("ebelp", ""));
//			
//			//转换源数据到SF能接受的目标二维list（csv）数据
//			List<List<String>> listsSf = lc.listsToLists(lists);
//			System.out.println(listsSf);
//			
//			//upload data to merge sObject: uploads upsert batches for lists and delete the records which are not upserted
//			BulkApiHelper bah = new BulkApiHelper(BulkApiHelper.DEFAULT_TEST_ENDPOINT, "shu.chen@kiongroup.devpro1", "password");
//			bah.merge("Purchase_Order__c", listsSf, "Name");
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (AsyncApiException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (ConnectionException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}		

	}
	
	public List<List<String>> mapPriceCoefficient() throws AsyncApiException, IOException, ConnectionException, InterruptedException{
		InputStream is = new FileInputStream("./src/data/crm021Data_20180425135959.csv");
		
		List<List<String>> lists = CSVHelper.readListsFromCSV(is);
		
		//convert lists to SF BulkAPI CSV format
		DataChunkConverter lc = new DataChunkConverter();
		//普通mapping
		lc.map("Remark__c", "text1Zh");
		//mapping后用lumbda表达式定制化value
		lc.map("Employee_Number__c", "pernr", v->Integer.valueOf(v)+"");
		//targetField对应多个sourceField组合
		lc.map("Name", m->m.getOrDefault("ebeln", "")+"/"+m.getOrDefault("ebelp", ""));
		
		//转换源数据到SF能接受的目标二维list（csv）数据
		List<List<String>> listsSf = lc.listsToLists(lists);
		
		BulkApiHelper bah = new BulkApiHelper(BulkApiHelper.DEFAULT_TEST_ENDPOINT, "shu.chen@kiongroup.devpro1", "password");
		bah.merge("Purchase_Order__c", listsSf, "Name");
		
		return listsSf;
	}
	
	public static List<List<String>> JSONToListString(String json){
		List<List<String>> rsList = new ArrayList<List<String>>();
		JSONObject jsonObject = JSON.parseObject(json);
		JSONArray jsonArray = jsonObject.getJSONArray("prodList");
		JSONObject joHeader= (JSONObject)jsonArray.get(0);
		List<String> headerList = new ArrayList<String>(joHeader.keySet());
		rsList.add(headerList);
		for(Object obj : jsonArray) {
			JSONObject jobj = (JSONObject)obj;
			List<String> contentList = new ArrayList<String>();
			for(String sourceField:headerList) {
				contentList.add(jobj.getString(sourceField));
			}
			rsList.add(contentList);
		}
		System.out.println(headerList);
		return rsList;

	}
}
