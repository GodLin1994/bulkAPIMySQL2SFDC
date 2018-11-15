package com.cg.zcript;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.sforce.async.AsyncApiException;
import com.sforce.ws.ConnectionException;


public class JDBC2SFDC
{
	
	 public static Connection getConnection() throws Exception {
		  //声明Connection对象
	        Connection con;
	        //驱动程序名
	        String driver = "com.mysql.jdbc.Driver";
	        //URL指向要访问的数据库名mydata
	        String url = "jdbc:mysql://45.199.156.122:3307/testBulk";
	        //MySQL配置时的用户名
	        String user = "root";
	        //MySQL配置时的密码
	        String password = "July07201516";
	        //遍历查询结果集
	        Class.forName(driver);
            //1.getConnection()方法，连接MySQL数据库！！
            con = DriverManager.getConnection(url,user,password);
	        return con;
	    }
    public static void main( String[] args )
    {
    	
        try {
            //加载驱动程序
        	Connection con = getConnection();
            //2.创建statement类对象，用来执行SQL语句！！
            Statement statement = con.createStatement();
            //要执行的SQL语句
            String sql = "select * from emp_Bulk";
            //3.ResultSet类，用来存放获取的结果集！！
            ResultSet rs = statement.executeQuery(sql);
             
            String job = null;
            String id = null;
            List<AccountWapper> accountWappers = new ArrayList<>();
            while(rs.next()){
                //获取stuname这列数据
                job = rs.getString("job");
                //获取stuid这列数据
                id = rs.getString("ename");

                AccountWapper accountWapper = new  AccountWapper(job, id);
                accountWappers.add(accountWapper);
                //输出结果
                System.out.println(id + "\t" + job);
            }
           HashMap<String, List<AccountWapper>> accountMap = new HashMap<String, List<AccountWapper>>();
            accountMap.put("accountValue",accountWappers );
            String jString = JSON.toJSONString(accountMap); 
            List<List<String>> listReadyToBeConvert = JSONToListString(jString);
            System.out.println(listReadyToBeConvert);
            DataChunkConverter lc = new DataChunkConverter();
            lc.map("Name","name");
            lc.map("Phone","phone");
            List<List<String>> listsSf = lc.listsToLists(listReadyToBeConvert);
            BulkApiHelper bah = new BulkApiHelper(BulkApiHelper.DEFAULT_TEST_ENDPOINT, "tony.cuil@qq.com", "July^0720");
            System.out.println("listsSf:"+listsSf);
            bah.insert("Account", listsSf);
            rs.close();
            con.close();
        } catch(ClassNotFoundException e) {   
            //数据库驱动类异常处理
            System.out.println("Sorry,can`t find the Driver!");   
            e.printStackTrace();   
            } catch(SQLException e) {
            //数据库连接失败异常处理
            e.printStackTrace();  
            }catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }finally{
            System.out.println("数据库数据成功获取！！");
        }
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
		JSONArray jsonArray = jsonObject.getJSONArray("accountValue");
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
