package com.cg.zcript;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

public class SFDCDatas2DB {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			BulkApiHelper bah = new BulkApiHelper(BulkApiHelper.DEFAULT_TEST_ENDPOINT, Hardcode.username, Hardcode.password);
			String soql = "SELECT Id,Name,Phone from Account";
			List<List<String>> lists = bah.query("Account", soql);//lists whose first line as header and the rest as data 
			System.out.println(lists);
	            // 3.准备插入的SQL语句
	         String sql = "INSERT INTO emp_Bulk (empno,job,ename) " 
	                    +"VALUES (?,?,?)";

	         PreparedStatement pstmt;
	         Connection conn = JDBC2SFDC.getConnection();
	         pstmt = (PreparedStatement) conn.prepareStatement(sql);
	         for (int j=1;j<lists.size();j++) {
	            
	             System.out.println(lists.get(j).get(0) + "--" + lists.get(j).get(1));
	             pstmt.setString(1,lists.get(j).get(0));
	             pstmt.setString(2,lists.get(j).get(1));
	             pstmt.setString(3,lists.get(j).get(2));
	             int i = pstmt.executeUpdate();
	             System.out.println(i);
	         }
	    
	         pstmt.close(); 
             conn.close();
		} catch (Exception e) {
			// TODO: handle exception
			System.out.println(e.getMessage());
		}
    
	}

}
