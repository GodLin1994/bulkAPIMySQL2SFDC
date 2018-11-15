package com.cg.zcript;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;

import com.alibaba.fastjson.JSON;

public class DynamicGetBeansFromDB {
    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        // 设置类成员属性
        try {

            String tableName = "db_account";
            PreparedStatement pstmt;
            HashMap<String, String> fieldName2fieldTypeMap = new HashMap<>();
            String sql = "select * from " + tableName;
            Connection conn;
            conn = JDBC2SFDC.getConnection();
            pstmt = (PreparedStatement) conn.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery(sql);
            ResultSetMetaData data = rs.getMetaData();
            System.out.println("data-->"+data);
            for (int i = 1; i <= data.getColumnCount(); i++) {
                String columnName = data.getColumnName(i);
                String columnType = data.getColumnTypeName(i);
                fieldName2fieldTypeMap.put(columnName, columnType);
            }

            System.out.println(fieldName2fieldTypeMap);

            // HashMap<String, String> f2tMap = getDBFields("db_account");
            HashMap propertyMap = new HashMap();
            for (String attr : fieldName2fieldTypeMap.keySet()) {
                switch (fieldName2fieldTypeMap.get(attr)) {
                    case "VARCHAR":
                        propertyMap.put(attr, Class.forName("java.lang.String"));
                        break;
                    default:
                        break;
                }
            }

            // 生成动态 Bean

            ArrayList<Object> objects = new ArrayList<>();

            while (rs.next()) {
                // 获取stuname这列数据

                CglibBean bean = new CglibBean(propertyMap);
                for (String attr : fieldName2fieldTypeMap.keySet()) {
                    switch (fieldName2fieldTypeMap.get(attr)) {
                        case "VARCHAR":
                            bean.setValue(attr, rs.getString(attr));
                            break;
                        default:
                            break;
                    }

                }
                Object object = bean.getObject();
                objects.add(object);
            }

            // 从 Bean 中获取值，当然了获得值的类型是 Object

            // 获得bean的实体
            String iString = JSON.toJSONString(objects);
            System.out.println(iString);
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }

    }

}
