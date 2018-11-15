package com.cg.zcript;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

import com.cg.zcript.StreamUtil;

public class CSVHelper {

	public static void main(String[] args) throws UnsupportedEncodingException {
//		String csvFilePath = "./src/excel/20170928145400.csv";
//		try (InputStream is = CSVHelper.class.getResourceAsStream("20170928145400.csv")){
//			System.out.println(readListsFromCSV(is));
//		} catch (IOException e) {
//			e.printStackTrace();
//		} 
		List<List<String>> lists = new ArrayList<>();
		lists.add(Arrays.asList(new String[]{"", "bc", null, "null"}));
		OutputStream os = new ByteArrayOutputStream();
		CSVHelper.writeListsToCSV(lists, os);
		System.out.println(StreamUtil.convertOutputStreamToString(os));
		System.out.println("==============================================");
		byte[] convertListToCSVBytes = convertListToCSVBytes(Arrays.asList(new String[]{"编号", "附加电池", "Extra Battery", ""}));
		System.out.println(new String(convertListToCSVBytes, "gb2312"));

	}

	public static List<List<String>> readListsFromCSV(InputStream inputStream) {
		CsvReader reader = null;
		List<List<String>> ret = new ArrayList<>();
		try {
			// 创建CSV读对象 例如:CsvReader(文件路径，分隔符，编码格式);
			reader = new CsvReader(inputStream, ',', Charset.forName("UTF-8"));
			// 跳过表头 如果需要表头的话，这句可以忽略
//			reader.readHeaders();
			// 逐行读入除表头的数据
			while (reader.readRecord()) {
				ret.add(Arrays.asList(reader.getValues()));
			}
			return ret;
		} catch (Exception e) {
			e.printStackTrace();
			return ret;
		} finally {
			if (reader != null) {
				reader.close();
			}
		}
	}
	
    //outputStream转inputStream
    public static InputStream convertToInputStream(OutputStream out)
    {
        ByteArrayOutputStream baos=new ByteArrayOutputStream();
        baos=(ByteArrayOutputStream) out;
        InputStream swapStream = new ByteArrayInputStream(baos.toByteArray());
        return swapStream;
    }

    public static void writeListsToCSV(List<List<String>> lists, OutputStream outputStream){
		CsvWriter csvWriter = null;
	    try {  
	        // 创建CSV写对象 例如:CsvWriter(文件路径，分隔符，编码格式);  
	        csvWriter = new CsvWriter(outputStream, ',', Charset.forName("UTF-8"));  
	        // 写表头  
//	        String[] csvHeaders = (String[]) lists.get(0).toArray();
//	        csvWriter.writeRecord(csvHeaders); 
	        // 写内容  
	        for (int i = 0; i < lists.size(); i++) {  
	            String[] csvContent = lists.get(i).toArray(new String[lists.get(i).size()]);
	            csvWriter.writeRecord(csvContent);  
	        }  
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (csvWriter != null) {
				csvWriter.close();
			}
		}

	}

    /**
     * 将一行数据转为UTF-8拆码的byte[]
     * @param list
     * @return
     */
    public static byte[] convertListToCSVBytes(List<String> list){
    	List<List<String>> lists = new ArrayList<>();
    	lists.add(list);
    	try(ByteArrayOutputStream os = new ByteArrayOutputStream()){
        	writeListsToCSV(lists, os);
        	return os.toByteArray();
    	} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
    }

}
