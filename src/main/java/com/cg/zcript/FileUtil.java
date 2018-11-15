package com.cg.zcript;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

public class FileUtil {

	public static String getStringFromUrl(String url) throws IOException {
		URL fileUrl = new URL(url);
		try(
			InputStream is = fileUrl.openStream();
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
		){
			byte[] buffer = new byte[8192];
			int len = 0;
			while ((len = is.read(buffer)) != -1) {
				baos.write(buffer, 0, len);
			}
			byte[] data = baos.toByteArray();
			return new String(data);
		}
	}

	public static String getStringFromFileLocation(String fileLocation) throws IOException {
		try(
			InputStream is = new FileInputStream(fileLocation);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
		){
			byte[] buffer = new byte[8192];
			int len = 0;
			while ((len = is.read(buffer)) != -1) {
				baos.write(buffer, 0, len);
			}
			byte[] data = baos.toByteArray();
			return new String(data);
		}
	}

    public static String readFileByClassloader(Class<?> clazz, String fileName) {
    	try(
	    	InputStream is = clazz.getResourceAsStream(fileName);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
    	){
    		byte[] buffer = new byte[4096];
    		int len = 0;
    		while ((len = is.read(buffer)) != -1) {
    			baos.write(buffer, 0, len);
    		}
    		byte[] data = baos.toByteArray();
        	return new String(data);
    	} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
    }

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
