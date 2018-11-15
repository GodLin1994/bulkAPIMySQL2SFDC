package com.cg.zcript;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class StreamUtil {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
System.out.println("====");
	}

    /**
     * outputStream转inputStream
     * @param out
     * @return
     */
    public static InputStream convertOutputStreamToInputStream(OutputStream os){
        ByteArrayOutputStream baos;
        baos=(ByteArrayOutputStream) os;
        InputStream swapStream = new ByteArrayInputStream(baos.toByteArray()); 
        return swapStream;
    }

    /**
     * inputStream转outputStream
     * @param out
     * @return
     */
    public static OutputStream convertInputStreamToOutputStream(InputStream is){
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int len;
		byte[] buffer = new byte[4096];
        try {
			while ((len = is.read(buffer)) != -1) {   
			    baos.write(buffer, 0, len);   
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
        return baos;
    }

	public static String convertInputStreamToString(InputStream is) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int len;
		byte[] buffer = new byte[4096];
        try {
			while ((len = is.read(buffer)) != -1) {   
			    baos.write(buffer, 0, len);   
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
        return baos.toString();
	}
    
	public static InputStream convertStringToInputStream(String s) {
        ByteArrayInputStream bais=new ByteArrayInputStream(s.getBytes());
        return bais;
	}

	public static OutputStream convertStringToOutputStream(String s) {
		return convertInputStreamToOutputStream(convertStringToInputStream(s));
	}

	public static String convertOutputStreamToString(OutputStream os) {
		return convertInputStreamToString(convertOutputStreamToInputStream(os));
	}

}
























