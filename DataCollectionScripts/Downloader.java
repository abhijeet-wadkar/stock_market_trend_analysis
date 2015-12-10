package com.bigdata.trugen;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;


public class Downloader 
{	
	public void download(String sUrl, String sFileName) throws IOException
	{
		URL url = new URL(sUrl);
		InputStream in = new BufferedInputStream(url.openStream());
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		byte[] buf = new byte[1024];
		int n = 0;
		while (-1!=(n=in.read(buf)))
		{
		    out.write(buf, 0, n);
		}
		out.close();
		in.close();
		byte[] response = out.toByteArray();
 
		FileOutputStream fos = new FileOutputStream(sFileName);
		fos.write(response);
		fos.close();
	}
}
