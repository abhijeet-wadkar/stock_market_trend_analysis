package com.bigdata.trugen;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;


public class Downloader 
{
        /*********************************************************************
           This method downloads data from URL and writes to a file.
        *********************************************************************/	
	public void download(String sUrl, String sFileName) throws IOException
	{
                // Create url
		URL url = new URL(sUrl);
		InputStream in = new BufferedInputStream(url.openStream());
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		byte[] buf = new byte[1024];
		int n = 0;

                // Download data
		while (-1!=(n=in.read(buf)))
		{
		    out.write(buf, 0, n);
		}
                
                // Close output stream
		out.close();
  
                // Close input stream
		in.close();

                // Write to a file
		byte[] response = out.toByteArray();
		FileOutputStream fos = new FileOutputStream(sFileName);
		fos.write(response);
		fos.close();
	}
}
