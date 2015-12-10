package com.bigdata.trugen;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;


/**********************************************************************************
    This class constructs URL for data downloader and calls the data downloader.
    It takes the list of ticker symbols in csv format and output file path as input.
**********************************************************************************/
public class DownloadManager 
{
        // create downloader 
	static Downloader downloader = new Downloader();
	static List<String> lSymbols = new ArrayList<String>();
	
	public static void main(String args[]) throws IOException
	{
                // get input and output file paths
		if (args.length != 2)
		{
			usage();
		}
		String sSymbolFile   = args[0];
		String sOutputFolder = args[1];
		
                // parse symbol file and store symbols to a list
		File fSymbolFile = new File(sSymbolFile);
		System.out.println("Loading symbols from file: " + sSymbolFile.toString() + "...");
		BufferedReader br = null;
		if (fSymbolFile.exists() && fSymbolFile.isFile())
		{
			
			try
			{
				String sCurrLine;
				br = new BufferedReader(new FileReader(fSymbolFile.toString()));
				while ((sCurrLine = br.readLine()) != null)
				{
					String[] toks = sCurrLine.split(",");
					lSymbols.add(toks[0].replaceAll("\"", ""));
				}
				br.close();
			}
			catch (IOException e)
			{
				System.err.println("Error : Reading symbol file. Exiting.");
				br.close();
				System.exit(1); /* 0=completed without error, 1=graceful termination, -1=unexpected error */
			}
		}
		
		System.out.println("Loading symbols done...");
		System.out.println(lSymbols.size() + " symbols read...");
		
		// Starts from index 1 to skip the header.
		for (int i = 1; i < lSymbols.size(); i++)
		{
                        // Construct URL
			System.out.println("Downloading data for symbol: " + lSymbols.get(i));
			
			Calendar cal = Calendar.getInstance();
			int year = cal.get(Calendar.YEAR);
			int month = cal.get(Calendar.MONTH);
			int date = cal.get(Calendar.DATE);

			String sUrl = "http://real-chart.finance.yahoo.com/table.csv?s=" + 
			               lSymbols.get(i) + 
			               "&d=" + month + "&e=" + date + "&f=" + year + "&g=d&a=11&b=12&c=1980&ignore=.csv";
			String sFileName = sOutputFolder + File.separatorChar + lSymbols.get(i) + ".csv";
			// System.out.println("URL :   " + sUrl);
			// System.out.println("File:   " + sFileName);

                        // Call downloader
			try
			{
				downloader.download(sUrl, sFileName);
			}
			catch (IOException e)
			{
				System.err.println("Error : Downloading data for " + lSymbols.get(i) + ". It's index is : " + i + "...");
				e.printStackTrace();
				continue;
			}
			System.out.println("Successfully downloaded data for " + lSymbols.get(i) + ". It's index is : " + i + "...");
		}
	}
	
        /*
            This method prints out usage to standard out
        */
	public static void usage()
	{
		System.out.println("Usage: ");
		System.out.println("       java -cp /path/to/DownloadManager.jar com.bigdata.trugen.DownloadManager /path/to/symbol/file /path/to/output/folder/");
		System.exit(0);
	}
}
