package com.umbc.bigdata.datastore;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CsvFileGenerator
{
	
	static Log logger = LogFactory.getLog(CsvFileGenerator.class);

	public static void main(String[] args) 
	{
		logger.info("CsvFileGenerator started ...");

        String sConfig;
        if (args.length != 1)
        {
        	usage();
        	System.exit(0);
        }
        sConfig = args[0];
        
        File fConfig = new File(sConfig);
        
        if (!fConfig.exists())
        {
        	logger.error("Config file does not exist.");
        	System.exit(1);
        }
        
        File fInDir           = null;
        String outFileName     = "";

        try
        {
            XMLConfiguration xConfig = new XMLConfiguration(fConfig.getAbsolutePath());
            xConfig.load();
            String sInDir = xConfig.getString("InputDir");
            fInDir = new File(sInDir);
            outFileName = xConfig.getString("OutFile");
        }
        catch (ConfigurationException e)
        {
        	logger.error("Error reading config file.");
        	System.exit(1);
        }
        
        String sExSymbol = getSymbol(fInDir);
        System.out.println("ExChange Symbol : " + sExSymbol);
        
        File[] fileList;
        if (fInDir.exists() && fInDir.isDirectory())
        {
        	fileList = fInDir.listFiles();
        	parseAndStore(fileList, sExSymbol, outFileName);
        }
        else
        {
        	logger.error("Error input is not a directory.");
        	System.exit(0);
        }
        logger.info("Hbase data store done!!");
	}
	
	private static String getSymbol(File fInDir)
	{
		return fInDir.getAbsolutePath().substring(fInDir.getAbsolutePath().lastIndexOf("/") + 1);
	}

	private static void usage()
	{
		System.out.println("Usage : app_name  <path/to/config/file>");
	}
	
	private static void parseAndStore(File[] in, String sExSymbol, String outFileName)
	{
		BufferedReader br = null;
		FileWriter fw     = null;
		BufferedWriter bw = null;
		File outFile      = new File(outFileName);
		if (!outFile.exists())
		{
			try
			{
				outFile.createNewFile();
				fw = new FileWriter(outFile.getAbsoluteFile());
				bw = new BufferedWriter(fw);
			}
			catch (IOException e)
			{
				System.err.println("Error creating a file.");
				e.printStackTrace();
				System.exit(0);
			}
		}
		
		for (int i = 0; i < in.length; i++)
		{
			File theFile = in[i];
			if (theFile.exists() && theFile.isFile())
			{
				String symbol = getSymbol(theFile);
				
				// remove extension ".csv" from file name
				symbol = symbol.substring(0, symbol.lastIndexOf("."));
				System.out.println(symbol);
				
				try
				{
					br = new BufferedReader(new FileReader(theFile.getAbsolutePath()));
					String sLine;
					
					// Skippin the header
					br.readLine();
					
					while ((sLine = br.readLine()) != null)
					{
						String[] toks = sLine.split(",");
						
						if (toks.length == 7)
						{
							System.out.println("ExCh  : " + sExSymbol.toUpperCase());
							System.out.println("Sym   : " + symbol.toUpperCase());
							System.out.println("Date  : " + toks[0]);
							System.out.println("Open  : " + toks[1]);
							System.out.println("High  : " + toks[2]);
							System.out.println("Low   : " + toks[3]);
							System.out.println("Close : " + toks[4]);
							System.out.println("Vol   : " + toks[5]);
							System.out.println("AdjVol: " + toks[6]);
							System.out.println("--------------------------------------");
							// String sKey = sExSymbol.toUpperCase() + ":" + symbol.toUpperCase() + ":" + toks[0];
							// insertHbase("market", sKey, toks[1], toks[2], toks[3], toks[4], toks[5], toks[6]);
							String tempString = sExSymbol.toUpperCase() + "," +
									symbol.toUpperCase() + "," +
									toks[0] + "," +
									toks[1] + "," +
									toks[2] + "," +
									toks[3] + "," +
									toks[4] + "," +
									toks[5] + "," +
									toks[6] + "\n";
							bw.write(tempString);
						}
						else
						{
							logger.error("Error missing some records.");
							continue;
						}
					}
					br.close();
				}
				catch (IOException e)
				{
					logger.error("Error : reading input file.");
					continue;
				}
			}// end if (theFile.exists() && theFile.isFile())
		}// end for loop
		try
		{
			bw.close();
		}
		catch (IOException e)
		{
			System.err.println("Error closing buffered writer.");
		}
	}
}
