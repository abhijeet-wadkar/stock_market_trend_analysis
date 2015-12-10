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

/*************************************************************************
   This class takes a config file as input. The config file contains
   a absolute path to historical market data and absolute path to
   the output file.

   Example Config file:
 
   <?xml version="1.0" encoding="UTF-8"?>
   <config>
       <InputDir>/media/sf_VMshare/stocks/nyse/</InputDir>
       <OutFile>/media/sf_VMshare/stocks/nyse.out</OutFile>
   </config>

   How to run:
  
   $> java -cp /path/to/the/app/jar com.umbc.bigdata.datastore.CsvFileGenerator /path/to/config/file.xml

   
***************************************************************************/
public class CsvFileGenerator
{
	// Create logger
	static Log logger = LogFactory.getLog(CsvFileGenerator.class);
	public static void main(String[] args) 
	{
		logger.info("CsvFileGenerator started ...");

        // Get config file path
        String sConfig;
        if (args.length != 1)
        {
        	usage();
        	System.exit(0);
        }
        sConfig = args[0];
        
        File fConfig = new File(sConfig);
        
        // See if config file exists
        if (!fConfig.exists())
        {
        	logger.error("Config file does not exist.");
        	System.exit(1);
        }
        
        File fInDir           = null;
        String outFileName     = "";

        // Get input path and output path from config
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
        
        // Get exchange code from directory name
        String sExSymbol = getSymbol(fInDir);
        System.out.println("ExChange Symbol : " + sExSymbol);
        
        File[] fileList;
        if (fInDir.exists() && fInDir.isDirectory())
        {
                // Get file list from the input directory
        	fileList = fInDir.listFiles();
                // Parse the file and generates csv file
        	parseAndStore(fileList, sExSymbol, outFileName);
        }
        else
        {
        	logger.error("Error input is not a directory.");
        	System.exit(0);
        }
        logger.info("Hbase data store done!!");
	}
	
        /*
            This method stripts file path and returns exchange code
        */
	private static String getSymbol(File fInDir)
	{
		return fInDir.getAbsolutePath().substring(fInDir.getAbsolutePath().lastIndexOf("/") + 1);
	}

        /*
            This method prints out Usage.
        */
	private static void usage()
	{
		System.out.println("Usage : app_name  <path/to/config/file>");
	}
	
        /*
            This method parses input file and populates data into hbase table.
        */
	private static void parseAndStore(File[] in, String sExSymbol, String outFileName)
	{
                // Create output file
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
		
                // Go through file list
		for (int i = 0; i < in.length; i++)
		{
			File theFile = in[i];
			if (theFile.exists() && theFile.isFile())
			{
                                // Get ticker symbol from file name
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
						
                                                // Get data from line
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
                                                        
                                                        // Add exchange symbol and ticker symbol to the output line
							String tempString = sExSymbol.toUpperCase() + "," +
									symbol.toUpperCase() + "," +
									toks[0] + "," +
									toks[1] + "," +
									toks[2] + "," +
									toks[3] + "," +
									toks[4] + "," +
									toks[5] + "," +
									toks[6] + "\n";
                                                        // write to the file
							bw.write(tempString);
						}
						else
						{
							logger.error("Error missing some records.");
							continue;
						}
					}
                                        // Close bufferred reader
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
                        // Close bufferred writer
			bw.close();
		}
		catch (IOException e)
		{
			System.err.println("Error closing buffered writer.");
		}
	}
}
