package eu.openaire.doc_urls_retriever.util.file;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Scanner;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;



public class FileUtils
{
	private static final Logger logger = LogManager.getLogger(FileUtils.class);
	
	private static Scanner inputScanner;
	//private static Scanner inputDataScanner;
	private static File inputFile;
	private static File outputFile;
	public static HashMap<String,String> outputEntries = new HashMap<String,String>();
	private static long fileIndex = 0;	// Index in the input file
	public static boolean skipFirstRow = false;
	private static FileWriter writer;
	private static String outputDelimiter = "\t";
	private static String endOfLine = "\n";
	public static long emptyInputLines = 0;	// For better statistics in the end.
	public static int groupCount = 1000;	// Just for testing.. TODO -> Later increase it..

    
	
    /**
     * This method returns the number of (non-heading, non-empty) lines we have read from the inputFile.
     * @return loadedUrls
     */
	public static long getCurrentlyLoadedUrls()	// In the end, it gives the total number of urls we have processed.
	{
		if ( FileUtils.skipFirstRow )
			return FileUtils.fileIndex - FileUtils.emptyInputLines -1; // -1 to exclude the first line
		else
			return FileUtils.fileIndex - FileUtils.emptyInputLines;
	}
	
	
	public FileUtils(String inputFileName, String outputFileName) throws RuntimeException
	{
    	logger.debug("Input file: " + inputFileName);	// DEBUG!
    	logger.debug("Output file: " + outputFileName);	// DEBUG!
    	
		try {
			FileUtils.inputFile = new File(inputFileName);
			FileUtils.inputScanner = new Scanner(FileUtils.inputFile /*System.in*/);
			FileUtils.outputFile = new File(outputFileName);
			FileUtils.writer = new FileWriter(outputFile);

			// Put the Headers in the Hash Tables.
			FileUtils.outputEntries.put("SourceUrls", "DocUrls");
			FileUtils.writeToFile();	// Write now to have the headers in the first raw... (There is a bug if we leave it for later.. but retry it)
		} catch (Exception e) {
			logger.error(e);
			throw new RuntimeException(e);	// If any of the files is not found.. we should NOT continue..
		}
	}
	
	
	/**
	 * Parses a csv file with only one column, which is the url one.
	 * @return HashSet<String>
	 */
	public static HashSet<String> getNextUrlGroup()
	{
		HashSet<String> urlGroup = new HashSet<String>();
		
		// Take a group of <groupCount> urls from the file..
		// If we are at the end and there are less than <groupCount>.. take as many as there are..
		
		//logger.debug("Retrieving the next group of " + groupCount + " elements from the inputFile.");
		long curBeggining = FileUtils.fileIndex;

		while ( (inputScanner.hasNextLine()) && (FileUtils.fileIndex < (curBeggining + groupCount)) )
		{// While (!EOF) iterate through lines.

			// Take each line, remove potential double quotes.
			String retrievedString = StringUtils.replace(inputScanner.nextLine(), "\"", "");	// Take next line and replace any '\"' in the input..

			FileUtils.fileIndex ++;

			if ( skipFirstRow && (FileUtils.fileIndex == 0) )
				continue;

			if ( retrievedString.isEmpty() ) {
				FileUtils.emptyInputLines ++;
				continue;
			}

			urlGroup.add( retrievedString );
			//logger.debug("Loaded from inputFile: " + retrievedString);	// DEBUG!
		}
		//logger.debug("FileUtils.fileIndex after taking urls after " + FileUtils.fileIndex / groupCount + " time(s), from input file: " + FileUtils.fileIndex);	// DEBUG!
		
		return urlGroup;
	}
	
	
	/**
	 * This function writes new source-doc URL set in the output file.
	 * Each time it's finished writing, it flushes the write stream and clears the urlTable.
	 */
	public static void writeToFile()
	{
		if ( FileUtils.fileIndex == 0 ) // If we haven't started reading the inputFile yet..
			logger.debug("Writing headings (\"SourceUrls\", \"DocUrls\") to the outputFile.");
		else
			logger.debug("Writing to the outputFile.. " + outputEntries.size() + " set(s) of (\"SourceUrl\", \"DocUrl\")");
		
		// If later we want to make the StringBuilder member of this class.. and we use multithreaded environment, we should use "StringBuffer" instead, as the last one is thread-safe.
		StringBuilder strB = new StringBuilder(groupCount * 300);	// 300: the maximum expected length for a source-doc-mime triple..
	    
		try {
			for ( Entry<String,String> entry : outputEntries.entrySet() )
		    {
				strB.append(entry.getKey());
				strB.append(outputDelimiter);
				strB.append(entry.getValue());
				strB.append(endOfLine);
		    }

		    // Print in System.out
		    /*System.out.print(strB.toString());
		    System.out.flush();*/

			writer.write(strB.toString());
			writer.flush();
		} catch (IOException e) {
			logger.warn(e);
			throw new RuntimeException(e);
		} finally {
			FileUtils.outputEntries.clear();	// Clear to keep in memory only <groupCount> values at a time.
		}
	}
	
	
	/**
	 * Closes open Streams.
	 */
	public static void closeStreams()
	{
		try {
			inputScanner.close();
			writer.close();
		} catch (IOException e) {
			logger.error("Unable to close FileWriter!", e);
			throw new RuntimeException(e);
		}
	}

}
