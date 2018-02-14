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



@SuppressWarnings("ALL")
public class FileUtils
{
	private static final Logger logger = LogManager.getLogger(FileUtils.class);
	
	private static Scanner inputScanner;
	private static Scanner inputDataScanner;
	private static Scanner outputScanner;
	private static File inputFile;
	private static File outputFile;
	public static HashMap<String,String> outputEntries = new HashMap<String,String>();
	//private static long fileIndex = 0;	// Index in the input file
	private static long fileIndex = 0;
	public static boolean skipFirstRow = false;
	private static FileWriter writer;
	private static String outputTsvSeperator = "\t";
	private static String endOfLine = "\n";
	public static int groupCount = 100;	// Just for testing.. TODO -> Later increase it..
    
	
    /**
     * This method returns the number of non-empty lines we have read from the inutFile. 
     * @return
     */
	public static long getFileIndex()	// In the end, it gives the total number of urls we have processed.
	{
		//logger.debug("FileUtils.fileIndex when asked to return its value: " + FileUtils.fileIndex);
		return FileUtils.fileIndex;
	}
	
	
	public FileUtils(String inputFileName, String outputFileName) throws RuntimeException
	{
    	logger.debug("Input file: " + inputFileName);	// DEBUG!
    	logger.debug("Output file: " + outputFileName);	// DEBUG!
    	
		try {
			FileUtils.inputFile = new File(inputFileName);
			FileUtils.inputScanner = new Scanner(inputFile);
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
		HashSet<String> urls = new HashSet<String>();
		
		// Take a group of <groupCount> urls from the file..
		// If we are at the end and there are less than <groupCount>.. take as many as there are..
		
		//logger.debug("Retrieving the next group of " + groupCount + " elements from the inputFile.");
		int curIteration = 0;
		while ( (inputScanner.hasNextLine()) && (FileUtils.fileIndex + curIteration <= (FileUtils.fileIndex + groupCount)) )
		{// While (!EOF) iterate through lines.
			
			// Take each line, remove potential double quotes.
			String retrievedString = StringUtils.replace(inputScanner.nextLine(), "\"", "");	// Take next line and replace any '\"' in the input..
			
			 if ( retrievedString.isEmpty() )
				continue;
			
			// If skipFirstRow = true, then if we are not at the firstRow -> add the url.
			if ( !skipFirstRow || !((FileUtils.fileIndex == 0) && (curIteration == 0)) ) {
				urls.add( retrievedString );
				//logger.debug(retrievedString);	// DEBUG!
			}
			curIteration++;
		}
		
		FileUtils.fileIndex = curIteration;	// Next line in file.
		//logger.debug("FileUtils.fileIndex after taking urls from input file: " + FileUtils.fileIndex);	// DEBUG!
		
		return urls;
	}
	
	
	/**
	 * This function writes new source-doc URL set in the output file.
	 * Each time it's finished writing, it flushes the write stream and clears the urlTable.
	 */
	public static void writeToFile()
	{
		logger.debug("WRITING TO DISK.. " + outputEntries.size() + " set(s)");
		
		// If later we want to make the StringBuilder member of this class.. and we use multithreaded environment, we should use "StringBuffer" instead, as the last one is thread-safe.
		StringBuilder strB = new StringBuilder(groupCount * 300);	// 300: the maximum expected length for a source-doc-mime triple..
	    
		try {
			for ( Entry<String,String> entry : outputEntries.entrySet() )
		    {
				strB.append(entry.getKey());
				strB.append(outputTsvSeperator);
				strB.append(entry.getValue());
				strB.append(endOfLine);
		    }
		    
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
	 * Closes open Scanners to files.
	 */
	public static void closeStreams()
	{
		try {
			inputDataScanner.close();
			inputScanner.close();
			outputScanner.close();
			writer.close();
		} catch ( NullPointerException npe ) {
			return;	// They are probably already nulled by the JGC.
		} catch (IOException e) {
			logger.error("Unable to close FileWriter!", e);
			throw new RuntimeException(e);
		}
	}
}
