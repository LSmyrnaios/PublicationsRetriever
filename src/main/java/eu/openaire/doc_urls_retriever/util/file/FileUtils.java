package eu.openaire.doc_urls_retriever.util.file;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.HashSet;
import java.util.Collection;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;



public class FileUtils
{
	private static final Logger logger = LogManager.getLogger(FileUtils.class);
	
	private static Scanner inputScanner;
	private static File inputFile;
	private static File outputFile;
	public static HashMap<String, String> idAndUrlMappedInput = new HashMap<String, String>();	// Contains the mapped key(id)-value(url) pairs.
	public static HashMap<String,String> outputEntries = new HashMap<String,String>();
	private static long fileIndex = 0;	// Index in the input file
	public static boolean skipFirstRow = false;
	private static FileWriter writer;
	//private static String outputDelimiter = "\t";
	private static String endOfLine = "\n";
	public static long unretrievableInputLines = 0;	// For better statistics in the end.
    public static long unretrievableUrlsOnly = 0;
    public static int groupCount = 1000;	// Just for testing.. TODO -> Later increase it..



    /**
     * This method returns the number of (non-heading, non-empty) lines we have read from the inputFile.
     * @return loadedUrls
     */
	public static long getCurrentlyLoadedUrls()	// In the end, it gives the total number of urls we have processed.
	{
		if ( FileUtils.skipFirstRow )
			return FileUtils.fileIndex - FileUtils.unretrievableInputLines - FileUtils.unretrievableUrlsOnly -1; // -1 to exclude the first line
		else
			return FileUtils.fileIndex - FileUtils.unretrievableInputLines - FileUtils.unretrievableUrlsOnly;
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
		} catch (Exception e) {
			logger.error(e);
			throw new RuntimeException(e);	// If any of the files is not found.. we should NOT continue..
		}
	}
	
	
	/**
	 * This method parses a testFile with one-url-per-line and extracts the urls.
	 * @return Collection<String>
	 */
	public static Collection<String> getNextUrlGroupTest()
	{
		Collection<String> urlGroup = new HashSet<String>();
		
		// Take a group of <groupCount> urls from the file..
		// If we are at the end and there are less than <groupCount>.. take as many as there are..
		
		//logger.debug("Retrieving the next group of " + groupCount + " elements from the inputFile.");
		long curBeginning = FileUtils.fileIndex;

		while ( (inputScanner.hasNextLine()) && (FileUtils.fileIndex < (curBeginning + groupCount)) )
		{// While (!EOF) iterate through lines.

			// Take each line, remove potential double quotes.
			String retrievedLineStr = StringUtils.replace(inputScanner.nextLine(), "\"", "");

			FileUtils.fileIndex ++;

			if ( (FileUtils.fileIndex == 0) && skipFirstRow )
				continue;

			if ( retrievedLineStr.isEmpty() ) {
				FileUtils.unretrievableInputLines ++;
				continue;
			}

			//logger.debug("Loaded from inputFile: " + retrievedLineStr);	// DEBUG!

			urlGroup.add(retrievedLineStr);
		}
		//logger.debug("FileUtils.fileIndex's value after taking urls after " + FileUtils.fileIndex / groupCount + " time(s), from input file: " + FileUtils.fileIndex);	// DEBUG!

		return urlGroup;
	}


	/**
	 * This method parses a Json file and extracts the urls, along with the IDs.
	 * @return Collection<String>
	 */
	public static Collection<String> getNextUrlGroupFromJson()
	{
		HashMap<String, String> inputIdUrlPair;
		Collection<String> urlGroup = new HashSet<String>();

		long curBeginning = FileUtils.fileIndex;

		while ( (inputScanner.hasNextLine()) && (FileUtils.fileIndex < (curBeginning + groupCount)) )// While (!EOF) iterate through lines.
		{
			//logger.debug("fileIndex: " + FileUtils.fileIndex);	// DEBUG

			// Take each line, remove potential double quotes.
			String retrievedLineStr = inputScanner.nextLine();

			logger.debug("Loaded from inputFile: " + retrievedLineStr);	// DEBUG!

			FileUtils.fileIndex ++;

			if ( (FileUtils.fileIndex == 0) && skipFirstRow )
				continue;

			if (retrievedLineStr.isEmpty()) {
				FileUtils.unretrievableInputLines ++;
				continue;
			}

			inputIdUrlPair = jsonDecoder(retrievedLineStr); // Decode the jsonLine and take the two attributes.
			if ( inputIdUrlPair == null ) {
				logger.warn("A problematic inputLine found: \"" + retrievedLineStr + "\"");
				FileUtils.unretrievableInputLines ++;
				continue;
			}
			idAndUrlMappedInput.putAll(inputIdUrlPair);    // Keep mapping to be put in the outputFile later..

			urlGroup.addAll(inputIdUrlPair.values());	// Make sure the our returning's source is the temporary collection (other wise we go into an infinite loop).
		}

		return urlGroup;	// Return just the urls to be crawled. We still keep the IDs.
	}


	/**
	 * This method decodes a Jason String into its members.
	 * @param jsonLine String
	 * @return HashMap<String,String>
	 */
	public static HashMap<String,String> jsonDecoder(String jsonLine)
	{
		HashMap<String, String> returnIdUrlMap = new HashMap<String, String>();

		JSONObject jObj = new JSONObject(jsonLine); // Construct a JSONObject from the retrieved jsonLine.

		// Get ID and url and put them in the HashMap
		String idStr = null;
		String urlStr = null;
		try {
			idStr = jObj.get("id").toString();
			urlStr = jObj.get("url").toString();
		} catch (JSONException je) {
			logger.warn("JSONException caught when tried to retrieve values from jsonLine: \"" + jsonLine + "\"", je);
			return null;
		}

		if ( idStr.isEmpty() && urlStr.isEmpty() )	// Allow one of them to be empty but not both. If ID is empty, then we still don't lose the URL.
			return null;	// If url is empty, we will still see the ID in the output and possible find its missing URL later.
        else if ( urlStr.isEmpty() )    // Keep track of lines with an id, but, with no url.
            FileUtils.unretrievableUrlsOnly++;

		returnIdUrlMap.put(idStr, urlStr);

		return returnIdUrlMap;
	}


	/**
	 * This method encodes json members into a Json object and returns its String representation..
	 * @param sourceUrl String
	 * @param docUrl String
	 * @param errorCause String
	 * @return String
	 */
	public static String jsonEncoder(String sourceUrl, String docUrl, String errorCause)
	{
		JSONObject firstJsonObject = new JSONObject().put("sourceUrl", sourceUrl);
        JSONObject secondJsonObject = new JSONObject().put("docUrl", docUrl);

        // Care about the order of the elements by using a JSONArray (otherwise it's uncertain which one will be first).
        JSONArray jsonArray = new JSONArray().put(firstJsonObject).put(secondJsonObject);

        // TODO - Later add the errorCause, if the docUrl is not found.
		/*if ( errorCause != null ) {   // It will be null if there is no error.

		    JSONObject thirdJsonObject = new JSONObject().put("errorCause", errorCause);
		    jsonArray.put(thirdJsonObject);
		}*/

		return jsonArray.toString();	// Return the jsonLine.
	}
	
	
	/**
	 * This function writes new source-doc URL set in the output file.
	 * Each time it's finished writing, it flushes the write stream and clears the urlTable.
	 */
	public static void writeToFile()
	{
		int numberOfEntries = outputEntries.size();
		logger.debug("Writing to the outputFile.. " + numberOfEntries + " set(s) of (\"SourceUrl\", \"DocUrl\")");
		StringBuilder strB = new StringBuilder(numberOfEntries * 300);  // 300: the maximum expected length for a source-doc-error triple..

		for ( Entry<String,String> entry : outputEntries.entrySet() )
		{
			strB.append(jsonEncoder(entry.getKey(), entry.getValue(), null));

			/*
			 * For an output in csv/tsv.
			strB.append(entry.getKey());
			strB.append(outputDelimiter);
			strB.append(entry.getValue());*/

			strB.append(endOfLine);
		}

		strB.deleteCharAt(strB.length() -1);    // Remove the last endOfLine.

		try {
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
