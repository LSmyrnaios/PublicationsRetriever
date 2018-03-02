package eu.openaire.doc_urls_retriever.util.file;

import java.io.*;

import java.util.*;

import eu.openaire.doc_urls_retriever.crawler.TripleToBeLogged;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author Lampros A. Smyrnaios
 */
public class FileUtils
{
	private static final Logger logger = LoggerFactory.getLogger(FileUtils.class);

	private static Scanner inputScanner;
	private static PrintStream printStream;
	public static HashMap<String, String> idAndUrlMappedInput = new HashMap<String, String>();	// Contains the mapped key(id)-value(url) pairs.
	private static long fileIndex = 0;	// Index in the input file
	public static boolean skipFirstRow = false;
	private static String endOfLine = "\n";
	public static long unretrievableInputLines = 0;	// For better statistics in the end.
    public static long unretrievableUrlsOnly = 0;
    public static int groupCount = 1000;	// Just for testing.. TODO -> Later increase it..

	public static List<TripleToBeLogged> tripleToBeLoggedOutputList = new ArrayList<>();
	
	
	
	public FileUtils(InputStream input, OutputStream output)
	{
    	logger.debug("Input: " + input.toString());
    	logger.debug("Output: " + output.toString());
    	
		FileUtils.inputScanner = new Scanner(input);
		FileUtils.printStream = new PrintStream(output);
	}
	
	
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
			//logger.debug("fileIndex: " + FileUtils.fileIndex);	// DEBUG!
			
			// Take each line, remove potential double quotes.
			String retrievedLineStr = inputScanner.nextLine();
			//logger.debug("Loaded from inputFile: " + retrievedLineStr);	// DEBUG!
			
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
			
			// TODO - Find a way to keep track of the redirections over the input pages, in order to match the id of the original-input-docPage, with the redirected-final-docPage.
			// So that the docUrl in the output will match to the ID of the inputDocPage.
			// Currently there is no control over the correlation of pre-redirected pages and after-redirected ones, as Crawler4j, which handles this process doesn't keep track of such thing.
			// So the output currently contains the redirected-final-docPages with their docUrls.
			
			//idAndUrlMappedInput.putAll(inputIdUrlPair);    // Keep mapping to be put in the outputFile later.
			
			urlGroup.addAll(inputIdUrlPair.values());	// Make sure that our returning's source is the temporary collection (otherwise we go into an infinite loop).
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
	 * @return jsonString
	 */
	public static String jsonEncoder(String sourceUrl, String docUrl, String errorCause)
	{
		JSONArray jsonArray;
		try {
			JSONObject firstJsonObject = new JSONObject().put("sourceUrl", sourceUrl);
			JSONObject secondJsonObject = new JSONObject().put("docUrl", docUrl);
			JSONObject thirdJsonObject = new JSONObject().put("errorCause", errorCause);	// The errorCause will be empty, if there is no error.

			// Care about the order of the elements by using a JSONArray (otherwise it's uncertain which one will be where).
			jsonArray = new JSONArray().put(firstJsonObject).put(secondJsonObject).put(thirdJsonObject);
		} catch (Exception e) {	// If there was an encoding problem.
			logger.error("Failed to encode jsonLine!", e);
			return null;
		}

		return jsonArray.toString();	// Return the jsonLine.
	}
	
	
	/**
	 * This function writes new source-doc URL set in the output file.
	 * Each time it's finished writing, it flushes the write stream and clears the urlTable.
	 */
	public static void writeToFile()
	{
		int numberOfTriples = FileUtils.tripleToBeLoggedOutputList.size();
		logger.debug("Writing to the outputFile.. " + numberOfTriples + " set(s) of (\"SourceUrl\", \"DocUrl\")");
		StringBuilder strB = new StringBuilder(numberOfTriples * 350);  // 350: the maximum expected length for a source-doc-error triple..

		String tempJsonString = null;

		for ( TripleToBeLogged triple : FileUtils.tripleToBeLoggedOutputList)
		{
            tempJsonString = triple.toJsonString();
			if ( tempJsonString == null )	// If there was an encoding error, move on..
				continue;
			
			strB.append(tempJsonString);
			strB.append(endOfLine);
		}
		
		printStream.print(strB.toString());
		printStream.flush();
		
		FileUtils.tripleToBeLoggedOutputList.clear();	// Clear to keep in memory only <groupCount> values at a time.
	}
	
	
	/**
	 * Closes open Streams.
	 */
	public static void closeStreams()
	{
        inputScanner.close();
		printStream.close();
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

}
