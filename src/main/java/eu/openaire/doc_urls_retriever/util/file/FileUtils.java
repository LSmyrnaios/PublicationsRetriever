package eu.openaire.doc_urls_retriever.util.file;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import eu.openaire.doc_urls_retriever.exceptions.DocFileNotRetrievedException;
import eu.openaire.doc_urls_retriever.util.url.TripleToBeLogged;
import eu.openaire.doc_urls_retriever.util.url.UrlUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.commons.io.FileUtils.deleteDirectory;
import static org.apache.commons.io.FileUtils.writeByteArrayToFile;


/**
 * @author Lampros A. Smyrnaios
 */
public class FileUtils
{
	private static final Logger logger = LoggerFactory.getLogger(FileUtils.class);
	
	private static Scanner inputScanner;
	private static PrintStream printStream;
	//public static HashMap<String, String> idAndUrlMappedInput = new HashMap<String, String>();	// Contains the mapped key(id)-value(url) pairs.
	private static long fileIndex = 0;	// Index in the input file
	public static boolean skipFirstRow = true;
	private static String endOfLine = "\n";
	public static long unretrievableInputLines = 0;	// For better statistics in the end.
    public static long unretrievableUrlsOnly = 0;
    public static int groupCount = 5000;	// Just for testing.. TODO -> Later increase it..
	
	public static List<TripleToBeLogged> tripleToBeLoggedOutputList = new ArrayList<>();
	
	public static final HashMap<String, Integer> numbersOfDuplicateDocFileNames = new HashMap<String, Integer>();	// Holds docFileNa,es with their duplicatesNum.
	
	public static boolean shouldDownloadDocFiles = true;
	public static final boolean shouldDeleteOlderDocFiles = true;	// Should we delete any older stored docFiles? This is useful for testing.
	public static final boolean shouldUseOriginalDocFileNames = false;
	public static final boolean shouldLogFullPathName = false;	// Should we log, in the jasonOutputFile, the fullPathName or just the ending fileName?
	public static int numOfDocFile = 0;	// In the case that we don't care for original docFileNames, the fileNames are produced using an incremential system.
	public static String docFilesDownloadPath = "//media//lampros//HDD2GB//downloadedDocFiles";
	public static long unretrievableDocNamesNum = 0;	// Num of docFiles for which we were not able to retrieve their docName.
	public static final Pattern FILENAME_FROM_CONTENT_DISPOSITION_FILTER = Pattern.compile(".*(?:filename=(?:\\\")?)([\\w\\-\\.\\%\\_]+)[\\\"\\;]*.*");
	
	
	public FileUtils(InputStream input, OutputStream output)
	{
		FileUtils.inputScanner = new Scanner(input);
		FileUtils.printStream = new PrintStream(output);
		
		if ( shouldDownloadDocFiles ) {
			try {
				File dir = new File(docFilesDownloadPath);
				if ( shouldDeleteOlderDocFiles ) {
					logger.debug("Deleting old docFiles..");
					deleteDirectory(dir);	// apache.commons.io.FileUtils
				}
				
				// If the directory doesn't exist, try to (re)create it.
				if ( !dir.exists() ) {
					if ( !dir.mkdir() ) {   // Create the directory.
						logger.error("Problem when creating the dir: " + docFilesDownloadPath);
						FileUtils.shouldDownloadDocFiles = false;
					}
				}
			} catch (Exception e) {
				logger.error("Problem when deleting directory: " + docFilesDownloadPath, e);
				FileUtils.shouldDownloadDocFiles = false;
			}
		}
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
		skipFirstRow = false;	// Make sure we don't use this rule for any calculations.
		
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
	 * @param comment String
	 * @return jsonString
	 */
	public static String jsonEncoder(String sourceUrl, String docUrl, String comment)
	{
		JSONArray jsonArray;
		try {
			JSONObject firstJsonObject = new JSONObject().put("sourceUrl", sourceUrl);
			JSONObject secondJsonObject = new JSONObject().put("docUrl", docUrl);
			JSONObject thirdJsonObject = new JSONObject().put("comment", comment);	// The comment will be empty, if there is no error or if there is no docFileName.
			
			// Care about the order of the elements by using a JSONArray (otherwise it's uncertain which one will be where).
			jsonArray = new JSONArray().put(firstJsonObject).put(secondJsonObject).put(thirdJsonObject);
		} catch (Exception e) {	// If there was an encoding problem.
			logger.error("Failed to encode jsonLine: \"" + sourceUrl + ", " + docUrl + ", " + comment + "\"", e);
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
		
		logger.debug("Finished writing to the outputFile.. " + numberOfTriples + " set(s) of (\"SourceUrl\", \"DocUrl\")");
	}
	
	
	/**
	 * This method is responsible for storing the docFiles and store them in permanent storage.
	 * @param contentData
	 * @param docUrl
	 * @param contentDisposition
	 * @throws DocFileNotRetrievedException
	 */
	public static String storeDocFile(byte[] contentData, String docUrl, String contentDisposition) throws DocFileNotRetrievedException
	{
		if ( contentData.length == 0 )
			throw new DocFileNotRetrievedException();
		
		File docFile;
		try {
			if ( FileUtils.shouldUseOriginalDocFileNames)
				docFile = getDocFileWithOriginalFileName(docUrl, contentDisposition);
			else
				docFile = new File(docFilesDownloadPath + File.separator + (numOfDocFile++) + ".pdf");	// TODO - Later, on different fileTypes, take care of the extension properly.
			
			writeByteArrayToFile(docFile, contentData, 0, contentData.length, false);	// apache.commons.io.FileUtils
			
			if ( FileUtils.shouldLogFullPathName )
				return docFile.getAbsolutePath();	// Return the fullPathName.
			else
				return docFile.getName();	// Return just the fileName.
			
		} catch (DocFileNotRetrievedException dfnre) {
			throw dfnre;
		} catch (Exception ioe) {
			logger.warn("", ioe);
			throw new DocFileNotRetrievedException();
		}
	}
	
	
	public static File getDocFileWithOriginalFileName(String docUrl, String contentDisposition) throws  DocFileNotRetrievedException
	{
		String docFileName = null;
		boolean hasUnretrievableDocName = false;
		
		if ( contentDisposition != null ) {	// Extract docFileName from contentDisposition.
			Matcher fileNameMatcher = FILENAME_FROM_CONTENT_DISPOSITION_FILTER.matcher(contentDisposition);
			if ( fileNameMatcher.matches() ) {
				docFileName = fileNameMatcher.group(1);	// Group<1> is the fileName.
				if ( docFileName == null || docFileName.isEmpty() )
					hasUnretrievableDocName = true;
			}
			else {
				logger.warn("Unmatched Content-Disposition:  " + contentDisposition);
				hasUnretrievableDocName = true;
			}
		}
		else if ( (docFileName = UrlUtils.getDocIdStr(docUrl)) == null ) // Extract fileName from docUrl.
				hasUnretrievableDocName = true;
		
		String dotFileExtension /*= "";
		if ( shouldAcceptOnlyPDFs )
			dotFileExtension*/ = ".pdf";
		/*else {	// TODO - Later we might accept also other fileTypes.
			if ( contentyType != null )
				// Use the Content-Type to determine the extension. A multi-mapping between mimeTypes and fileExtensions is needed.
			else
				// Use the subString as last resort, although not reliable! (subString after the last "." will not work if the fileName doesn't include an extension).
		}*/
		
		if ( hasUnretrievableDocName )
			docFileName = "unretrievableDocName(" + (++unretrievableDocNamesNum) + ")" + dotFileExtension;
		
		try {
			if ( !hasUnretrievableDocName && !docFileName.contains(dotFileExtension) )	// If there is no extension, add ".pdf" in the end. TODO - Later it can be extension-dependent.
				docFileName += dotFileExtension;
			
			String saveDocFileFullPath = docFilesDownloadPath + File.separator + docFileName;
			File docFile = new File(saveDocFileFullPath);
			
			if ( !hasUnretrievableDocName ) {	// If we retrieved the fileName, go check if it's a duplicate.
				
				boolean isDuplicate = false;
				int curDuplicateNum = 1;
				
				if ( numbersOfDuplicateDocFileNames.containsKey(docFileName) ) {	// First check -in O(1)- if it's an already-known duplicate.
					curDuplicateNum += numbersOfDuplicateDocFileNames.get(docFileName);
					isDuplicate = true;
				}
				else if ( docFile.exists() )	// If it's not an already-known duplicate, go check if it exists in the fileSystem.
					isDuplicate = true;
				
				if ( isDuplicate ) {
					numbersOfDuplicateDocFileNames.put(docFileName, curDuplicateNum);
					
					// Construct final-DocFileName by renaming.
					String preExtensionFileName = docFileName.substring(0, docFileName.lastIndexOf(".") - 1);
					String newDocFileName = preExtensionFileName + "(" + curDuplicateNum + ")" + dotFileExtension;
					saveDocFileFullPath = docFilesDownloadPath + File.separator + newDocFileName;
					File renamedDocFile = new File(saveDocFileFullPath);
					if ( !docFile.renameTo(renamedDocFile) ) {
						logger.error("Renaming operation of \"" + docFileName + "\" to \"" + newDocFileName + "\" has failed!");
						throw new DocFileNotRetrievedException();
					}
				}
			}
			
			return docFile;
			
		} catch (Exception ioe) {
			logger.warn("", ioe);
			throw new DocFileNotRetrievedException();
		}
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
			
			if ( (FileUtils.fileIndex == 1) && skipFirstRow )
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
