package eu.openaire.doc_urls_retriever.util.file;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimaps;
import eu.openaire.doc_urls_retriever.exceptions.DocFileNotRetrievedException;
import eu.openaire.doc_urls_retriever.util.url.QuadrupleToBeLogged;
import eu.openaire.doc_urls_retriever.util.url.UrlUtils;
import org.json.JSONException;
import org.json.JSONObject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.commons.io.FileUtils.deleteDirectory;


/**
 * @author Lampros A. Smyrnaios
 */
public class FileUtils
{
	private static final Logger logger = LoggerFactory.getLogger(FileUtils.class);
	
	private static Scanner inputScanner;
	private static PrintStream printStream;
	private static int fileIndex = 0;	// Index in the input file
	public static final boolean skipFirstRow = false;	// Use this to skip the HeaderLine in a csv-kindOf-File.
	private static String endOfLine = "\n";
	public static int unretrievableInputLines = 0;	// For better statistics in the end.
    public static int unretrievableUrlsOnly = 0;
    public static int groupCount = 300;
    public static int maxStoringWaitingTime = 30000;	// 30sec
	
	public static final List<QuadrupleToBeLogged> quadrupleToBeLoggedOutputList = new ArrayList<>();
	
	public static final HashMap<String, Integer> numbersOfDuplicateDocFileNames = new HashMap<String, Integer>();	// Holds docFileNa,es with their duplicatesNum.
	
	public static boolean shouldDownloadDocFiles = true;
	public static final boolean shouldDeleteOlderDocFiles = false;	// Should we delete any older stored docFiles? This is useful for testing.
	public static final boolean shouldUseOriginalDocFileNames = false;
	public static final boolean shouldLogFullPathName = true;	// Should we log, in the jasonOutputFile, the fullPathName or just the ending fileName?
	public static int numOfDocFile = 0;	// In the case that we don't care for original docFileNames, the fileNames are produced using an incremential system.
	public static String storeDocFilesDir = System.getProperty("user.dir") + "//docFiles";
	public static int unretrievableDocNamesNum = 0;	// Num of docFiles for which we were not able to retrieve their docName.
	public static final Pattern FILENAME_FROM_CONTENT_DISPOSITION_FILTER = Pattern.compile(".*(?:filename=(?:\\\")?)([\\w\\-\\.\\%\\_]+)[\\\"\\;]*.*");
	
	
	public FileUtils(InputStream input, OutputStream output)
	{
		FileUtils.inputScanner = new Scanner(input);
		FileUtils.printStream = new PrintStream(output);
		
		if ( shouldDownloadDocFiles ) {
			try {
				File dir = new File(storeDocFilesDir);
				if ( shouldDeleteOlderDocFiles ) {
					logger.debug("Deleting old docFiles..");
					deleteDirectory(dir);	// apache.commons.io.FileUtils
				}
				
				// If the directory doesn't exist, try to (re)create it.
				if ( !dir.exists() ) {
					if ( !dir.mkdir() ) {   // Create the directory.
						String errorMessage = "Problem when creating the \"storeDocFilesDir\": \"" + FileUtils.storeDocFilesDir + "\"."
								+ "\nThe docFiles will NOT be stored, but the docUrls will be retrieved and kept in the outputFile."
								+ "\nIf this is not desired, please terminate the program and re-define the \"storeDocFilesDir\"!";
						System.err.println(errorMessage);
						logger.error(errorMessage);
						FileUtils.shouldDownloadDocFiles = false;
					}
				}
			} catch (Exception e) {
				logger.error("Problem when deleting directory: " + storeDocFilesDir, e);
				FileUtils.shouldDownloadDocFiles = false;
			}
		}
	}
	
	
	/**
	 * This method returns the number of (non-heading, non-empty) lines we have read from the inputFile.
	 * @return loadedUrls
	 */
	public static int getCurrentlyLoadedUrls()	// In the end, it gives the total number of urls we have processed.
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
	public static HashMultimap<String, String> getNextIdUrlPairGroupFromJson()
	{
		HashMap<String, String> inputIdUrlPair;
		HashMultimap<String, String> idAndUrlMappedInput = HashMultimap.create();
		
		int curBeginning = FileUtils.fileIndex;
		
		while ( (inputScanner.hasNextLine()) && (FileUtils.fileIndex < (curBeginning + groupCount)) )// While (!EOF) iterate through lines.
		{
			//logger.debug("fileIndex: " + FileUtils.fileIndex);	// DEBUG!
			
			// Take each line, remove potential double quotes.
			String retrievedLineStr = inputScanner.nextLine();
			//logger.debug("Loaded from inputFile: " + retrievedLineStr);	// DEBUG!
			
			FileUtils.fileIndex ++;
			
			if ( retrievedLineStr.isEmpty() ) {
				FileUtils.unretrievableInputLines ++;
				continue;
			}
			
			inputIdUrlPair =  jsonDecoder(retrievedLineStr);// Decode the jsonLine and take the two attributes.
			if ( inputIdUrlPair == null ) {
				logger.warn("A problematic inputLine found: \"" + retrievedLineStr + "\"");
				FileUtils.unretrievableInputLines ++;
				continue;
			}
			
			idAndUrlMappedInput.putAll(Multimaps.forMap(inputIdUrlPair));    // Keep mapping to be put in the outputFile later.
		}
		
		return idAndUrlMappedInput;
	}
	
	
	/**
	 * This method decodes a Jason String into its members.
	 * @param jsonLine String
	 * @return HashMap<String,String>
	 */
	public static HashMap<String, String> jsonDecoder(String jsonLine)
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
		
        if ( urlStr.isEmpty() ) {
			if ( idStr.isEmpty() )	// Allow one of them to be empty but not both. If ID is empty, then we still don't lose the URL.
				return null;	// If url is empty, we will still see the ID in the output and possible find its missing URL later.
			else
				FileUtils.unretrievableUrlsOnly ++;    // Keep track of lines with an id, but, with no url.
		}
        
		returnIdUrlMap.put(idStr, urlStr);
		
		return returnIdUrlMap;
	}
	
	
	/**
	 * This function writes new source-doc URL set in the output file.
	 * Each time it's finished writing, it flushes the write stream and clears the urlTable.
	 */
	public static void writeToFile()
	{
		int numberOfTriples = FileUtils.quadrupleToBeLoggedOutputList.size();
		StringBuilder strB = new StringBuilder(numberOfTriples * 350);  // 350: the maximum expected length for a source-doc-error triple..

		for ( QuadrupleToBeLogged quadruple : FileUtils.quadrupleToBeLoggedOutputList)
		{
			strB.append(quadruple.toJsonString());
			strB.append(endOfLine);
		}
		
		printStream.print(strB.toString());
		printStream.flush();
		
		FileUtils.quadrupleToBeLoggedOutputList.clear();	// Clear to keep in memory only <groupCount> values at a time.
		
		logger.debug("Finished writing to the outputFile.. " + numberOfTriples + " set(s) of (\"SourceUrl\", \"DocUrl\")");
	}
	
	
	/**
	 * This method is responsible for storing the docFiles and store them in permanent storage.
	 * @param inStream
	 * @param docUrl
	 * @param contentDisposition
	 * @throws DocFileNotRetrievedException
	 */
	public static String storeDocFile(InputStream inStream, String docUrl, String contentDisposition) throws DocFileNotRetrievedException
	{
		File docFile;
		FileOutputStream outStream = null;
		try {
			if ( FileUtils.shouldUseOriginalDocFileNames)
				docFile = getDocFileWithOriginalFileName(docUrl, contentDisposition);
			else
				docFile = new File(storeDocFilesDir + File.separator + (numOfDocFile++) + ".pdf");	// TODO - Later, on different fileTypes, take care of the extension properly.
			
			outStream = new FileOutputStream(docFile);
			
			int bytesRead = -1;
			byte[] buffer = new byte[3145728];	// 3Mb (average docFiles-size)
			long startTime = System.nanoTime();
			while ( (bytesRead = inStream.read(buffer)) != -1 )
			{
				long elapsedTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
				if ( (elapsedTime > FileUtils.maxStoringWaitingTime) || (elapsedTime == Long.MIN_VALUE) ) {
					logger.warn("Storing docFile from docUrl: \"" + docUrl + "\" took over "+ TimeUnit.MILLISECONDS.toSeconds(FileUtils.maxStoringWaitingTime) + "secs!");
					if ( !docFile.delete() )
						logger.error("Error when deleting the half-retrieved file from docUrl: " + docUrl);
					numOfDocFile --;	// Revert number, as this docFile was not retrieved.
					throw new DocFileNotRetrievedException();
				}
				else
					outStream.write(buffer, 0, bytesRead);
			}
			//logger.debug("Elapsed time for storing: " + TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime));
			
			if ( FileUtils.shouldLogFullPathName )
				return docFile.getAbsolutePath();	// Return the fullPathName.
			else
				return docFile.getName();	// Return just the fileName.
			
		} catch (DocFileNotRetrievedException dfnre) {
			throw dfnre;
		} catch (Exception ioe) {
			logger.warn("", ioe);
			throw new DocFileNotRetrievedException();
		} finally {
			try {
				if ( inStream != null )
					inStream.close();
			} catch (Exception e) {
				logger.error("", e);
			}
			try {
				if ( outStream != null )
					outStream.close();
			} catch (Exception e) {
				logger.error("", e);
			}
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
		else if ( (docFileName = UrlUtils.getDocIdStr(docUrl)) == null )	// Extract fileName from docUrl.
				hasUnretrievableDocName = true;
		
		String dotFileExtension /*= "";
		if ( shouldAcceptOnlyPDFs )
			dotFileExtension*/ = ".pdf";
		/*else {	// TODO - Later we might accept also other fileTypes.
			if ( contentType != null )
				// Use the Content-Type to determine the extension. A multi-mapping between mimeTypes and fileExtensions is needed.
			else
				// Use the subString as last resort, although not reliable! (subString after the last "." will not work if the fileName doesn't include an extension).
		}*/
		
		if ( hasUnretrievableDocName )
			docFileName = "unretrievableDocName(" + (++unretrievableDocNamesNum) + ")" + dotFileExtension;
		
		try {
			if ( !hasUnretrievableDocName && !docFileName.contains(dotFileExtension) )	// If there is no extension, add ".pdf" in the end. TODO - Later it can be extension-dependent.
				docFileName += dotFileExtension;
			
			String saveDocFileFullPath = storeDocFilesDir + File.separator + docFileName;
			File docFile = new File(saveDocFileFullPath);
			
			if ( !hasUnretrievableDocName ) {	// If we retrieved the fileName, go check if it's a duplicate.
				
				boolean isDuplicate = false;
				int curDuplicateNum = 1;
				
				if ( numbersOfDuplicateDocFileNames.containsKey(docFileName) ) {	// First check -in O(1)- if it's an already-known duplicate.
					curDuplicateNum += numbersOfDuplicateDocFileNames.get(docFileName);
					isDuplicate = true;
				} else if ( docFile.exists() )	// If it's not an already-known duplicate, go check if it exists in the fileSystem.
					isDuplicate = true;
				
				if ( isDuplicate ) {
					numbersOfDuplicateDocFileNames.put(docFileName, curDuplicateNum);
					
					// Construct final-DocFileName by renaming.
					String preExtensionFileName = docFileName.substring(0, docFileName.lastIndexOf(".") - 1);
					String newDocFileName = preExtensionFileName + "(" + curDuplicateNum + ")" + dotFileExtension;
					saveDocFileFullPath = storeDocFilesDir + File.separator + newDocFileName;
					File renamedDocFile = new File(saveDocFileFullPath);
					if ( !docFile.renameTo(renamedDocFile) ) {
						logger.error("Renaming operation of \"" + docFileName + "\" to \"" + newDocFileName + "\" has failed!");
						throw new DocFileNotRetrievedException();
					}
				}
			}
			
			FileUtils.numOfDocFile ++;
			return docFile;
			
		} catch (DocFileNotRetrievedException dfnre) {
			throw dfnre;
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
		int curBeginning = FileUtils.fileIndex;
		
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
