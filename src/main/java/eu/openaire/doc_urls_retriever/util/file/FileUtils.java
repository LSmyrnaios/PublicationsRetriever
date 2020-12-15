package eu.openaire.doc_urls_retriever.util.file;

import ch.qos.logback.classic.LoggerContext;
import com.google.common.collect.HashMultimap;
import eu.openaire.doc_urls_retriever.DocUrlsRetriever;
import eu.openaire.doc_urls_retriever.crawler.MachineLearning;
import eu.openaire.doc_urls_retriever.exceptions.DocFileNotRetrievedException;
import eu.openaire.doc_urls_retriever.util.url.LoaderAndChecker;
import eu.openaire.doc_urls_retriever.util.url.QuadrupleToBeLogged;
import eu.openaire.doc_urls_retriever.util.url.UrlUtils;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.commons.io.FileUtils.deleteDirectory;


/**
 * @author Lampros Smyrnaios
 */
public class FileUtils
{
	private static final Logger logger = LoggerFactory.getLogger(FileUtils.class);
	
	private static Scanner inputScanner = null;
	private static PrintStream printStream = null;
	
	public static long numOfLines;
	
	public static final int jsonBatchSize = 300;
	
	private static final StringBuilder strB = new StringBuilder(jsonBatchSize * 500);  // 500: the usual-maximum-expected-length for an <id-sourceUrl-docUrl-comment> quadruple.
	
	private static int fileIndex = 0;	// Index in the input file
	public static boolean skipFirstRow = false;	// Use this to skip the HeaderLine in a csv-kindOf-File.
	public static final String endOfLine = System.lineSeparator();
	public static int unretrievableInputLines = 0;	// For better statistics in the end.
    public static int unretrievableUrlsOnly = 0;
    public static final int maxStoringWaitingTime = 45000;	// 45sec (some files can take several minutes or even half an hour)
	
	public static final List<QuadrupleToBeLogged> quadrupleToBeLoggedList = new ArrayList<>(jsonBatchSize);
	
	public static final HashMap<String, Integer> numbersOfDuplicateDocFileNames = new HashMap<String, Integer>();	// Holds docFileNa,es with their duplicatesNum.
	
	public static boolean shouldDownloadDocFiles = false;	// It will be set to "true" if the related command-;ine-argument is given.
	public static final boolean shouldDeleteOlderDocFiles = false;	// Should we delete any older stored docFiles? This is useful for testing.
	public static boolean shouldUseOriginalDocFileNames = false;	// Use number-fileNames by default.
	public static final boolean shouldLogFullPathName = true;	// Should we log, in the jasonOutputFile, the fullPathName or just the ending fileName?
	public static int numOfDocFile = 0;	// In the case that we don't care for original docFileNames, the fileNames are produced using an incremential system.
	public static final String workingDir = System.getProperty("user.dir") + File.separator;
	public static String storeDocFilesDir = workingDir + "docFiles" + File.separator;
	public static int unretrievableDocNamesNum = 0;	// Num of docFiles for which we were not able to retrieve their docName.
	public static final Pattern FILENAME_FROM_CONTENT_DISPOSITION_FILTER = Pattern.compile(".*(?:filename(?:\\*)?=(?:.*(?:\\\"|\\'))?)([^\\\"^\\;]+)[\\\"\\;]*.*");
	
	public static final int MAX_FILENAME_LENGTH = 250;	// TODO - Find a way to get the current-system's MAX-value.
	
	public static String fullInputFilePath = null;	// Used when the MLA is enabled and we want to count the number of lines in the inputFile, in order to optimize the M.L.A.'s execution.
	
	
	public FileUtils(InputStream input, OutputStream output)
	{
		FileUtils.inputScanner = new Scanner(input);
		
		if ( MachineLearning.useMLA ) {	// In case we are using the MLA, go get the numOfLines to be used.
			numOfLines = getInputNumOfLines();
			logger.debug("Num of lines in the inputFile: " + numOfLines);
		}
		
		FileUtils.printStream = new PrintStream(output);
		
		if ( shouldDownloadDocFiles ) {
			File dir = new File(storeDocFilesDir);
			if ( shouldDeleteOlderDocFiles ) {
				logger.info("Deleting old docFiles..");
				try {
					deleteDirectory(dir);	// org.apache.commons.io.FileUtils
				} catch (IOException ioe) {
					logger.error(ioe.getMessage(), ioe);
					FileUtils.shouldDownloadDocFiles = false;	// Continue without downloading the docFiles, just create the jsonOutput.
					return;
				}
			}
			
			// If the directory doesn't exist, try to (re)create it.
			try {
				if ( !dir.exists() ) {
					if ( !dir.mkdir() ) {   // Create the directory.
						String errorMessage;
						if ( DocUrlsRetriever.docFilesStorageGivenByUser )
							errorMessage = "Problem when creating the \"storeDocFilesDir\": \"" + FileUtils.storeDocFilesDir + "\"."
									+ "\nPlease give a valid Directory-path.";
						else	// User leaves the storageDir to be the default one.
							errorMessage = "Problem when creating the \"storeDocFilesDir\": \"" + FileUtils.storeDocFilesDir + "\"."
									+ "\nThe docFiles will NOT be stored, but the docUrls will be retrieved and kept in the outputFile."
									+ "\nIf this is not desired, please terminate the program and re-define the \"storeDocFilesDir\"!";
						System.err.println(errorMessage);
						logger.error(errorMessage);
						FileUtils.closeIO();
						System.exit(-3);
					}
				}
			} catch (SecurityException se) {
				logger.error(se.getMessage(), se);
				logger.warn("There was an error creating the docFiles-storageDir! Continuing without downloading the docFiles, while creating the jsonOutput with the docUrls.");
				FileUtils.shouldDownloadDocFiles = false;
			}
		}
	}
	
	
	public static long getInputNumOfLines()
	{
		long lineCount = 0;
		
		// Create a new file to write the input-data.
		// Since the input-stream is not reusable.. we cant count the line AND use the data..
		// (the data will be consumed and the program will exit with error since no data will be read from the inputFile).
		String baseFileName = workingDir + "inputFile";
		String extension;
		if ( LoaderAndChecker.useIdUrlPairs )
			extension = ".json";
		else
			extension = ".csv";	// This is just a guess, it may be a ".tsv" as well or sth else. There is no way to know what type of file was assigned in the "System.in".
		
		// Make sure we create a new distinct file which will not replace any other existing file. Tis new file will get deleted in the end.
		int fileNum = 1;
		fullInputFilePath = baseFileName + extension;
		File file = new File(fullInputFilePath);
		while ( file.exists() ) {
			fullInputFilePath = baseFileName + (fileNum++) + extension;
			file = new File(fullInputFilePath);
		}
		
		try {
			printStream = new PrintStream(new FileOutputStream(file));
			
			while ( inputScanner.hasNextLine() ) {
				printStream.print(inputScanner.nextLine());
				printStream.print(endOfLine);
				lineCount ++;
			}
			
			printStream.flush();
			printStream.close();
			
			inputScanner.close();
			
			// Assign the new input-file from which the data will be read for the rest of the program's execution.
			inputScanner = new Scanner(new FileInputStream(fullInputFilePath));
		} catch (Exception e) {
			logger.error("", e);
			FileUtils.closeIO();
			System.exit(-10);	// The inputFile (stream) is already partly-consumed, no point to continue.
		}
		
		if ( FileUtils.skipFirstRow && (lineCount != 0) )
			return (lineCount -1);
		else
			return lineCount;
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
	 * @return HashMultimap<String, String>
	 */
	public static HashMultimap<String, String> getNextIdUrlPairGroupFromJson()
	{
		IdUrlTuple inputIdUrlTuple;
		HashMultimap<String, String> idAndUrlMappedInput = HashMultimap.create();
		
		int curBeginning = FileUtils.fileIndex;
		
		while ( inputScanner.hasNextLine() && (FileUtils.fileIndex < (curBeginning + jsonBatchSize)) )
		{// While (!EOF) and inside the current url-group, iterate through lines.

			//logger.debug("fileIndex: " + FileUtils.fileIndex);	// DEBUG!

			// Take each line, remove potential double quotes.
			String retrievedLineStr = inputScanner.nextLine();
			//logger.debug("Loaded from inputFile: " + retrievedLineStr);	// DEBUG!

			FileUtils.fileIndex ++;

			if ( retrievedLineStr.isEmpty() ) {
				FileUtils.unretrievableInputLines ++;
				continue;
			}

			if ( (inputIdUrlTuple = jsonDecoder(retrievedLineStr)) == null ) {	// Decode the jsonLine and take the two attributes.
				logger.warn("A problematic inputLine found: \"" + retrievedLineStr + "\"");
				FileUtils.unretrievableInputLines++;
				continue;
			}

			if ( !idAndUrlMappedInput.put(inputIdUrlTuple.id, inputIdUrlTuple.url) )    // We have a duplicate in the input.. log it here as we cannot pass it through the HashMultimap. It's possible that this as well as the original might be/give a docUrl.
				UrlUtils.logQuadruple(inputIdUrlTuple.id, inputIdUrlTuple.url, null, "duplicate", "Discarded in FileUtils.getNextIdUrlPairGroupFromJson(), as it is a duplicate.", null, false);
		}

		return idAndUrlMappedInput;
	}
	
	
	/**
	 * This method decodes a Jason String into its members.
	 * @param jsonLine String
	 * @return HashMap<String,String>
	 */
	public static IdUrlTuple jsonDecoder(String jsonLine)
	{
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
				return null;
			else	// If url is empty but not the id, then we will still see the ID in the output and possible find its missing URL later.
				FileUtils.unretrievableUrlsOnly ++;    // Keep track of lines with an id, but, with no url.
		}

		return new IdUrlTuple(idStr, urlStr);
	}
	
	
	/**
	 * This function writes new "quadruplesToBeLogged"(id-sourceUrl-docUrl-comment) in the output file.
	 * Each time it's finished writing, it flushes the write-stream and clears the "quadrupleToBeLoggedList".
	 */
	public static void writeToFile()
	{
		for ( QuadrupleToBeLogged quadruple : FileUtils.quadrupleToBeLoggedList)
		{
			strB.append(quadruple.toJsonString()).append(endOfLine);
		}
		
		printStream.print(strB.toString());
		printStream.flush();
		
		strB.setLength(0);	// Reset the buffer (the same space is still used, no reallocation is made).
		
		logger.debug("Finished writing " + FileUtils.quadrupleToBeLoggedList.size() + " quadruples to the outputFile.");
		
		FileUtils.quadrupleToBeLoggedList.clear();	// Clear the list to put the new <jsonBatchSize> values. The backing array used by List is not de-allocated. Only the String-references contained get GC-ed.
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
				docFile = new File(storeDocFilesDir + (numOfDocFile++) + ".pdf");	// TODO - Later, on different fileTypes, take care of the extension properly.
			
			try {
				outStream = new FileOutputStream(docFile);
			} catch (FileNotFoundException fnfe) {
				logger.warn("", fnfe);
				throw new DocFileNotRetrievedException();
			}
			
			int bytesRead = -1;
			byte[] buffer = new byte[3145728];	// 3Mb (average docFiles-size)
			long startTime = System.nanoTime();
			while ( (bytesRead = inStream.read(buffer)) != -1 )
			{
				long elapsedTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
				if ( (elapsedTime > FileUtils.maxStoringWaitingTime) || (elapsedTime == Long.MIN_VALUE) ) {
					logger.warn("Storing docFile from docUrl: \"" + docUrl + "\" is taking over "+ TimeUnit.MILLISECONDS.toSeconds(FileUtils.maxStoringWaitingTime) + "secs! Aborting..");
					if ( !docFile.delete() )
						logger.error("Error when deleting the half-retrieved file from docUrl: " + docUrl);
					numOfDocFile --;	// Revert number, as this docFile was not retrieved. In case of delete-failure, this file will just be overwritten, except if it's the last one.
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
		} catch (IOException ioe) {
			throw new DocFileNotRetrievedException(ioe.getMessage());
		} catch (Exception e) {
			logger.warn("", e);
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
		
		String dotFileExtension /*= "";
		if ( shouldAcceptOnlyPDFs )
			dotFileExtension*/ = ".pdf";
		/*else {	// TODO - Later we might accept more fileTypes.
			if ( contentType != null )
				// Use the Content-Type to determine the extension. A multi-mapping between mimeTypes and fileExtensions is needed.
			else
				// Use the last subString of the url-string as last resort, although not reliable! (subString after the last "." will not work if the docFileName-url-part doesn't include is not an extension but part of the original name or a "random"-string).
		}*/
		
		if ( contentDisposition != null ) {	// Extract docFileName from contentDisposition.
			Matcher fileNameMatcher = FILENAME_FROM_CONTENT_DISPOSITION_FILTER.matcher(contentDisposition);
			if ( fileNameMatcher.matches() ) {
				try {
					docFileName = fileNameMatcher.group(1);	// Group<1> is the fileName.
				} catch (Exception e) { logger.error("", e); }
				if ( (docFileName == null) || docFileName.isEmpty() )
					docFileName = null;	// Ensure null-value for future checks.
			} else
				logger.warn("Unmatched file-content-Disposition: " + contentDisposition);
		}
		
		//docFileName = null;	// Just to test the docFileNames retrieved from the DocId-part of the docUrls.
		
		// If we couldn't get the fileName from the "Content-Disposition", try getting it from the url.
		if ( docFileName == null )
			docFileName = UrlUtils.getDocIdStr(docUrl, null);	// Extract the docID as the fileName from docUrl.
		
		if ( (docFileName != null) && !docFileName.isEmpty() ) {

			if ( !docFileName.endsWith(dotFileExtension) )
				docFileName += dotFileExtension;
			
			String fullDocName = storeDocFilesDir + docFileName;

			// Check if the FileName is too long and we are going to get an error at file-creation.
			int docFullNameLength = fullDocName.length();
			if ( docFullNameLength > MAX_FILENAME_LENGTH ) {
				logger.warn("Too long docFullName found (" + docFullNameLength + " chars), it would cause file-creation to fail, so we mark the file-name as \"unretrievable\".\nThe long docName is: \"" + fullDocName + "\".");
				hasUnretrievableDocName = true;	// TODO - Maybe I should take the part of the full name that can be accepted instead of marking it unretrievable..
			}
		} else
			hasUnretrievableDocName = true;
		
		
		if ( hasUnretrievableDocName ) {
			if ( unretrievableDocNamesNum == 0 )
				docFileName = "unretrievableDocName" + dotFileExtension;
			else
				docFileName = "unretrievableDocName(" + unretrievableDocNamesNum + ")" + dotFileExtension;
			
			unretrievableDocNamesNum ++;
		}
		
		//logger.debug("docFileName: " + docFileName);
		
		try {
			String saveDocFileFullPath = storeDocFilesDir + docFileName;
			File docFile = new File(saveDocFileFullPath);
			
			if ( !hasUnretrievableDocName ) {	// If we retrieved the fileName, go check if it's a duplicate.
				
				boolean isDuplicate = false;
				Integer curDuplicateNum = 1;

				if ( (curDuplicateNum = numbersOfDuplicateDocFileNames.get(docFileName)) != null ) {
					curDuplicateNum += 1;
					isDuplicate = true;
				} else if ( docFile.exists() )	// If it's not an already-known duplicate (this is the first duplicate-case for this file), go check if it exists in the fileSystem.
					isDuplicate = true;
				
				if ( isDuplicate ) {
					// Construct final-DocFileName by renaming.
					String preExtensionFileName = docFileName.substring(0, docFileName.lastIndexOf("."));
					String newDocFileName = preExtensionFileName + "(" + curDuplicateNum + ")" + dotFileExtension;
					saveDocFileFullPath = storeDocFilesDir + File.separator + newDocFileName;
					File renamedDocFile = new File(saveDocFileFullPath);
					if ( docFile.renameTo(renamedDocFile) )	// If renaming was successful, store the "curDuplicateNum" for this base-"docFileName".
						numbersOfDuplicateDocFileNames.put(docFileName, curDuplicateNum);
					else {
						logger.error("Renaming operation of \"" + docFileName + "\" to \"" + newDocFileName + "\" has failed!");
						throw new DocFileNotRetrievedException();
					}
				}
			}
			
			FileUtils.numOfDocFile ++;
			return docFile;
			
		} catch (DocFileNotRetrievedException dfnre) {
			throw dfnre;
		} catch (Exception e) {	// Mostly I/O and Security Exceptions.
			logger.warn("", e);
			throw new DocFileNotRetrievedException();
		}
	}
	
	
	/**
	 * Closes open Streams and deletes the temporary-inputFile which is used when the MLA is enabled.
	 */
	public static void closeIO()
	{
		if ( inputScanner != null )
        	inputScanner.close();
		
		if ( printStream != null ) {
			printStream.flush();
			printStream.close();
		}

		// If the MLA was enabled then an extra file was used to store the streamed content and count the number of urls, in order to optimize the M.L.A.'s execution.
		if ( fullInputFilePath != null ) {
			try {
				org.apache.commons.io.FileUtils.forceDelete(new File(fullInputFilePath));
			} catch (Exception e) {
				logger.error("", e);
				closeLogger();
				System.exit(-11);
			}
		}
		closeLogger();
	}
	
	
	private static void closeLogger()
	{
		LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
		loggerContext.stop();
	}
	
	
	/**
	 * This method parses a testFile with one-url-per-line and extracts the urls (e.g. ".txt", ".csv", ".tsv").
	 * @return Collection<String>
	 */
	public static Collection<String> getNextUrlGroupTest()
	{
		Collection<String> urlGroup = new HashSet<String>();
		
		// Take a group of <jsonBatchSize> urls from the file..
		// If we are at the end and there are less than <jsonBatchSize>.. take as many as there are..
		
		//logger.debug("Retrieving the next group of " + jsonBatchSize + " elements from the inputFile.");
		int curBeginning = FileUtils.fileIndex;
		
		while ( inputScanner.hasNextLine() && (FileUtils.fileIndex < (curBeginning + jsonBatchSize)) )
		{// While (!EOF) and inside the current url-group, iterate through lines.
			
			// Take each line, remove potential double quotes.
			String retrievedLineStr = inputScanner.nextLine();
			
			FileUtils.fileIndex ++;
			
			if ( (FileUtils.fileIndex == 1) && skipFirstRow )
				continue;
			
			if ( retrievedLineStr.isEmpty() ) {
				FileUtils.unretrievableInputLines ++;
				continue;
			}
			
			retrievedLineStr = StringUtils.remove(retrievedLineStr, "\"");
			
			//logger.debug("Loaded from inputFile: " + retrievedLineStr);	// DEBUG!

			if ( !urlGroup.add(retrievedLineStr) )    // We have a duplicate in the input.. log it here as we cannot pass it through the HashSet. It's possible that this as well as the original might be/give a docUrl.
				UrlUtils.logQuadruple(null, retrievedLineStr, null, "duplicate", "Discarded in FileUtils.getNextUrlGroupTest(), as it is a duplicate.", null, false);
		}
		//logger.debug("FileUtils.fileIndex's value after taking urls after " + FileUtils.fileIndex / jsonBatchSize + " time(s), from input file: " + FileUtils.fileIndex);	// DEBUG!
		
		return urlGroup;
	}
	
}
