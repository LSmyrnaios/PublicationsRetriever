package eu.openaire.publications_retriever.util.file;

import ch.qos.logback.classic.LoggerContext;
import com.google.common.collect.HashMultimap;
import eu.openaire.publications_retriever.PublicationsRetriever;
import eu.openaire.publications_retriever.crawler.MachineLearning;
import eu.openaire.publications_retriever.exceptions.FileNotRetrievedException;
import eu.openaire.publications_retriever.util.args.ArgsUtils;
import eu.openaire.publications_retriever.util.http.ConnSupportUtils;
import eu.openaire.publications_retriever.util.url.DataForOutput;
import eu.openaire.publications_retriever.util.url.LoaderAndChecker;
import eu.openaire.publications_retriever.util.url.UrlUtils;
import org.apache.commons.io.FileDeleteStrategy;
import org.apache.commons.lang3.Strings;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
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

	public static long numOfLines = 0;	// Only the main thread accesses it.

	public static int jsonBatchSize = 3000;	// Do not set it as "final", since other apps using this program might want to set their own limit.

	private static StringBuilder stringToBeWritten = null;	// It will be assigned and pre-allocated only if the "WriteToFile()" method is called.
	// Other programs using this program as a library might not want to write the results in a file, but send them over the network.

	private static int fileIndex = 0;	// Index in the input file
	public static boolean skipFirstRow = false;	// Use this to skip the HeaderLine in a csv-kindOf-File.
	public static final String endOfLine = System.lineSeparator();
	public static int unretrievableInputLines = 0;	// For better statistics in the end.


	public static final List<DataForOutput> dataForOutput = Collections.synchronizedList(new ArrayList<>(jsonBatchSize));

	public static final HashMap<String, Integer> numbersOfDuplicateDocFileNames = new HashMap<>();	// Holds docFileNames with their duplicatesNum.
	// If we use the above without external synchronization, then the "ConcurrentHashMap" should be used instead.

	public static final boolean shouldOutputFullPathName = true;	// Should we log, in the jasonOutputFile, the fullPathName or just the ending fileName?
	public static int numOfDocFile = 0;	// In the case that we don't care for original docFileNames, the fileNames are produced using an incremental system.
	public static final String workingDir = System.getProperty("user.dir") + File.separator;

	public static int unretrievableDocNamesNum = 0;	// Num of docFiles for which we were not able to retrieve their docName.
	public static final Pattern FILENAME_FROM_CONTENT_DISPOSITION_FILTER = Pattern.compile(".*filename[*]?=(?:.*[\"'])?([^\"^;]+)[\";]*.*");

	public static final int MAX_FILENAME_LENGTH = 250;	// TODO - Find a way to get the current-system's MAX-value.

	public static String fullInputFilePath = null;	// Used when the MLA is enabled, and we want to count the number of lines in the inputFile, in order to optimize the M.L.A.'s execution.

	public static int duplicateIdUrlEntries = 0;

	private static final String utf8Charset = StandardCharsets.UTF_8.toString();

	public static final Pattern EXTENSION_PATTERN = Pattern.compile("(\\.[^._-]+)$");


	public FileUtils(InputStream input, OutputStream output)
	{
		FileUtils.inputScanner = new Scanner(input, utf8Charset);

		if ( MachineLearning.useMLA ) {	// In case we are using the MLA, go get the numOfLines to be used.
			if ( numOfLines == 0 ) {	// If the inputFile was not given as an argument, but as the stdin, instead.
				numOfLines = getInputNumOfLines();
				logger.debug("Num of lines in the inputFile: " + numOfLines);
			}
		}

		setOutput(output);

		if ( ArgsUtils.shouldUploadFilesToS3 )
			new S3ObjectStore();
	}


	public static void setOutput(OutputStream output)
	{
		try {
			FileUtils.printStream = new PrintStream(output, false, utf8Charset);
		}
		catch ( Exception e ) {
			logger.error(e.getMessage(), e);
			System.exit(20);
		}

		if ( ArgsUtils.shouldJustDownloadHtmlFiles )
			handleStoreFilesDirectory(ArgsUtils.storeHtmlFilesDir, ArgsUtils.shouldDeleteOlderHTMLFiles , false);
		else if ( ArgsUtils.shouldDownloadDocFiles )
			handleStoreFilesDirectory(ArgsUtils.storeDocFilesDir, ArgsUtils.shouldDeleteOlderDocFiles , true);
	}


	public static void handleStoreFilesDirectory(String storeFilesDir, boolean shouldDeleteOlderFiles, boolean calledForDocFiles)
	{
		File dir = new File(storeFilesDir);
		if ( shouldDeleteOlderFiles ) {
			logger.info("Deleting old " + (calledForDocFiles ? "doc" : "html") + "Files..");
			try {
				deleteDirectory(dir);	// org.apache.commons.io.FileUtils
			} catch (IOException ioe) {
				logger.error("The following directory could not be deleted: " + storeFilesDir, ioe);
				if ( calledForDocFiles )
					ArgsUtils.shouldDownloadDocFiles = false;	// Continue without downloading the docFiles, just create the jsonOutput.
				//else
					//ArgsUtils.shouldDownloadHTMLFiles = false;
				return;
			} catch (IllegalArgumentException iae) {
				logger.error("This directory does not exist: " + storeFilesDir + "\n" + iae.getMessage());
				return;
			}
		}

		// If the directory doesn't exist, try to (re)create it.
		try {
			if ( !dir.exists() ) {
				if ( !dir.mkdirs() ) {	// Try to create the directory(-ies) if they don't exist. If they exist OR if sth went wrong, the result is the same: "false".
					String errorMessage;
					if ( ArgsUtils.docFilesStorageGivenByUser )
						errorMessage = "Problem when creating the \"storeDocFilesDir\": \"" + storeFilesDir + "\"."
								+ "\nPlease give a valid Directory-path.";
					else	// User has left the storageDir to be the default one.
						errorMessage = "Problem when creating the default \"storeDocFilesDir\": \"" + storeFilesDir + "\"."
								+ "\nPlease verify you have the necessary privileges in the directory you are running the program from or specify the directory you want to save the files to."
								+ "\nIf the above is not an option, then you can set to retrieve just the " + ArgsUtils.targetUrlType + "s and download the full-texts later (on your own).";
					System.err.println(errorMessage);
					logger.error(errorMessage);
					FileUtils.closeIO();
					System.exit(-3);
				}
			}
		} catch (SecurityException se) {
			logger.error(se.getMessage(), se);
			logger.warn("There was an error creating the docFiles-storageDir! Continuing without downloading the docFiles, while creating the jsonOutput with the docUrls.");
			if ( calledForDocFiles )
				ArgsUtils.shouldDownloadDocFiles = false;	// Continue without downloading the docFiles, just create the jsonOutput.
			else
				ArgsUtils.shouldJustDownloadHtmlFiles = false;
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

		// Make sure we create a new distinct file which will not replace any other existing file. This new file will get deleted in the end.
		int fileNum = 1;
		fullInputFilePath = baseFileName + extension;
		File file = new File(fullInputFilePath);
		while ( file.exists() ) {
			fullInputFilePath = baseFileName + (fileNum++) + extension;
			file = new File(fullInputFilePath);
		}

		try {
			printStream = new PrintStream(Files.newOutputStream(file.toPath()), false, utf8Charset);

			while ( inputScanner.hasNextLine() ) {
				printStream.print(inputScanner.nextLine());
				printStream.print(endOfLine);
				lineCount ++;
			}

			printStream.close();
			inputScanner.close();

			// Assign the new input-file from which the data will be read for the rest of the program's execution.
			inputScanner = new Scanner(Files.newInputStream(Paths.get(fullInputFilePath)));
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
			return FileUtils.fileIndex - FileUtils.unretrievableInputLines -1; // -1 to exclude the first line
		else
			return FileUtils.fileIndex - FileUtils.unretrievableInputLines;
	}


	private static final int expectedPathsPerID = 5;
	private static final int expectedIDsPerBatch = (jsonBatchSize / expectedPathsPerID);

	private static HashMultimap<String, String> idAndUrlMappedInput = null;	// This is used only be the main Thread, so no synchronization is needed.
	// This will be initialized and pre-allocated the 1st time it is needed.
	// When PublicationsRetriever is used as a library by a service, this functionality may not be needed.

	/**
	 * This method parses a Json file and extracts the urls, along with the IDs.
	 * @return HashMultimap<String, String>
	 */
	public static HashMultimap<String, String> getNextIdUrlPairBatchFromJson()
	{
		IdUrlTuple inputIdUrlTuple;

		if ( idAndUrlMappedInput == null )	// Create this HashMultimap the first time it is needed.
			idAndUrlMappedInput = HashMultimap.create(expectedIDsPerBatch, expectedPathsPerID);
		else
			idAndUrlMappedInput.clear();	// Clear it from its elements (without deallocating the memory), before gathering the next batch.

		int curBeginning = FileUtils.fileIndex;

		while ( inputScanner.hasNextLine() && (FileUtils.fileIndex < (curBeginning + jsonBatchSize)) )
		{// While (!EOF) and inside the current url-batch, iterate through lines.

			//logger.debug("fileIndex: " + FileUtils.fileIndex);	// DEBUG!

			// Take each line, remove potential double quotes.
			String retrievedLineStr = inputScanner.nextLine();
			//logger.debug("Loaded from inputFile: " + retrievedLineStr);	// DEBUG!

			FileUtils.fileIndex ++;

			if ( retrievedLineStr.isEmpty() ) {
				FileUtils.unretrievableInputLines ++;
				continue;
			}

			if ( (inputIdUrlTuple = getDecodedJson(retrievedLineStr)) == null ) {	// Decode the jsonLine and take the two attributes.
				logger.warn("A problematic inputLine found: \t" + retrievedLineStr);
				FileUtils.unretrievableInputLines ++;
				continue;
			}

			if ( !idAndUrlMappedInput.put(inputIdUrlTuple.id, inputIdUrlTuple.url) ) {    // We have a duplicate id-url pair in the input, log it here as we cannot pass it through the HashMultimap. We will handle the first found pair only.
				duplicateIdUrlEntries ++;
				UrlUtils.addOutputData(inputIdUrlTuple.id, inputIdUrlTuple.url, null, UrlUtils.duplicateUrlIndicator, "Discarded in FileUtils.getNextIdUrlPairBatchFromJson(), as it is a duplicate.", "null", null, false, "false", "null", "null", "null", "true", null, "null", "null");
			}
		}

		return idAndUrlMappedInput;
	}


	/**
	 * This method decodes a Jason String into its members.
	 * @param jsonLine String
	 * @return HashMap<String,String>
	 */
	public static IdUrlTuple getDecodedJson(String jsonLine)
	{
		// Get ID and url and put them in the HashMap
		String idStr;
		String urlStr;
		try {
			JSONObject jObj = new JSONObject(jsonLine); // Construct a JSONObject from the retrieved jsonLine.
			idStr = jObj.get("id").toString();
			urlStr = jObj.get("url").toString();
		} catch (JSONException je) {
			logger.warn("JSONException caught when tried to parse and extract values from jsonLine: \t" + jsonLine, je);
			return null;
		}

		if ( urlStr.isEmpty() ) {
			if ( !idStr.isEmpty() )	// If we only have the id, then go and log it.
				UrlUtils.addOutputData(idStr, urlStr, null, UrlUtils.unreachableDocOrDatasetUrlIndicator, "Discarded in FileUtils.jsonDecoder(), as the url was not found.", "null", null, true, "true", "false", "false", "false", "false", null, "null", "null");
			return null;
		}

		return new IdUrlTuple(idStr, urlStr);
	}


	/**
	 * This function writes new "quadruplesToBeLogged"(id-sourceUrl-docUrl-comment) in the output file.
	 * Each time it's finished writing, it flushes the write-stream and clears the "quadrupleToBeLoggedList".
	 */
	public static void writeResultsToFile()
	{
		if ( stringToBeWritten == null )
			stringToBeWritten = new StringBuilder(jsonBatchSize * 900);  // 900: the usual-maximum-expected-length for an <id-sourceUrl-docUrl-comment> quadruple.

		for ( DataForOutput data : FileUtils.dataForOutput )
		{
			stringToBeWritten.append(data.toJsonString()).append(endOfLine);
		}

		printStream.print(stringToBeWritten);
		printStream.flush();

		stringToBeWritten.setLength(0);	// Reset the buffer (the same space is still used, no reallocation is made).
		logger.debug("Finished writing " + FileUtils.dataForOutput.size() + " quadruples to the outputFile.");

		FileUtils.dataForOutput.clear();	// Clear the list to put the new <jsonBatchSize> values. The backing array used by List is not de-allocated. Only the String-references contained get GC-ed.
	}


	public static final AtomicInteger numOfDocFiles = new AtomicInteger(0);

	public static FileData storeDocFileWithIdOrOriginalFileName(HttpResponse<InputStream> response, String docUrl, String id, int contentSize) throws FileNotRetrievedException
			//, NoSpaceLeftException
	{
		FileData fileData;
		if ( ArgsUtils.fileNameType.equals(ArgsUtils.fileNameTypeEnum.idName) )
			fileData = getFileAndHandleExisting(id, ".pdf", false, contentSize, ArgsUtils.storeDocFilesDir, numbersOfDuplicateDocFileNames);    // TODO - Later, on different fileTypes, take care of the extension properly.
		else if ( ArgsUtils.fileNameType.equals(ArgsUtils.fileNameTypeEnum.originalName) )
			fileData = getDocFileWithOriginalFileName(docUrl, response.headers().firstValue("Content-Disposition").orElse(null), contentSize);
		else
			throw new FileNotRetrievedException("The 'fileNameType' was invalid: " + ArgsUtils.fileNameType);

		InputStream inputStream = ConnSupportUtils.checkEncodingAndGetInputStream(response, false);
		if ( inputStream == null )
			throw new FileNotRetrievedException("Could not acquire the inputStream!");

		MessageDigest md;
		try {
			md = MessageDigest.getInstance("MD5");
		} catch (Exception e) {
			logger.error("Failed to get instance for MD5 hash-algorithm!", e);
            try{inputStream.close();} catch(Exception ignore){}
            throw new RuntimeException("MD5 HASH ALGO MISSING");
		}

		numOfDocFiles.incrementAndGet();

		File docFile = fileData.getFile();
		FileOutputStream fileOutputStream = fileData.getFileOutputStream();
		int bufferSize = (((contentSize != -2) && contentSize < fiveMb) ? contentSize : fiveMb);

		try ( BufferedInputStream inStream = new BufferedInputStream(inputStream, bufferSize);
			  BufferedOutputStream outStream = new BufferedOutputStream(((fileOutputStream != null) ? fileOutputStream : new FileOutputStream(docFile)), bufferSize) )
		{
			long bytesCount = downloadFile(fileData.getLocation(), contentSize, docUrl, md, inStream, outStream);
			String md5Hash = printHexBinary(md.digest());
			if ( ArgsUtils.shouldUploadFilesToS3 ) {
				fileData = S3ObjectStore.uploadToS3(docFile.getName(), docFile.getAbsolutePath());
				if ( fileData != null ) {    // Otherwise, the returned object will be null.
					fileData.setFile(docFile);
					fileData.setHash(md5Hash);
					fileData.setSize(bytesCount);
					// In the S3 case, we use IDs or increment-numbers as the names, so no duplicate-overwrite should be a problem here.
					// as we delete the local files and the online tool just overwrites the file, without responding if it was overwritten.
					// So if the other naming methods were used with S3, we would have to check if the file existed online and then rename of needed and upload back.
				} else
					numOfDocFiles.decrementAndGet();	// Revert number, as this docFile was not retrieved. In case of delete-failure, this file will just be overwritten, except if it's the last one.
			} else
				fileData = new FileData(docFile, md5Hash, bytesCount, ((FileUtils.shouldOutputFullPathName) ? docFile.getAbsolutePath() : docFile.getName()));

			// TODO - HOW TO SPOT DUPLICATE-NAMES IN THE S3 MODE?
			// The local files are deleted after uploaded.. (as they should be)
			// So we will be uploading files with the same filename-KEY.. in that case, they get overwritten.
			// An option would be to check if an object already exists, then increment a number and upload the object.
			// From that moment forward, the number will be stored in memory along with the fileKeyName, just like with "originalNames", so next time no online check should be needed..!
			// Of-course the above fast-check algorithm would work only if the bucket was created or filled for the first time, from this program, in a single machine.
			// Otherwise, a file-key-name (with incremented number-string) might already exist, from a previous or parallel upload from another run and so it will be overwritten!

			return fileData;	// It may be null.
		} catch (Exception e) {
			numOfDocFiles.decrementAndGet();	// Revert number, as this docFile was not retrieved.
			// When creating the above FileOutputStream, the file may be created as an "empty file", like a zero-byte file, when there is a "No space left on device" error.
			// So the empty file may exist, but we will also get the "FileNotFoundException".
            return checkAndThrowDocFileException(docUrl, contentSize, docFile, e);
        } finally {
            try {   // In case the initialization of the "BufferedInputStream" failed, we need to manually close the "inputStream", as it will NOT be auto-closed..
                inputStream.close();
            } catch (IOException ignored) {}
        }
	}


    public static long downloadFile(String fileFullPath, int contentSize, String docUrl, MessageDigest md, BufferedInputStream inStream, BufferedOutputStream outStream)
			throws IOException, FileNotRetrievedException
	{
		int maxStoringWaitingTime = getMaxStoringWaitingTime(contentSize);	// It handles the "-2" case.
		int bytesRead = -1;
		final byte[] buffer = new byte[65536];	// This is used to reduce the number of iterations of the "while"-loop and every call inside. It does not affect how often the actual-data is read/write to/from streams.
		long bytesCount = 0;
		long startTime = System.nanoTime();
		while ( (bytesRead = inStream.read(buffer)) != -1 )
		{
			long elapsedTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
			if ( (elapsedTime > maxStoringWaitingTime) || (elapsedTime == Long.MIN_VALUE) ) {
				String errMsg = "Storing docFile from docUrl: \"" + docUrl + "\" is taking over " + TimeUnit.MILLISECONDS.toSeconds(maxStoringWaitingTime) + " seconds (for contentSize: " + (PublicationsRetriever.df.format((double) contentSize / FileUtils.mb)) + " MB)! Aborting..";
				logger.warn(errMsg);
				throw new FileNotRetrievedException(errMsg);
			} else {
				outStream.write(buffer, 0, bytesRead);
				md.update(buffer, 0, bytesRead);
				bytesCount += bytesRead;
			}
		}
		//logger.debug("Elapsed time for storing: " + TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime));
		if ( bytesCount == 0 ) {	// This will be the case if the "inStream.read()" returns "-1" in the first call.
			// It's not practical to decrease the potential "duplicate-filename-counter" in this point.
			// So we will just skip it: for example we will have: filename.pdf, filename(1).pdf [SKIPPED], filename(2).pdf
			String errMsg = "InputStream was empty when trying to download file: " + fileFullPath;
			logger.warn(errMsg);
			throw new FileNotRetrievedException(errMsg);
		}
		return bytesCount;
	}


	/**
	 * This method is responsible for storing the docFiles and store them in permanent storage.
	 * It is synchronized, in order to avoid files' numbering inconsistency.
	 *
	 * @param response
	 * @param docUrl
	 * @param contentSize
	 * @throws FileNotRetrievedException
	 */
	public static synchronized FileData storeDocFileWithNumberName(HttpResponse<InputStream> response, String docUrl, int contentSize) throws FileNotRetrievedException//, NoSpaceLeftException
    {
		InputStream inputStream = ConnSupportUtils.checkEncodingAndGetInputStream(response, false);
		if ( inputStream == null )
			throw new FileNotRetrievedException("Could not acquire the inputStream!");

		MessageDigest md;
		try {
			md = MessageDigest.getInstance("MD5");
		} catch (Exception e) {
			logger.error("Failed to get instance for MD5 hash-algorithm!", e);
            try{inputStream.close();} catch(Exception ignore){}
			throw new RuntimeException("MD5 HASH ALGO MISSING");
		}

		String docFileFullPath = ArgsUtils.storeDocFilesDir + (numOfDocFile++) + ".pdf";	// First use the "numOfDocFile" and then increment it.
		// TODO - Later, on different fileTypes, take care of the extension properly.
		File docFile = new File(docFileFullPath);
		int bufferSize = (((contentSize != -2) && contentSize < fiveMb) ? contentSize : fiveMb);

		try ( BufferedInputStream inStream = new BufferedInputStream(inputStream, bufferSize);
			BufferedOutputStream outStream = new BufferedOutputStream(new FileOutputStream(docFile), bufferSize))
		{
			long bytesCount = downloadFile(docFileFullPath, contentSize, docUrl, md, inStream, outStream);
			String md5Hash = printHexBinary(md.digest());
			FileData fileData;
			if ( ArgsUtils.shouldUploadFilesToS3 ) {
				fileData = S3ObjectStore.uploadToS3(docFile.getName(), docFile.getAbsolutePath());
				if ( fileData != null ) {    // Otherwise, the returned object will be null.
					fileData.setFile(docFile);
					fileData.setHash(md5Hash);
					fileData.setSize(bytesCount);
					// In the S3 case, we use IDs or increment-numbers as the names, so no duplicate-overwrite should be a problem here.
					// as we delete the local files and the online tool just overwrites the file, without responding if it was overwritten.
					// So if the other naming methods were used with S3, we would have to check if the file existed online and then rename of needed and upload back.
				} else
					numOfDocFile --;
			} else
				fileData = new FileData(docFile, md5Hash, bytesCount, ((FileUtils.shouldOutputFullPathName) ? docFile.getAbsolutePath() : docFile.getName()));

			// TODO - HOW TO SPOT DUPLICATE-NAMES IN THE S3 MODE?
			// The local files are deleted after uploaded.. (as they should be)
			// So we will be uploading files with the same filename-KEY.. in that case, they get overwritten.
			// An option would be to check if an object already exists, then increment a number and upload the object.
			// From that moment forward, the number will be stored in memory along with the fileKeyName, just like with "originalNames", so next time no online check should be needed..!
			// Of-course the above fast-check algorithm would work only if the bucket was created or filled for the first time, from this program, in a single machine.
			// Otherwise, a file-key-name (with incremented number-string) might already exist, from a previous or parallel upload from another run and so it will be overwritten!

			return fileData;	// It may be null.
		} catch (Exception e) {
			numOfDocFile --;	// Revert number, as this docFile was not retrieved. In case of delete-failure, this file will just be overwritten, except if it's the last one.
			// When creating the above FileOutputStream, the file may be created as an "empty file", like a zero-byte file, when there is a "No space left on device" error.
			// So the empty file may exist, but we will also get the "FileNotFoundException".
            return checkAndThrowDocFileException(docUrl, contentSize, docFile, e);
        } finally {
            try {   // In case the initialization of the "BufferedInputStream" failed, we need to manually close the "inputStream", as it will NOT be auto-closed..
                inputStream.close();
            } catch (IOException ignored) {}
        }
	}


    private static FileData checkAndThrowDocFileException(String docUrl, int contentSize, File docFile, Exception e)
            throws FileNotRetrievedException//, NoSpaceLeftException
    {
        try {
            if ( docFile.exists() ) {
                try {
                    FileDeleteStrategy.FORCE.delete(docFile);
                } catch (Exception e1) {
                    logger.error("Error when deleting the half-created file from docUrl: " + docUrl, e1);
                }
            }
        } catch (Exception e2) {
            logger.error("Error when checking if there is a half-created file from docUrl: " + docUrl, e2);
        }

        if ( e instanceof FileNotRetrievedException )
            throw (FileNotRetrievedException) e;
        else {
            /*if ( e instanceof FileNotFoundException) {    // This may be thrown in case the file cannot be created.
                String msg = e.getMessage();    // This kind of exception is thrown, among other reasons, when there is no space on the device.
                if ( (msg != null) && msg.contains("(No space left on device)") ) {
                    String fileName = docFile.getName();
                    throw new NoSpaceLeftException("No space left, when downloading file: " + fileName + " with advertised size: " + contentSize);
                }
            } else*/ if ( ! (e instanceof IOException) )
                logger.error("", e);

            throw new FileNotRetrievedException(e.getMessage());
        }
    }


	/**
	 * This method Returns the Document-"File" object which has the original file name as the final fileName.
	 * It is effectively synchronized, since it's always called from a synchronized method.
	 *
	 * @param docUrl
	 * @param contentDisposition
	 * @param contentSize
	 * @return
	 * @throws FileNotRetrievedException
	 */
	public static FileData getDocFileWithOriginalFileName(String docUrl, String contentDisposition, int contentSize) throws FileNotRetrievedException
			//, NoSpaceLeftException
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

		if ( (docFileName != null) && !docFileName.isEmpty() )	// Re-check as the value might have changed.
		{
			// The "DocIdStr" is guaranteed to not have forwarded-slashes.
			// But theoretically, it could have back-slashes and if the app runs on Windows, it could cause a "missing internal directories error".
			if ( File.separator.equals("\\") )
				docFileName = Strings.CS.replace(docFileName, File.separator, "_");

			if ( !docFileName.endsWith(dotFileExtension) )
				docFileName += dotFileExtension;
			
			String fullDocName = ArgsUtils.storeDocFilesDir + docFileName;

			// Check if the FileName is too long, and we are going to get an error at file-creation.
			int docFullNameLength = fullDocName.length();
			if ( docFullNameLength > MAX_FILENAME_LENGTH ) {
				logger.warn("Too long docFullName found (" + docFullNameLength + " chars), it would cause file-creation to fail, so we mark the file-name as \"unretrievable\".\nThe long docName is: \"" + fullDocName + "\".");
				hasUnretrievableDocName = true;	// TODO - Maybe I should take the part of the full name that can be accepted instead of marking it unretrievable..
			}
		} else
			hasUnretrievableDocName = true;

		if ( hasUnretrievableDocName ) {
			// If this ever is called from a code block without synchronization, then it should be a synchronized block.
			if ( unretrievableDocNamesNum == 0 )
				docFileName = "unretrievableDocName" + dotFileExtension;
			else
				docFileName = "unretrievableDocName(" + unretrievableDocNamesNum + ")" + dotFileExtension;

			unretrievableDocNamesNum ++;
		}
		
		//logger.debug("docFileName: " + docFileName);

		return getFileAndHandleExisting(docFileName, dotFileExtension, hasUnretrievableDocName, contentSize, ArgsUtils.storeDocFilesDir, numbersOfDuplicateDocFileNames);
	}


	private static final Lock fileNameLock = new ReentrantLock(true);   // Here keep the "fairness" as we do not want to time out our inputStreams for some unlucky threads.

	public static FileData getFileAndHandleExisting(String fileName, String dotFileExtension, boolean hasUnretrievableDocName, int contentSize, String storeFilesDir,
													HashMap<String, Integer> numbersOfDuplicateFileNames)
			throws FileNotRetrievedException	//, NoSpaceLeftException
	{
		String saveFileFullPath = storeFilesDir + fileName;
		if ( ! fileName.endsWith(dotFileExtension) )
			saveFileFullPath += dotFileExtension;

		File file = new File(saveFileFullPath);
		FileOutputStream fileOutputStream = null;
		Integer curDuplicateNum = 0;
		String initialFileName = fileName;

		// Synchronize to avoid file-overriding and corruption when multiple threads try to store a file with the same name (like when there are multiple files related with the same ID), which results also in loosing docFiles.
		// The file is created inside the synchronized block, by initializing the "fileOutputStream", so the next thread to check if it is going to download a duplicate-named file is guaranteed to find the previous file in the file-system
		// (without putting the file-creation inside the locks, the previous thread might have not created the file in time and the next thread will just override it, instead of creating a new file)
		fileNameLock.lock();
		try {
			if ( !hasUnretrievableDocName )	// If we retrieved the fileName, go check if it's a duplicate.
			{
				if ( (curDuplicateNum = numbersOfDuplicateFileNames.get(fileName)) != null )	// Since this data-structure is accessed inside the SYNCHRONIZED BLOCK, it can simpy be a HashMap without any internal sync, in order to speed it up.
					curDuplicateNum += 1;
				else if ( file.exists() )	// If it's not an already-known duplicate, check the fileSystem, if this is the first duplicate-case for this file.
					curDuplicateNum = 1;	// It was "null", after the "ConcurrentHashMap.get()" check.
				else
					curDuplicateNum = 0;	// First-time.

				if ( curDuplicateNum > 0 ) {	// Construct final-DocFileName for this duplicate-name file.
					String preExtensionFileName = fileName;
					int lastIndexOfDot = fileName.lastIndexOf(".");
					if ( lastIndexOfDot != -1 )
						preExtensionFileName = fileName.substring(0, lastIndexOfDot);
					fileName = preExtensionFileName + "(" + curDuplicateNum + ")" + dotFileExtension;
					saveFileFullPath = storeFilesDir + fileName;
					file = new File(saveFileFullPath);
				}
				// else, we use the initial file.
			}

            if ( !dotFileExtension.equals(".html") )
			    fileOutputStream = new FileOutputStream(file);
			if ( curDuplicateNum > 0 )	// After the file is created successfully, from the above outputStream-initialization, add the new duplicate-num in our HashMap.
				numbersOfDuplicateFileNames.put(initialFileName, curDuplicateNum);    // We should add the new "curDuplicateNum" for the original fileName, only if the new file can be created.

		} catch (FileNotFoundException fnfe) {	// This may be thrown in case the file cannot be created.
			String msg = fnfe.getMessage();
			if ( msg != null && msg.contains("(No space left on device)") ) {
				// When creating the above FileOutputStream, the file may be created as an "empty file", like a zero-byte file, when there is a "No space left on device" error.
				// So the empty file may exist now, while also getting the "FileNotFoundException".
				try {
					if ( file.exists() )
						FileDeleteStrategy.FORCE.delete(file);
				} catch (Exception e) {
					logger.error("Error when deleting the half-created file: " + fileName);
				}
			}
			logger.error("", fnfe);
			// The "fileOutputStream" was not created in this case, so no closing is needed.
			throw new FileNotRetrievedException(fnfe.getMessage());
		} catch (Exception e) {	// Mostly I/O and Security Exceptions.
			if ( fileOutputStream != null ) {
				try {
					fileOutputStream.close();
				} catch (Exception ignored) {}
			}
			String errMsg = "Error when handling the fileName = \"" + fileName + "\" and dotFileExtension = \"" + dotFileExtension + "\"!";
			logger.error(errMsg, e);
			throw new FileNotRetrievedException(errMsg);
		} finally {
			fileNameLock.unlock();
		}

		return new FileData(file, saveFileFullPath, fileOutputStream);
	}


	public static final int mb = 1_048_576;
	public static final int fiveMb = (5 * mb);
	static final int fiftyMBInBytes = (50 * mb);
	static final int oneHundredMBInBytes = (100 * mb);
	static final int twoHundredMBInBytes = (200 * mb);
	static final int threeHundredMBInBytes = (300 * mb);


	private static int getMaxStoringWaitingTime(int contentSize)
	{
		if ( contentSize == -2 )	// In case the server did not provide the "Content Length" header.
			return 45_000;	// 45 seconds

		if ( contentSize <= fiftyMBInBytes )
			return 45_000;	// 45 seconds
		else if ( contentSize <= oneHundredMBInBytes )
			return 70_000;	// 1 min & 10 seconds.
		else if ( contentSize <= twoHundredMBInBytes )
			return 140_000;	// 2 mins & 20 seconds.
		else if ( contentSize <= threeHundredMBInBytes )
			return 210_000;	// 3.5 mins.
		else
			return 330_000;	// 5.5 mins.
	}

	
	/**
	 * Closes open Streams and deletes the temporary-inputFile which is used when the MLA is enabled.
	 */
	public static void closeIO()
	{
		if ( inputScanner != null )
        	inputScanner.close();
		
		if ( printStream != null )
			printStream.close();

		// If the MLA was enabled then an extra file was used to store the streamed content and count the number of urls, in order to optimize the M.L.A.'s execution.
		if ( fullInputFilePath != null ) {
			try {
				org.apache.commons.io.FileUtils.forceDelete(new File(fullInputFilePath));
			} catch (Exception e) {
				logger.error("", e);
				closeLogger();
				PublicationsRetriever.executor.shutdownNow();
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
	public static Collection<String> getNextUrlBatchTest()
	{
		Collection<String> urlGroup = new HashSet<>(jsonBatchSize);
		
		// Take a batch of <jsonBatchSize> urls from the file..
		// If we are at the end and there are less than <jsonBatchSize>.. take as many as there are..
		
		//logger.debug("Retrieving the next batch of " + jsonBatchSize + " elements from the inputFile.");
		int curBeginning = FileUtils.fileIndex;
		
		while ( inputScanner.hasNextLine() && (FileUtils.fileIndex < (curBeginning + jsonBatchSize)) )
		{// While (!EOF) and inside the current url-batch, iterate through lines.
			
			// Take each line, remove potential double quotes.
			String retrievedLineStr = inputScanner.nextLine();
			
			FileUtils.fileIndex ++;
			
			if ( (FileUtils.fileIndex == 1) && skipFirstRow )
				continue;
			
			if ( retrievedLineStr.isEmpty() ) {
				FileUtils.unretrievableInputLines ++;
				continue;
			}
			
			retrievedLineStr = Strings.CS.remove(retrievedLineStr, "\"");
			
			//logger.debug("Loaded from inputFile: " + retrievedLineStr);	// DEBUG!

			if ( !urlGroup.add(retrievedLineStr) )    // We have a duplicate in the input.. log it here as we cannot pass it through the HashSet. It's possible that this as well as the original might be/give a docUrl.
				UrlUtils.addOutputData(null, retrievedLineStr, null, UrlUtils.duplicateUrlIndicator, "Discarded in FileUtils.getNextUrlGroupTest(), as it is a duplicate.", "null", null, false, "false", "null", "null", "null", "true", null, "null", "null");
		}

		return urlGroup;
	}


	/**
	 * Convert hash bytes to hexadecimal string.
	 * */
	public static String printHexBinary(byte[] hashBytes)
	{
		StringBuilder hexString = new StringBuilder(hashBytes.length);
		for ( byte b : hashBytes ) {
			hexString.append(String.format("%02x", b)); // Convert byte to hex-integer (lowercase) and append
		}
		return hexString.toString();
	}

}
