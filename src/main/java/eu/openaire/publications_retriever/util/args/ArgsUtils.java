package eu.openaire.publications_retriever.util.args;


import eu.openaire.publications_retriever.util.file.FileUtils;
import eu.openaire.publications_retriever.util.url.LoaderAndChecker;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;


/**
 * @author Lampros Smyrnaios
 */
public class ArgsUtils {

	private static final Logger logger = LoggerFactory.getLogger(ArgsUtils.class);

	public static int initialNumOfFile = 0;

	public static boolean shouldDownloadDocFiles = false;	// It will be set to "true" if the related command-line-argument is given.
	public static boolean shouldUploadFilesToS3 = false;	// Should we upload the files to S3 ObjectStore? Otherwise, they will be stored locally.
	public static boolean shouldDeleteOlderDocFiles = false;	// Should we delete any older stored docFiles? This is useful for testing.

	public static boolean docFilesStorageGivenByUser = false;
	public static boolean htmlFilesStorageGivenByUser = false;

	public static String storeDocFilesDir = FileUtils.workingDir + "docFiles" + File.separator;

	public static String storeHtmlFilesDir = FileUtils.workingDir + "htmlFiles" + File.separator;
	public static boolean shouldDeleteOlderHTMLFiles = false;	// Should we delete any older stored docFiles? This is useful for testing.

	public static boolean shouldJustDownloadHtmlFiles = false;	// It will be set to "true" if the related command-line-argument is given.

	public static boolean inputFromUrl = false;
	public static String inputDataUrl = null;

	public static InputStream inputStream = null;
	public static String inputFileFullPath = null;

	public static String targetUrlType = "docOrDatasetUrl";	// docUrl, documentUrl, docOrDatasetUrl ; this is set by the args-parser, and it's used only when outputting data, not inside the program.

	public static int workerThreadsCount = 0;
	public static int threadsMultiplier = 2;	// Use *3 without downloading docFiles and when having the domains to appear in uniform distribution in the inputFile. Use *2 when downloading.

	private static final String usageMessage = "\nUsage: java -jar publications_retriever-<VERSION>.jar -retrieveDataType <dataType: document | dataset | all> -inputFileFullPath inputFile [-downloadDocFiles(OPTIONAL) | -downloadJustHtmlFiles(OPTIONAL)] -fileNameType(OPTIONAL) <nameType: originalName | idName | numberName> -firstFileNum(OPTIONAL) 'num' -docFilesStorage(OPTIONAL) 'storageDir' -inputDataUrl(OPTIONAL) 'inputUrl' -numOfThreads(OPTIONAL) 'threadsNum' < 'input' > 'output'";

	private static boolean firstNumGiven = false;

	public enum fileNameTypeEnum {
		originalName, idName, numberName
	}
	public static fileNameTypeEnum fileNameType = null;

	public static boolean retrieveDocuments = true;
	public static boolean retrieveDatasets = true;


	public static void parseArgs(String[] mainArgs)
	{
		if ( mainArgs.length > 15 ) {
			String errMessage = "\"PublicationsRetriever\" expected only up to 15 arguments, while you gave: " + mainArgs.length + "!" + usageMessage;
			logger.error(errMessage);
			System.err.println(errMessage);
			System.exit(-1);
		}

		for ( short i = 0; i < mainArgs.length; i++ )
		{
			try {
				switch ( mainArgs[i] ) {
					case "-retrieveDataType":
						i ++;
						handleDatatypeArg(mainArgs[i]);
						break;
					case "-inputFileFullPath":
						i ++;
						handleFilePathArg(mainArgs[i]);
						break;
					case "-downloadDocFiles":
						shouldDownloadDocFiles = true;
						break;
					case "-docFilesStorage":
						i ++;
						handleDocFilesStorage(mainArgs[i]);
						break;
					case "-downloadJustHtmlFiles":
						shouldJustDownloadHtmlFiles = true;
						break;
					case "-htmlFilesStorage":
						i ++;
						handleHtmlFilesStorage(mainArgs[i]);
						break;
					case "-fileNameType":
						i ++;
						handleFileNameType(mainArgs[i]);
						break;
					case "-firstFileNum":
						i ++;
						handleFirstFileNum(mainArgs[i]);
						break;
					case "-inputDataUrl":
						i++;
						inputDataUrl = mainArgs[i];
						inputFromUrl = true;
						logger.info("Using the inputFile from the URL: " + inputDataUrl);
						break;
					case "-numOfThreads":
						i++;
						handleNumThreads(mainArgs[i]);
						break;
					default:	// log & ignore the argument
						String errMessage = "Argument: \"" + mainArgs[i] + "\" was not expected!" + usageMessage;
						System.err.println(errMessage);
						logger.error(errMessage);
						break;
				}
			} catch (ArrayIndexOutOfBoundsException aioobe) {
				String errMessage = "The argument-set of \"" + mainArgs[i] + "\" was not complete!\nThe provided arguments are: " + Arrays.toString(mainArgs) + usageMessage;
				System.err.println(errMessage);
				logger.error(errMessage);
				System.exit(90);
			}
		}

		if ( shouldJustDownloadHtmlFiles && shouldDownloadDocFiles ) {
			String errMessage = "The \"downloadJustHtmlFiles\" was given alongside \"downloadDocFiles\"! Using both of them is not possible." + usageMessage;
			System.err.println(errMessage);
			logger.error(errMessage);
			System.exit(91);
		}

		if ( shouldDownloadDocFiles || shouldJustDownloadHtmlFiles )
			handleDownloadCase();
	}


	private static void handleDatatypeArg(String dataType)
	{
		switch (dataType) {
			case "document":
				logger.info("Going to retrieve only records of \"document\"-type.");
				retrieveDocuments = true;
				retrieveDatasets = false;
				targetUrlType = "docUrl";
				break;
			case "dataset":
				logger.info("Going to retrieve only records of \"dataset\"-type.");
				retrieveDocuments = false;
				retrieveDatasets = true;
				targetUrlType = "datasetUrl";
				break;
			case "all":
				logger.info("Going to retrieve records of all types (documents and datasets).");
				retrieveDocuments = true;
				retrieveDatasets = true;
				targetUrlType = "docOrDatasetUrl";
				break;
			default:
				String errMessage = "Argument: \"" + dataType + "\" was invalid!\nExpected one of the following: \"docFiles | datasets | all\"" + usageMessage;
				System.err.println(errMessage);
				logger.error(errMessage);
				System.exit(9);
		}
	}


	private static void handleFilePathArg(String filePathArg)
	{
		inputFileFullPath = filePathArg;
		if ( !(inputFileFullPath.startsWith(File.separator) || inputFileFullPath.startsWith("~")) )
		{
			if ( inputFileFullPath.startsWith("." + File.separator) )	// Remove the starting "dot" + "/" or "\", if exists.
				inputFileFullPath = StringUtils.replace(inputFileFullPath, "." + File.separator, "", 1);

			inputFileFullPath = System.getProperty("user.dir") + File.separator + inputFileFullPath;	// In case the given path starts with "..", then this also works.
		}
		try {
			inputStream = new BufferedInputStream(new FileInputStream(inputFileFullPath), FileUtils.fiveMb);
		} catch (FileNotFoundException fnfe) {
			String errMessage = "No inputFile was found in \"" + inputFileFullPath + "\"";
			logger.error(errMessage);
			System.err.println(errMessage);
			System.exit(-144);
		} catch (Exception e) {
			String errMessage = e.toString();
			logger.error(errMessage);
			System.err.println(errMessage);
			System.exit(-145);
		}
	}


	private static void handleFileNameType(String nameType)
	{
		switch ( nameType ) {
			case "originalName":
				logger.info("Going to use the \"originalName\" type.");
				fileNameType = fileNameTypeEnum.originalName;
				break;
			case "idName":
				if ( !LoaderAndChecker.useIdUrlPairs ) {
					String errMessage = "You provided the \"fileNameType.idName\", but the program's reader is not set to retrieve IDs from the inputFile! Set the program to retrieve IDs by setting the \"utils.url.LoaderAndChecker.useIdUrlPairs\"-variable to \"true\".";
					System.err.println(errMessage);
					logger.error(errMessage);
					System.exit(10);
				} else {
					logger.info("Going to use the \"idName\" type.");
					fileNameType = fileNameTypeEnum.idName;
				}
				break;
			case "numberName":
				logger.info("Going to use the \"numberName\" type.");
				fileNameType = fileNameTypeEnum.numberName;
				break;
			default:
				String errMessage = "Invalid \"fileNameType\" given (\"" + nameType + "\")\nExpected one of the following: \"originalName | idName | numberName\"" + usageMessage;
				System.err.println(errMessage);
				logger.error(errMessage);
				System.exit(11);
		}
	}


	private static void handleFirstFileNum(String initNumStr)
	{
		try {
			FileUtils.numOfDocFile = initialNumOfFile = Integer.parseInt(initNumStr);    // We use both variables in statistics.
			if ( initialNumOfFile <= 0 ) {
				logger.warn("The given \"initialNumOfFile\" (" + initialNumOfFile + ") was a number less or equal to zero! Setting that number to <1> and continuing downloading..");
				initialNumOfFile = 1;
			}
			firstNumGiven = true;
		} catch (NumberFormatException nfe) {
			String errorMessage = "Argument \"-firstFileNum\" must be followed by an integer value! Given one was: \"" + initNumStr + "\"" + usageMessage;
			System.err.println(errorMessage);
			logger.error(errorMessage);
			System.exit(-2);
		}
	}


	private static void handleDocFilesStorage(String docStorageDir)
	{
		docFilesStorageGivenByUser = true;
		if ( docStorageDir.equals("S3ObjectStore") )
			shouldUploadFilesToS3 = true;
		else
			storeDocFilesDir = docStorageDir + (!docStorageDir.endsWith(File.separator) ? File.separator : "");    // Pre-process it.. otherwise, it may cause problems.
	}


	private static void handleHtmlFilesStorage(String htmlStorageDir)
	{
		htmlFilesStorageGivenByUser = true;
		if ( htmlStorageDir.equals("S3ObjectStore") ) {
			// At the moment, we will not support S3ObjectStore for the HTML-files.
			String errorMessage = "Uploading html-files to S3ObjectStore is not supported at the moment!";
			System.err.println(errorMessage);
			logger.error(errorMessage);
			System.exit(-22);
		} else
			storeHtmlFilesDir = htmlStorageDir + (!htmlStorageDir.endsWith(File.separator) ? File.separator : "");
	}


	private static void handleNumThreads(String workerCountString)
	{
		try {
			workerThreadsCount = initialNumOfFile = Integer.parseInt(workerCountString);    // We use both variables in statistics.
			if ( workerThreadsCount < 1 ) {
				logger.warn("The \"workerThreadsCount\" given was less than < 1 > (" + workerThreadsCount + "), continuing with < 1 > instead..");
				workerThreadsCount = 1;
			}
		} catch (NumberFormatException nfe) {
			logger.error("Invalid \"workerThreadsCount\" was given: \"" + workerCountString + "\".\tContinue by using the system's available threads multiplied by " + threadsMultiplier);
		}
	}


	private static void handleDownloadCase()
	{
		if ( fileNameType == null ) {
			logger.warn("You did not specified the docNameType!" + usageMessage);
			if ( LoaderAndChecker.useIdUrlPairs ) {
				fileNameType = fileNameTypeEnum.idName;
				logger.warn("The program will use the \"idName\"-type!");
			} else {
				fileNameType = fileNameTypeEnum.numberName;
				logger.warn("The program will use the \"numberName\"-type!");
			}
		}

		if ( shouldUploadFilesToS3 && fileNameType.equals(fileNameTypeEnum.originalName) ) {
			String baseMsg = "The uploading of the docFiles to the S3-ObjectStore requires the use of \"ID-names\" or \"Number-names\" for the DocFiles. You specified the \"originalName\" fileNameType.";
			if ( LoaderAndChecker.useIdUrlPairs ) {
				logger.warn(baseMsg + " Replacing the fileNameType \"originalName\" with \"idName\".");
				fileNameType = fileNameTypeEnum.idName;
			} else {
				logger.warn(baseMsg + " Replacing the fileNameType \"originalName\" with \"numberName\".");
				fileNameType = fileNameTypeEnum.numberName;
			}
		}

		if ( firstNumGiven && !fileNameType.equals(fileNameTypeEnum.numberName) )
			logger.warn("You provided the \"-firstFileNum\" a, but you also specified a \"fileNameType\" of non numeric-type. The \"-firstFileNum\" will be ignored!" + usageMessage);
	}

}
