package eu.openaire.publications_retriever;

import eu.openaire.publications_retriever.crawler.MachineLearning;
import eu.openaire.publications_retriever.crawler.MetaDocUrlsHandler;
import eu.openaire.publications_retriever.crawler.PageCrawler;
import eu.openaire.publications_retriever.util.file.FileUtils;
import eu.openaire.publications_retriever.util.http.ConnSupportUtils;
import eu.openaire.publications_retriever.util.http.DomainConnectionData;
import eu.openaire.publications_retriever.util.http.HttpConnUtils;
import eu.openaire.publications_retriever.util.signal.SignalUtils;
import eu.openaire.publications_retriever.util.url.GenericUtils;
import eu.openaire.publications_retriever.util.url.LoaderAndChecker;
import eu.openaire.publications_retriever.util.url.UrlTypeChecker;
import eu.openaire.publications_retriever.util.url.UrlUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;


/**
 * This class contains the entry-point of this program, the "main()" method.
 * The "main()" method calls other methods to set the input/output streams and retrieve the docUrls for each docPage in the inputFile.
 * In the end, the outputFile consists of docPages along with their docUrls.
 * @author Lampros Smyrnaios
 */
public class PublicationsRetriever
{
	private static final Logger logger = LoggerFactory.getLogger(PublicationsRetriever.class);

	private static int initialNumOfDocFile = 0;

	public static boolean docFilesStorageGivenByUser = false;

	public static boolean inputFromUrl = false;
	public static String inputDataUrl = null;

	public static InputStream inputStream = null;
	public static String inputFileFullPath = null;

	public static Instant startTime = null;
	public static String targetUrlType = "docOrDatasetUrl";	// docUrl, documentUrl, docOrDatasetUrl ; this is set by the args-parser, and it's used when outputting data.

	public static DecimalFormat df = new DecimalFormat("0.00");

	public static ExecutorService executor;
	public static int workerThreadsCount = 0;
	public static int threadsMultiplier = 2;	// Use *3 without downloading docFiles and when having the domains to appear in uniform distribution in the inputFile. Use *2 when downloading.


	public static void main( String[] args )
    {
		logger.info("Calling main method with these args: ");
		for ( String arg: args )
			logger.info("'" + arg + "'");

		SignalUtils.setSignalHandlers();

		startTime = Instant.now();

		parseArgs(args);

		if ( ! GenericUtils.checkInternetConnectivity() ) {
			FileUtils.closeIO();
			System.exit(-44);
		}

		logger.info("Starting PublicationsRetriever..");
		ConnSupportUtils.setKnownMimeTypes();

		// Check if the user gave the input file in the commandLineArgument, if not, then check for other options.
		if ( PublicationsRetriever.inputStream == null ) {
			if ( PublicationsRetriever.inputFromUrl )
				PublicationsRetriever.inputStream = ConnSupportUtils.getInputStreamFromInputDataUrl();
			else
				PublicationsRetriever.inputStream = System.in;
		} else {
			try ( Stream<String> linesStream = Files.lines(Paths.get(PublicationsRetriever.inputFileFullPath)) ) {
				FileUtils.numOfLines = linesStream.count();
				logger.info("The numOfLines in the inputFile is " + FileUtils.numOfLines);
			} catch (IOException ioe) {
				logger.error("Problem when retrieving the input-\"numOfLines\"!", ioe);
			}
		}

		// Use standard input/output.
		new FileUtils(inputStream, System.out);

		if ( MachineLearning.useMLA )
			new MachineLearning();

		if ( workerThreadsCount == 0 ) {	// If the user did not provide the "workerThreadsCount", then get the available number from the system.
			int availableThreads = Runtime.getRuntime().availableProcessors();
			availableThreads *= threadsMultiplier;

			// If the domains of the urls in the inputFile, are in "uniform distribution" (each one of them to be equally likely to appear in any place), then the more threads the better (triple the computer's number)
			// Else, if there are far lees domains and/or closely placed inside the inputFile.. then use only the number of threads provided by the computer, since the "politenessDelay" will block them more than the I/O would ever do..
			workerThreadsCount = availableThreads;	// Due to I/O, blocking the threads all the time, more threads handle the workload faster..
		}
		logger.info("Use " + workerThreadsCount + " worker-threads.");
		executor = Executors.newFixedThreadPool(workerThreadsCount);

		try {
			new LoaderAndChecker();
		} catch (RuntimeException e) {  // In case there was no input, a RuntimeException will be thrown, after logging the cause.
			String errorMessage = "There was a serious error! Output data is affected! Exiting..";
			System.err.println(errorMessage);
			logger.error(errorMessage);
			FileUtils.closeIO();
			executor.shutdownNow();
			System.exit(-4);
		}

		logger.info("Shutting down the threads..");
		PublicationsRetriever.executor.shutdown();	// Define that no new tasks will be scheduled.
		try {
			if ( !PublicationsRetriever.executor.awaitTermination(1, TimeUnit.MINUTES) ) {
				logger.warn("The working threads did not finish on time! Stopping them immediately..");
				PublicationsRetriever.executor.shutdownNow();
			}
		} catch (SecurityException se) {
			logger.error("Could not shutdown the threads in any way..!", se);
		} catch (InterruptedException ie) {
			try {
				PublicationsRetriever.executor.shutdownNow();
			} catch (SecurityException se) {
				logger.error("Could not shutdown the threads in any way..!", se);
			}
		}

		showStatistics(startTime);

		// Close the open streams (imported and exported content).
		FileUtils.closeIO();
    }


	public static void parseArgs(String[] mainArgs)
	{
		String usageMessage = "\nUsage: java -jar publications_retriever-<VERSION>.jar -retrieveDataType <dataType: document | dataset | all> -inputFileFullPath inputFile -downloadDocFiles(OPTIONAL) -docFileNameType(OPTIONAL) <nameType: originalName | idName | numberName> -firstDocFileNum(OPTIONAL) 'num' -docFilesStorage(OPTIONAL) 'storageDir' -inputDataUrl(OPTIONAL) 'inputUrl' -numOfThreads(OPTIONAL) 'threadsNum' < 'input' > 'output'";

		if ( mainArgs.length > 15 ) {
			String errMessage = "\"PublicationsRetriever\" expected only up to 15 arguments, while you gave: " + mainArgs.length + "!" + usageMessage;
			logger.error(errMessage);
			System.err.println(errMessage);
			System.exit(-1);
		}

		boolean firstNumGiven = false;

		for ( short i = 0; i < mainArgs.length; i++ )
		{
			try {
				switch ( mainArgs[i] ) {
					case "-retrieveDataType":
						i ++;
						String dataType = mainArgs[i];
						switch (dataType) {
							case "document":
								logger.info("Going to retrieve only records of \"document\"-type.");
								LoaderAndChecker.retrieveDocuments = true;
								LoaderAndChecker.retrieveDatasets = false;
								targetUrlType = "docUrl";
								break;
							case "dataset":
								logger.info("Going to retrieve only records of \"dataset\"-type.");
								LoaderAndChecker.retrieveDocuments = false;
								LoaderAndChecker.retrieveDatasets = true;
								targetUrlType = "datasetUrl";
								break;
							case "all":
								logger.info("Going to retrieve records of all types (documents and datasets).");
								LoaderAndChecker.retrieveDocuments = true;
								LoaderAndChecker.retrieveDatasets = true;
								targetUrlType = "docOrDatasetUrl";
								break;
							default:
								String errMessage = "Argument: \"" + dataType + "\" was invalid!\nExpected one of the following: \"docFiles | datasets | all\"" + usageMessage;
								System.err.println(errMessage);
								logger.error(errMessage);
								System.exit(9);
						}
						break;
					case "-inputFileFullPath":
						i ++;
						inputFileFullPath = mainArgs[i];
						if ( !(inputFileFullPath.startsWith(File.separator) || inputFileFullPath.startsWith("~")) )
						{
							if ( inputFileFullPath.startsWith("." + File.separator) )	// Remove the starting "dot" + "/" or "\", if exists.
								inputFileFullPath = StringUtils.replace(inputFileFullPath, "." + File.separator, "", 1);

							inputFileFullPath = System.getProperty("user.dir") + File.separator + inputFileFullPath;	// In case the given path starts with "..", then this also works.
						}
						try {
							inputStream = new FileInputStream(inputFileFullPath);
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
						break;
					case "-downloadDocFiles":
						FileUtils.shouldDownloadDocFiles = true;
						break;
					case "-docFileNameType":
						i ++;
						String nameType = mainArgs[i];
						switch ( nameType ) {
							case "originalName":
								logger.info("Going to use the \"originalName\" type.");
								FileUtils.docFileNameType = FileUtils.DocFileNameType.originalName;
								break;
							case "idName":
								if ( !LoaderAndChecker.useIdUrlPairs ) {
									String errMessage = "You provided the \"DocFileNameType.idName\", but the program's reader is not set to retrieve IDs from the inputFile! Set the program to retrieve IDs by setting the \"utils.url.LoaderAndChecker.useIdUrlPairs\"-variable to \"true\".";
									System.err.println(errMessage);
									logger.error(errMessage);
									System.exit(10);
								} else {
									logger.info("Going to use the \"idName\" type.");
									FileUtils.docFileNameType = FileUtils.DocFileNameType.idName;
								}
								break;
							case "numberName":
								logger.info("Going to use the \"numberName\" type.");
								FileUtils.docFileNameType = FileUtils.DocFileNameType.numberName;
								break;
							default:
								String errMessage = "Invalid \"docFileNameType\" given (\"" + nameType + "\")\nExpected one of the following: \"originalName | idName | numberName\"" + usageMessage;
								System.err.println(errMessage);
								logger.error(errMessage);
								System.exit(11);
						}
						break;
					case "-firstDocFileNum":
						try {
							i ++;	// Go get the following first-Number-argument.
							FileUtils.numOfDocFile = PublicationsRetriever.initialNumOfDocFile = Integer.parseInt(mainArgs[i]);    // We use both variables in statistics.
							if ( PublicationsRetriever.initialNumOfDocFile <= 0 ) {
								logger.warn("The given \"initialNumOfDocFile\" (" + PublicationsRetriever.initialNumOfDocFile + ") was a number less or equal to zero! Setting that number to <1> and continuing downloading..");
								PublicationsRetriever.initialNumOfDocFile = 1;
							}
							firstNumGiven = true;
							break;
						} catch (NumberFormatException nfe) {
							String errorMessage = "Argument \"-firstDocFileNum\" must be followed by an integer value! Given one was: \"" + mainArgs[i] + "\"" + usageMessage;
							System.err.println(errorMessage);
							logger.error(errorMessage);
							System.exit(-2);
						}
					case "-docFilesStorage":
						i ++;
						String storageDir = mainArgs[i];
						if ( storageDir.equals("S3ObjectStore") )
							FileUtils.shouldUploadFilesToS3 = true;
						else
							FileUtils.storeDocFilesDir = storageDir + (!storageDir.endsWith(File.separator) ? File.separator : "");    // Pre-process it.. otherwise, it may cause problems.
						PublicationsRetriever.docFilesStorageGivenByUser = true;
						break;
					case "-inputDataUrl":
						i++;
						inputDataUrl = mainArgs[i];
						inputFromUrl = true;
						logger.info("Using the inputFile from the URL: " + inputDataUrl);
						break;
					case "-numOfThreads":
						i++;
						String workerCountString = mainArgs[i];
						try {
							workerThreadsCount = PublicationsRetriever.initialNumOfDocFile = Integer.parseInt(workerCountString);    // We use both variables in statistics.
							if ( workerThreadsCount < 1 ) {
								logger.warn("The \"workerThreadsCount\" given was less than < 1 > (" + workerThreadsCount + "), continuing with < 1 > instead..");
								workerThreadsCount = 1;
							}
						} catch (NumberFormatException nfe) {
							logger.error("Invalid \"workerThreadsCount\" was given: \"" + workerCountString + "\".\tContinue by using the system's available threads multiplied by " + threadsMultiplier);
						}
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

		if ( FileUtils.shouldDownloadDocFiles )
		{
			if ( FileUtils.docFileNameType == null ) {
				logger.warn("You did not specified the docNameType!" + usageMessage);
				if ( LoaderAndChecker.useIdUrlPairs ) {
					FileUtils.docFileNameType = FileUtils.DocFileNameType.idName;
					logger.warn("The program will use the \"idName\"-type!");
				} else {
					FileUtils.docFileNameType = FileUtils.DocFileNameType.numberName;
					logger.warn("The program will use the \"numberName\"-type!");
				}
			}

			if ( FileUtils.shouldUploadFilesToS3 && FileUtils.docFileNameType.equals(FileUtils.DocFileNameType.originalName) ) {
				String baseMsg = "The uploading of the docFiles to the S3-ObjectStore requires the use of \"ID-names\" or \"Number-names\" for the DocFiles. You specified the \"originalName\" DocFileNameType.";
				if ( LoaderAndChecker.useIdUrlPairs ) {
					logger.warn(baseMsg + " Replacing the DocFileNameType \"originalName\" with \"idName\".");
					FileUtils.docFileNameType = FileUtils.DocFileNameType.idName;
				} else {
					logger.warn(baseMsg + " Replacing the DocFileNameType \"originalName\" with \"numberName\".");
					FileUtils.docFileNameType = FileUtils.DocFileNameType.numberName;
				}
			}

			if ( firstNumGiven && !FileUtils.docFileNameType.equals(FileUtils.DocFileNameType.numberName) )
				logger.warn("You provided the \"-firstDocFileNum\" a, but you also specified a \"docFileNameType\" of non numeric-type. The \"-firstDocFileNum\" will be ignored!" + usageMessage);
		}
	}


	public static void showStatistics(Instant startTime)
	{
		long inputCheckedUrlNum = 0;
		long notConnectedIDs = 0;
		int currentlyLoadedUrls = FileUtils.getCurrentlyLoadedUrls();

		if ( LoaderAndChecker.useIdUrlPairs ) {
			logger.debug(LoaderAndChecker.numOfIDsWithoutAcceptableSourceUrl.get() + " IDs (about " + df.format(LoaderAndChecker.numOfIDsWithoutAcceptableSourceUrl.get() * 100.0 / LoaderAndChecker.numOfIDs) + "%) had no acceptable sourceUrl.");
			notConnectedIDs = LoaderAndChecker.numOfIDsWithoutAcceptableSourceUrl.get() + FileUtils.duplicateIdUrlEntries;
			inputCheckedUrlNum = LoaderAndChecker.numOfIDs - notConnectedIDs;	// For each ID we usually check only one of its urls, except if the chosen one fails to connect. But if we add here the retries, then we should add how many more codUrls were retrieved per Id, later...
		} else {
			inputCheckedUrlNum = currentlyLoadedUrls;
			if ( (FileUtils.skipFirstRow && (inputCheckedUrlNum < 0)) || (!FileUtils.skipFirstRow && (inputCheckedUrlNum == 0)) ) {
				String errorMessage = "\"FileUtils.getCurrentlyLoadedUrls()\" is unexpectedly reporting that no urls were retrieved from input file! Output data may be affected! Exiting..";
				System.err.println(errorMessage);
				logger.error(errorMessage);
				FileUtils.closeIO();
				PublicationsRetriever.executor.shutdownNow();
				System.exit(-5);
			}
		}

		if ( LoaderAndChecker.useIdUrlPairs && (inputCheckedUrlNum < currentlyLoadedUrls) )
			logger.info("Total num of urls (IDs) checked (& connected) from the input was: " + inputCheckedUrlNum
					+ ". The rest " + notConnectedIDs + " urls (about " + df.format(notConnectedIDs * 100.0 / LoaderAndChecker.numOfIDs) + "%) belonged to duplicate (" + FileUtils.duplicateIdUrlEntries +") or problematic (" + LoaderAndChecker.numOfIDsWithoutAcceptableSourceUrl + ") IDs.");
		else
			logger.info("Total num of urls (IDs) checked from the input was: " + inputCheckedUrlNum);

		if ( SignalUtils.receivedSIGINT )
			logger.warn("A SIGINT signal was received, so some of the \"checked-urls\" may have not been actually checked, that's more of a number of the \"loaded-urls\".");

		logger.info("Total " + targetUrlType + "s found: " + UrlUtils.sumOfDocUrlsFound + ". That's about: " + df.format(UrlUtils.sumOfDocUrlsFound.get() * 100.0 / inputCheckedUrlNum) + "% from the total numOfUrls checked. The rest were problematic or non-handleable url-cases.");
		if ( FileUtils.shouldDownloadDocFiles ) {
			int numOfStoredDocFiles = 0;
			if ( !FileUtils.docFileNameType.equals(FileUtils.DocFileNameType.numberName) )	// If we have anything than the numberName-type..
				numOfStoredDocFiles = FileUtils.numOfDocFile;
			else
				numOfStoredDocFiles = FileUtils.numOfDocFile - initialNumOfDocFile;
			logger.info("From which docUrls, we were able to retrieve: " + numOfStoredDocFiles + " distinct docFiles. That's about: " + df.format(numOfStoredDocFiles * 100.0 / UrlUtils.sumOfDocUrlsFound.get()) + "%."
					+ " The un-retrieved docFiles were either belonging to already-found docUrls or they had connection-issues.");
		}
		logger.debug("The metaDocUrl-handler is responsible for the discovery of " + MetaDocUrlsHandler.numOfMetaDocUrlsFound + " docUrls (" + df.format(MetaDocUrlsHandler.numOfMetaDocUrlsFound.get() * 100.0 / UrlUtils.sumOfDocUrlsFound.get()) + "% of the found docUrls).");
		logger.debug("The re-crossed docUrls (from all handlers) were " + ConnSupportUtils.reCrossedDocUrls.get() + ". That's about " + df.format(ConnSupportUtils.reCrossedDocUrls.get() * 100.0 / UrlUtils.sumOfDocUrlsFound.get()) + "% of the found docUrls.");
		if ( MachineLearning.useMLA )
			logger.debug("The M.L.A. is responsible for the discovery of " + MachineLearning.docUrlsFoundByMLA.get() + " of the docUrls (" + df.format(MachineLearning.docUrlsFoundByMLA.get() * 100.0 / UrlUtils.sumOfDocUrlsFound.get()) + "%). The M.L.A.'s average success-rate was: " + df.format(MachineLearning.getAverageSuccessRate()) + "%. Gathered data for " + MachineLearning.timesGatheredData.get() + " valid pageUrl-docUrl pairs.");
		else
			logger.debug("The M.L.A. was not enabled.");

		logger.debug("About " + df.format(LoaderAndChecker.connProblematicUrls.get() * 100.0 / inputCheckedUrlNum) + "% (" + LoaderAndChecker.connProblematicUrls.get() + " urls) were pages which had connectivity problems.");
		logger.debug("About " + df.format(UrlTypeChecker.pagesNotProvidingDocUrls.get() * 100.0 / inputCheckedUrlNum) + "% (" + UrlTypeChecker.pagesNotProvidingDocUrls.get() + " urls) were pages which did not provide docUrls.");
		logger.debug("About " + df.format(UrlTypeChecker.longToRespondUrls.get() * 100.0 / inputCheckedUrlNum) + "% (" + UrlTypeChecker.longToRespondUrls.get() + " urls) were urls which belong to domains which take too long to respond.");
		logger.debug("About " + df.format(PageCrawler.contentProblematicUrls.get() * 100.0 / inputCheckedUrlNum) + "% (" + PageCrawler.contentProblematicUrls.get() + " urls) were urls which had problematic content.");

		long problematicUrlsNum = LoaderAndChecker.connProblematicUrls.get() + UrlTypeChecker.pagesNotProvidingDocUrls.get() + UrlTypeChecker.longToRespondUrls.get() + PageCrawler.contentProblematicUrls.get();

		if ( !LoaderAndChecker.useIdUrlPairs )
		{
			logger.debug("About " + df.format(UrlTypeChecker.crawlerSensitiveDomains.get() * 100.0 / inputCheckedUrlNum) + "% (" + UrlTypeChecker.crawlerSensitiveDomains.get()  + " urls) were from known crawler-sensitive domains.");
			logger.debug("About " + df.format(UrlTypeChecker.javascriptPageUrls.get() * 100.0 / inputCheckedUrlNum) + "% (" + UrlTypeChecker.javascriptPageUrls.get() + " urls) were from a JavaScript-powered domain, other than the \"sciencedirect.com\", which has dynamic links.");
			logger.debug("About " + df.format(UrlTypeChecker.doajResultPageUrls.get() * 100.0 / inputCheckedUrlNum) + "% (" + UrlTypeChecker.doajResultPageUrls.get() + " urls) were \"doaj.org/toc/\" urls, which are resultPages, thus being avoided to be crawled.");
			logger.debug("About " + df.format(UrlTypeChecker.pagesWithHtmlDocUrls.get() * 100.0 / inputCheckedUrlNum) + "% (" + UrlTypeChecker.pagesWithHtmlDocUrls.get() + " urls) were docUrls, but, in HTML, thus being avoided to be crawled.");
			logger.debug("About " + df.format(UrlTypeChecker.pagesRequireLoginToAccessDocFiles.get() * 100.0 / inputCheckedUrlNum) + "% (" + UrlTypeChecker.pagesRequireLoginToAccessDocFiles.get() + " urls) were of domains which are known to require login to access docFiles, thus, they were blocked before being connected.");
			logger.debug("About " + df.format(UrlTypeChecker.pagesWithLargerCrawlingDepth.get() * 100.0 / inputCheckedUrlNum) + "% (" + UrlTypeChecker.pagesWithLargerCrawlingDepth.get() + " urls) were docPages which have their docUrl deeper inside their server, thus being currently avoided.");
			logger.debug("About " + df.format(UrlTypeChecker.pangaeaUrls.get() * 100.0 / inputCheckedUrlNum) + "% (" + UrlTypeChecker.pangaeaUrls + " urls) were \"PANGAEA.\" with invalid form and non-docUrls in their internal links.");
			logger.debug("About " + df.format(UrlTypeChecker.urlsWithUnwantedForm.get() * 100.0 / inputCheckedUrlNum) + "% (" + UrlTypeChecker.urlsWithUnwantedForm.get() + " urls) were urls which are plain-domains, have unwanted url-extensions, ect...");
			logger.debug("About " + df.format(LoaderAndChecker.inputDuplicatesNum.get() * 100.0 / inputCheckedUrlNum) + "% (" + LoaderAndChecker.inputDuplicatesNum.get() + " urls) were duplicates in the input file.");

			problematicUrlsNum += UrlTypeChecker.crawlerSensitiveDomains.get() + UrlTypeChecker.javascriptPageUrls.get() + UrlTypeChecker.doajResultPageUrls.get() + UrlTypeChecker.pagesWithHtmlDocUrls.get() + UrlTypeChecker.pagesRequireLoginToAccessDocFiles.get()
					+ UrlTypeChecker.pagesWithLargerCrawlingDepth.get() + UrlTypeChecker.pangaeaUrls.get() + UrlTypeChecker.urlsWithUnwantedForm.get() + LoaderAndChecker.inputDuplicatesNum.get();
		}

		logger.info("From the " + inputCheckedUrlNum + " urls checked from the input, the " + problematicUrlsNum + " of them (about " + df.format(problematicUrlsNum * 100.0 / inputCheckedUrlNum) + "%) were problematic (sum of all of the cases that appear in debug-mode).");

		long remainingNonProblematicUrls = inputCheckedUrlNum + LoaderAndChecker.loadingRetries.get() - UrlUtils.sumOfDocUrlsFound.get() - problematicUrlsNum;
		if ( remainingNonProblematicUrls > 0 ) {
			int failedTasks = LoaderAndChecker.totalNumFailedTasks.get();
			if ( failedTasks > 0 ) {
				remainingNonProblematicUrls -= failedTasks;
				logger.info("The rest " + remainingNonProblematicUrls + " urls were either not docUrls or their status is unknown since " + failedTasks + " of them failed.");
			} else
				logger.info("The rest " + remainingNonProblematicUrls + " urls were not docUrls.");
		}

		logger.debug("The number of offline-redirects to HTTPS (reducing the online-redirection-overhead), was: " + HttpConnUtils.timesDidOfflineHTTPSredirect.get());
		logger.debug("The number of offline-redirects to slash-ending url (reducing the online-redirection-overhead), was: " + HttpConnUtils.timesDidOfflineSlashRedirect.get());

		logger.debug("The number of domains blocked due to an \"SSL Exception\", was: " + HttpConnUtils.numOfDomainsBlockedDueToSSLException.get());
		logger.debug("The number of domains blocked in total, was: " + HttpConnUtils.blacklistedDomains.size());
		logger.debug("The number of paths blocked -due to HTTP 403- in total, was: " + ConnSupportUtils.domainsMultimapWithPaths403BlackListed.values().size());

		calculateAndPrintElapsedTime(startTime, Instant.now());
		logger.debug("Used " + workerThreadsCount + " worker threads.");

		if ( logger.isDebugEnabled() )
		{
			List<Map.Entry<String, DomainConnectionData>> list = new LinkedList<>(ConnSupportUtils.domainsWithConnectionData.entrySet());
			Comparator<Map.Entry<String, DomainConnectionData>> comparator = Comparator.comparingInt(o -> o.getValue().getTimesConnected());
			list.sort(comparator.reversed());	// Descending order.
			logger.debug(list.size() + " domains : timesConnected");
			for ( Map.Entry<String, DomainConnectionData> domainWithLock : list )
			{
				logger.debug(domainWithLock.getKey() + " : " + domainWithLock.getValue().getTimesConnected());
			}

			//sortHashTableByValueAndPrint(UrlUtils.domainsAndHits, true);

			// DEBUG! comment-out the following in production (even in debug-mode).
			/*if ( MachineLearning.useMLA )
				MachineLearning.printGatheredData();*/
		}
	}


	public static void calculateAndPrintElapsedTime(Instant startTime, Instant finishTime)
	{
		/*
		Calculating time using the following method-example.
			2904506 millis
			secs = millis / 1000 = 2904506 / 1000 = 2904.506 secs = 2904secs + 506millis
			remaining millis = 506
			mins = secs / 60 = 2904 / 60 = 48.4 mins = 48mins + (0.4 * 60) secs = 48 mins + 24 secs
			remaining secs = 24
		Elapsed time --> "48 minutes, 24 seconds and 506 milliseconds."
		 */
		
		long timeElapsedMillis = Duration.between(startTime, finishTime).toMillis();
		
		// Millis - Secs
		double timeElapsedSecs = (double)timeElapsedMillis / 1000;	// 0.006
		long secs = (long)Math.floor(timeElapsedSecs);	// 0
		long remainingMillis = (long)((timeElapsedSecs - secs) * 1000);	// (0.006 - 0) / 1000 = 0.006 * 1000 = 6
		
		String millisMessage = "";
		if ( (secs > 0) && (remainingMillis > 0) )
			millisMessage = " and " + remainingMillis + " milliseconds.";
		else
			millisMessage = timeElapsedMillis + " milliseconds.";
		
		// Secs - Mins
		double timeElapsedMins = (double)secs / 60;
		long mins = (long)Math.floor(timeElapsedMins);
		long remainingSeconds = (long)((timeElapsedMins - mins) * 60);
		
		String secondsMessage = "";
		if ( remainingSeconds > 0 )
			secondsMessage = remainingSeconds + " seconds";
		
		// Mins - Hours
		double timeElapsedHours = (double)mins / 60;
		long hours = (long)Math.floor(timeElapsedHours);
		long remainingMinutes = (long)((timeElapsedHours - hours) * 60);
		
		String minutesMessage = "";
		if ( remainingMinutes > 0 )
			minutesMessage = remainingMinutes + " minutes, ";
		
		// Hours - Days
		double timeElapsedDays = (double)hours / 24;
		long days = (long)Math.floor(timeElapsedDays);
		long remainingHours = (long)((timeElapsedDays - days) * 24);
		
		String hoursMessage = "";
		if ( remainingHours > 0 )
			hoursMessage = remainingHours + " hours, ";
		
		String daysMessage = "";
		if ( days > 0 )
			daysMessage = days + " days, ";
		
		logger.info("The program finished after: " + daysMessage + hoursMessage + minutesMessage + secondsMessage + millisMessage);
	}


	public static void sortHashTableByValueAndPrint(Hashtable<String, Integer> table, boolean descendingOrder)
	{
		List<Map.Entry<String, Integer>> list = new LinkedList<>(table.entrySet());
		list.sort((o1, o2) -> {
			if ( descendingOrder )
				return o2.getValue().compareTo(o1.getValue());
			else
				return o1.getValue().compareTo(o2.getValue());
		});
		logger.debug("The " + list.size() + " domains which gave docUrls and their number:");
/*		for ( Map.Entry<String, Integer> entry : list )
			logger.debug(entry.getKey() + " : " + entry.getValue());*/
	}
	
}
