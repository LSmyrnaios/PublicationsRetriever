package eu.openaire.publications_retriever;

import eu.openaire.publications_retriever.crawler.MachineLearning;
import eu.openaire.publications_retriever.crawler.MetadataHandler;
import eu.openaire.publications_retriever.crawler.PageCrawler;
import eu.openaire.publications_retriever.machine_learning.PageStructureMLA;
import eu.openaire.publications_retriever.util.args.ArgsUtils;
import eu.openaire.publications_retriever.util.file.FileUtils;
import eu.openaire.publications_retriever.util.file.HtmlFileUtils;
import eu.openaire.publications_retriever.util.http.ConnSupportUtils;
import eu.openaire.publications_retriever.util.http.DomainConnectionData;
import eu.openaire.publications_retriever.util.http.HttpConnUtils;
import eu.openaire.publications_retriever.util.signal.SignalUtils;
import eu.openaire.publications_retriever.util.url.GenericUtils;
import eu.openaire.publications_retriever.util.url.LoaderAndChecker;
import eu.openaire.publications_retriever.util.url.UrlTypeChecker;
import eu.openaire.publications_retriever.util.url.UrlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;


/**
 * This class contains the entry-point of this program, the "main()" method.
 * The "main()" method calls other methods to set the input/output streams and retrieve the docOrDatasetUrls for each docPage in the inputFile.
 * In the end, the outputFile consists of docPages along with their docUrls.
 * @author Lampros Smyrnaios
 */
public class PublicationsRetriever
{
	private static final Logger logger = LoggerFactory.getLogger(PublicationsRetriever.class);

	public static Instant startTime = null;

	public static final DecimalFormat df = new DecimalFormat("0.00");

	public static ExecutorService executor;


	public static void main( String[] args )
    {
		logger.info("Calling main method with these args: ");
		for ( String arg: args )
			logger.info("'" + arg + "'");

		SignalUtils.setSignalHandlers();

		startTime = Instant.now();

		ArgsUtils.parseArgs(args);

		if ( ! GenericUtils.checkInternetConnectivity() ) {
			FileUtils.closeIO();
			System.exit(-44);
		}

		logger.info("Starting PublicationsRetriever..");
		ConnSupportUtils.setKnownMimeTypes();
		UrlTypeChecker.setRuntimeInitializedRegexes();

		// Check if the user gave the input file in the commandLineArgument, if not, then check for other options.
		if ( ArgsUtils.inputStream == null ) {
			if ( ArgsUtils.inputFromUrl )
				ArgsUtils.inputStream = ConnSupportUtils.getInputStreamFromInputDataUrl();
			else
				ArgsUtils.inputStream = new BufferedInputStream(System.in, FileUtils.fiveMb);
		} else {
			try ( Stream<String> linesStream = Files.lines(Paths.get(ArgsUtils.inputFileFullPath)) ) {
				FileUtils.numOfLines = linesStream.count();
				logger.info("The numOfLines in the inputFile is " + FileUtils.numOfLines);
			} catch (IOException ioe) {
				logger.error("Problem when retrieving the input-\"numOfLines\"!", ioe);
			}
		}

		// Use standard input/output.
		new FileUtils(ArgsUtils.inputStream, System.out);

		if ( MachineLearning.useMLA )
			new MachineLearning();

        executor = Executors.newVirtualThreadPerTaskExecutor();

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
					+ ". The rest " + notConnectedIDs + " urls (about " + df.format(notConnectedIDs * 100.0 / LoaderAndChecker.numOfIDs) + "%) belonged to duplicate (" + FileUtils.duplicateIdUrlEntries +") and/or problematic (" + LoaderAndChecker.numOfIDsWithoutAcceptableSourceUrl + ") IDs.");
		else
			logger.info("Total num of urls (IDs) checked from the input was: " + inputCheckedUrlNum);

		if ( SignalUtils.receivedSIGINT )
			logger.warn("A SIGINT signal was received, so some of the \"checked-urls\" may have not been actually checked, that's more of a number of the \"loaded-urls\".");

		logger.info("Total " + ArgsUtils.targetUrlType + "s found: " + UrlUtils.sumOfDocUrlsFound + ". That's about: " + df.format(UrlUtils.sumOfDocUrlsFound.get() * 100.0 / inputCheckedUrlNum) + "% from the total numOfUrls checked (" + inputCheckedUrlNum + "). The rest were problematic or non-handleable url-cases.");
		if ( ArgsUtils.shouldDownloadDocFiles ) {
			int numOfStoredDocFiles = 0;
			if ( !ArgsUtils.fileNameType.equals(ArgsUtils.fileNameTypeEnum.numberName) )	// If we have anything different from the numberName-type..
				numOfStoredDocFiles = FileUtils.numOfDocFiles.get();
			else
				numOfStoredDocFiles = FileUtils.numOfDocFile - ArgsUtils.initialNumOfFile;
			logger.info("From which docUrls, we were able to retrieve: " + numOfStoredDocFiles + " distinct docFiles. That's about: " + df.format(numOfStoredDocFiles * 100.0 / UrlUtils.sumOfDocUrlsFound.get()) + "%."
					+ " The un-retrieved docFiles were either belonging to already-found " + ArgsUtils.targetUrlType + "s or they had connection-issues or they had problematic content.");
		}
		logger.debug("The metaDocUrl-handler is responsible for the discovery of " + MetadataHandler.numOfMetaDocUrlsFound + " docUrls (" + df.format(MetadataHandler.numOfMetaDocUrlsFound.get() * 100.0 / UrlUtils.sumOfDocUrlsFound.get()) + "% of the found docUrls).");
		logger.debug("The re-crossed " + ArgsUtils.targetUrlType + "s (from all handlers) were " + ConnSupportUtils.reCrossedDocUrls.get() + ". That's about " + df.format(ConnSupportUtils.reCrossedDocUrls.get() * 100.0 / UrlUtils.sumOfDocUrlsFound.get()) + "% of the found docUrls.");

		if ( ArgsUtils.shouldJustDownloadHtmlFiles )
			logger.info("Downloaded " + HtmlFileUtils.htmlFilesNum.get() + " HTML files. That's about: " + df.format(HtmlFileUtils.htmlFilesNum.get() * 100.0 / inputCheckedUrlNum) + "% from the total numOfUrls checked (" + inputCheckedUrlNum + "). The rest either were not pageUrls or they had various issues.");
		else {
			if ( MachineLearning.useMLA )
				logger.debug("The legacy M.L.A. is responsible for the discovery of " + MachineLearning.docUrlsFoundByMLA.get() + " of the " + ArgsUtils.targetUrlType + "s (" + df.format(MachineLearning.docUrlsFoundByMLA.get() * 100.0 / UrlUtils.sumOfDocUrlsFound.get()) + "%). The M.L.A.'s average success-rate was: " + df.format(MachineLearning.getAverageSuccessRate()) + "%. Gathered data for " + MachineLearning.timesGatheredData.get() + " valid pageUrl-docUrl pairs.");
			else
				logger.debug("The legacy M.L.A. was not enabled.");

			logger.debug("The Structure-M.L.A. is responsible for the discovery of " + PageStructureMLA.structureValidatedDocLinks.get() + " of the " + ArgsUtils.targetUrlType + "s (" + df.format(PageStructureMLA.structureValidatedDocLinks.get() * 100.0 / UrlUtils.sumOfDocUrlsFound.get()) + "%).");
			logger.debug("In total, it predicted " + PageStructureMLA.structurePredictedDocLinks.get() + " docLinks, with some of them not leading to a fulltext for various reasons (connection-problem, removed-file, unsupported docType, ect.).");
		}

		logger.debug("About " + df.format(LoaderAndChecker.connProblematicUrls.get() * 100.0 / inputCheckedUrlNum) + "% (" + LoaderAndChecker.connProblematicUrls.get() + " urls) were pages which had connectivity problems.");
		logger.debug("About " + df.format(MetadataHandler.numOfProhibitedAccessPagesFound.get() * 100.0 / inputCheckedUrlNum) + "% (" + MetadataHandler.numOfProhibitedAccessPagesFound.get() + " urls) were pages with prohibited access.");
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
				logger.info("The remaining " + remainingNonProblematicUrls + " urls either did not provide a fulltext or their status is unknown since " + failedTasks + " of them failed.");
			} else
				logger.info("The remaining " + remainingNonProblematicUrls + " urls did not provide a fulltext.");
		}

		logger.debug("The number of offline-redirects to HTTPS (reducing the online-redirection-overhead), was: " + HttpConnUtils.timesDidOfflineHTTPSredirect.get());
		logger.debug("The number of offline-redirects to slash-ending url (reducing the online-redirection-overhead), was: " + HttpConnUtils.timesDidOfflineSlashRedirect.get());

		logger.debug("The number of contentTypes which were extracted from the body of http-responses was: " + ConnSupportUtils.numContentTypeExtractedFromPageContent.get());

		logger.debug("The number of domains blocked due to an \"SSL Exception\", was: " + HttpConnUtils.numOfDomainsBlockedDueToSSLException.get());
		logger.debug("The number of domains blocked in total, during runtime, was: " + HttpConnUtils.blacklistedDomains.size());
		logger.debug("The number of paths blocked -due to HTTP 403- in total, was: " + ConnSupportUtils.domainsMultimapWithPaths403BlackListed.values().size());

		calculateAndPrintElapsedTime(startTime, Instant.now(), null);

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

			//sortConcurrentHashMapByValueAndPrint(UrlUtils.domainsAndHits, true);

			// DEBUG! comment-out the following in production (even in debug-mode).
			/*if ( MachineLearning.useMLA )
				MachineLearning.printGatheredData();*/
		}
	}


	public static void calculateAndPrintElapsedTime(Instant startTime, Instant finishTime, String customMessage)
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
		
		String millisMessage;
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
		
		logger.info(((customMessage != null) ? customMessage : "The program finished after: ") + daysMessage + hoursMessage + minutesMessage + secondsMessage + millisMessage);
	}


	public static void sortConcurrentHashMapByValueAndPrint(ConcurrentHashMap<String, Integer> table, boolean descendingOrder)
	{
		List<Map.Entry<String, Integer>> list = new LinkedList<>(table.entrySet());
		list.sort((o1, o2) -> {
			if ( descendingOrder )
				return o2.getValue().compareTo(o1.getValue());
			else
				return o1.getValue().compareTo(o2.getValue());
		});
		logger.debug("The " + list.size() + " domains which gave " + ArgsUtils.targetUrlType + "s and their number:");
/*		for ( Map.Entry<String, Integer> entry : list )
			logger.debug(entry.getKey() + " : " + entry.getValue());*/
	}
	
}
