package eu.openaire.doc_urls_retriever;

import eu.openaire.doc_urls_retriever.crawler.MachineLearning;
import eu.openaire.doc_urls_retriever.crawler.MetaDocUrlsHandler;
import eu.openaire.doc_urls_retriever.crawler.PageCrawler;
import eu.openaire.doc_urls_retriever.util.file.FileUtils;
import eu.openaire.doc_urls_retriever.util.http.HttpConnUtils;
import eu.openaire.doc_urls_retriever.util.signal.SignalUtils;
import eu.openaire.doc_urls_retriever.util.url.LoaderAndChecker;
import eu.openaire.doc_urls_retriever.util.url.UrlTypeChecker;
import eu.openaire.doc_urls_retriever.util.url.UrlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;


/**
 * This class contains the entry-point of this program, the "main()" method.
 * The "main()" method calls other methods to set the input/output streams and retrieve the docUrls for each docPage in the inputFile.
 * In the end, the outputFile consists of docPages along with their docUrls.
 * @author Lampros Smyrnaios
 */
public class DocUrlsRetriever
{
	private static final Logger logger = LoggerFactory.getLogger(DocUrlsRetriever.class);

	private static int initialNumOfDocFile = 0;

	public static boolean docFilesStorageGivenByUser = false;

	public static Instant startTime = null;

	public static DecimalFormat df = new DecimalFormat("0.00");


	public static void main( String[] args )
    {
		SignalUtils.setSignalHandlers();

		startTime = Instant.now();

		parseArgs(args);

		logger.info("Starting DocUrlsRetriever..");

		// Use standard input/output.
		new FileUtils(System.in, System.out);

		if ( MachineLearning.useMLA )
			new MachineLearning();

		try {
			new LoaderAndChecker();
		} catch (RuntimeException e) {  // In case there was no input, a RuntimeException will be thrown, after logging the cause.
			String errorMessage = "There was a serious error! Output data is affected! Exiting..";
			System.err.println(errorMessage);
			logger.error(errorMessage);
			FileUtils.closeIO();
			System.exit(-4);
		}

		showStatistics(startTime);

		// Close the open streams (imported and exported content).
		FileUtils.closeIO();
    }


	public static void parseArgs(String[] mainArgs)
	{
		if ( mainArgs.length > 7 ) {
			String errMessage = "\"DocUrlsRetriever\" expected only up to 7 arguments, while you gave: " + mainArgs.length + "!";
			logger.error(errMessage);
			System.err.println(errMessage
					+ "\nUsage: java -jar doc_urls_retriever-<VERSION>.jar -retrieveDataType <dataType: document | dataset | all>  -downloadDocFiles(OPTIONAL) -firstDocFileNum(OPTIONAL) 'num' -docFilesStorage(OPTIONAL) 'storageDir' < 'input' > 'output'");
			System.exit(-1);
		}

		boolean firstNumGiven = false;

		for ( short i = 0; i < mainArgs.length; i++ )
		{
			switch ( mainArgs[i] )
			{
				case "-retrieveDataType":
					i ++;
					String dataType = mainArgs[i];
					switch (dataType) {
						case "document":
							logger.info("Going to retrieve only records of \"document\"-type.");
							LoaderAndChecker.retrieveDocuments = true;
							LoaderAndChecker.retrieveDatasets = false;
							break;
						case "dataset":
							logger.info("Going to retrieve only records of \"dataset\"-type.");
							LoaderAndChecker.retrieveDocuments = false;
							LoaderAndChecker.retrieveDatasets = true;
							break;
						case "all":
							logger.info("Going to retrieve records of all types (documents and datasets).");
							LoaderAndChecker.retrieveDocuments = true;
							LoaderAndChecker.retrieveDatasets = true;
							break;
						default:
							String errMessage = "Argument: \"" + dataType + "\" was invalid!\nExpected one of the following: \"docFiles | datasets | all\"";
							System.err.println(errMessage);
							logger.error(errMessage);
							System.exit(9);
					}
					break;
				case "-downloadDocFiles":
					FileUtils.shouldDownloadDocFiles = true;
					break;
				case "-firstDocFileNum":
					try {
						i ++;	// Go get the following first-Number-argument.
						FileUtils.numOfDocFile = DocUrlsRetriever.initialNumOfDocFile = Integer.parseInt(mainArgs[i]);	// We use both variables in statistics.
						if ( DocUrlsRetriever.initialNumOfDocFile <= 0 )
							logger.warn("The given \"initialNumOfDocFile\" (" + DocUrlsRetriever.initialNumOfDocFile + ") was a number less or equal to zero! Continuing downloading nevertheless..");	// The statistics are not affected by a "negative" or "zero" value.
						firstNumGiven = true;
						break;
					} catch (NumberFormatException nfe) {
						String errorMessage = "Argument \"-firstDocFileNum\" must be followed by an integer value! Given one was: \"" + mainArgs[i] + "\"";
						System.err.println(errorMessage);
						logger.error(errorMessage);
						System.exit(-2);
					}
				case "-docFilesStorage":
					i ++;
					String dir = mainArgs[i];
					FileUtils.storeDocFilesDir = dir + (!dir.endsWith(File.separator) ? File.separator : "");	// Pre-process it.. otherwise it may cause problems.
					DocUrlsRetriever.docFilesStorageGivenByUser = true;
					break;
				default:	// log & ignore the argument
					String errMessage = "Argument: \"" + mainArgs[i] + "\" was not expected!";
					System.err.println(errMessage);
					logger.error(errMessage);
					break;
			}
		}

		if ( FileUtils.shouldDownloadDocFiles ) {
			if ( !firstNumGiven ) {
				logger.warn("No \"-firstDocFileNum\" argument was given. The original-docFilesNames will be used.");
				FileUtils.shouldUseOriginalDocFileNames = true;
			}
		}
	}


	public static void showStatistics(Instant startTime)
	{
		long inputCheckedUrlNum = 0;
		long notConnectedIDs = 0;
		int currentlyLoadedUrls = FileUtils.getCurrentlyLoadedUrls();

		if ( LoaderAndChecker.useIdUrlPairs ) {
			logger.debug(LoaderAndChecker.numOfIDsWithoutAcceptableSourceUrl + " IDs (about " + df.format(LoaderAndChecker.numOfIDsWithoutAcceptableSourceUrl * 100.0 / LoaderAndChecker.numOfIDs) + "%) had no acceptable sourceUrl.");
			notConnectedIDs = LoaderAndChecker.numOfIDsWithoutAcceptableSourceUrl + FileUtils.duplicateIdUrlEntries;
			inputCheckedUrlNum = LoaderAndChecker.numOfIDs - notConnectedIDs;	// For each ID we usually check only one of its urls, except if the chosen one fails to connect. But if we add here the retries, then we should add how many more codUrls were retrieved per Id, later...
		} else {
			inputCheckedUrlNum = currentlyLoadedUrls;
			if ( (FileUtils.skipFirstRow && (inputCheckedUrlNum < 0)) || (!FileUtils.skipFirstRow && (inputCheckedUrlNum == 0)) ) {
				String errorMessage = "\"FileUtils.getCurrentlyLoadedUrls()\" is unexpectedly reporting that no urls were retrieved from input file! Output data may be affected! Exiting..";
				System.err.println(errorMessage);
				logger.error(errorMessage);
				FileUtils.closeIO();
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

		logger.info("Total docUrls found: " + UrlUtils.sumOfDocUrlsFound + ". That's about: " + df.format(UrlUtils.sumOfDocUrlsFound * 100.0 / inputCheckedUrlNum) + "% from the total numOfUrls checked. The rest were problematic or non-handleable url-cases.");
		if ( FileUtils.shouldDownloadDocFiles ) {
			int numOfStoredDocFiles = 0;
			if ( FileUtils.shouldUseOriginalDocFileNames )
				numOfStoredDocFiles = FileUtils.numOfDocFile;
			else
				numOfStoredDocFiles = FileUtils.numOfDocFile - initialNumOfDocFile;
			logger.info("From which docUrls, we were able to retrieve: " + numOfStoredDocFiles + " distinct docFiles. That's about: " + df.format(numOfStoredDocFiles * 100.0 / UrlUtils.sumOfDocUrlsFound) + "%."
					+ " The un-retrieved docFiles were either belonging to already-found docUrls or they had content-issues.");
		}
		logger.debug("The metaDocUrl-handler is responsible for the discovery of " + MetaDocUrlsHandler.numOfMetaDocUrlsFound + " of the docUrls (" + df.format(MetaDocUrlsHandler.numOfMetaDocUrlsFound* 100.0 / UrlUtils.sumOfDocUrlsFound) + "%).");
		logger.debug("The re-crossed docUrls (from all handlers) were " + LoaderAndChecker.reCrossedDocUrls + ". That's about " + df.format(LoaderAndChecker.reCrossedDocUrls * 100.0 / UrlUtils.sumOfDocUrlsFound) + "% of the total docUrls found.");
		if ( MachineLearning.useMLA )
			logger.debug("The M.L.A. is responsible for the discovery of " + MachineLearning.docUrlsFoundByMLA + " of the docUrls (" + df.format(MachineLearning.docUrlsFoundByMLA * 100.0 / UrlUtils.sumOfDocUrlsFound) + "%). The M.L.A.'s average success-rate was: " + df.format(MachineLearning.getAverageSuccessRate()) + "%. Gathered data for " + MachineLearning.timesGatheredData + " valid pageUrl-docUrl pairs.");
		else
			logger.debug("The M.L.A. was not enabled.");

		logger.debug("About " + df.format(LoaderAndChecker.connProblematicUrls * 100.0 / inputCheckedUrlNum) + "% (" + LoaderAndChecker.connProblematicUrls + " urls) were pages which had connectivity problems.");
		logger.debug("About " + df.format(UrlTypeChecker.pagesNotProvidingDocUrls * 100.0 / inputCheckedUrlNum) + "% (" + UrlTypeChecker.pagesNotProvidingDocUrls + " urls) were pages which did not provide docUrls.");
		logger.debug("About " + df.format(UrlTypeChecker.longToRespondUrls * 100.0 / inputCheckedUrlNum) + "% (" + UrlTypeChecker.longToRespondUrls + " urls) were urls which belong to domains which take too long to respond.");
		logger.debug("About " + df.format(PageCrawler.contentProblematicUrls * 100.0 / inputCheckedUrlNum) + "% (" + PageCrawler.contentProblematicUrls + " urls) were urls which had problematic content.");

		long problematicUrlsNum = LoaderAndChecker.connProblematicUrls + UrlTypeChecker.pagesNotProvidingDocUrls + UrlTypeChecker.longToRespondUrls + PageCrawler.contentProblematicUrls;

		if ( !LoaderAndChecker.useIdUrlPairs )
		{
			logger.debug("About " + df.format(UrlTypeChecker.crawlerSensitiveDomains * 100.0 / inputCheckedUrlNum) + "% (" + UrlTypeChecker.crawlerSensitiveDomains  + " urls) were from known crawler-sensitive domains.");
			logger.debug("About " + df.format(UrlTypeChecker.javascriptPageUrls * 100.0 / inputCheckedUrlNum) + "% (" + UrlTypeChecker.javascriptPageUrls + " urls) were from a JavaScript-powered domain, other than the \"sciencedirect.com\", which has dynamic links.");
			logger.debug("About " + df.format(UrlTypeChecker.elsevierUnwantedUrls * 100.0 / inputCheckedUrlNum) + "% (" + UrlTypeChecker.elsevierUnwantedUrls + " urls) were from, or reached after redirects, the unwanted domain: \"elsevier.com\", which either doesn't provide docUrls in its docPages, or it redirects to \"sciencedirect.com\", thus being avoided to be crawled.");
			logger.debug("About " + df.format(UrlTypeChecker.doajResultPageUrls * 100.0 / inputCheckedUrlNum) + "% (" + UrlTypeChecker.doajResultPageUrls + " urls) were \"doaj.org/toc/\" urls, which are resultPages, thus being avoided to be crawled.");
			logger.debug("About " + df.format(UrlTypeChecker.pagesWithHtmlDocUrls * 100.0 / inputCheckedUrlNum) + "% (" + UrlTypeChecker.pagesWithHtmlDocUrls + " urls) were docUrls, but, in HTML, thus being avoided to be crawled.");
			logger.debug("About " + df.format(UrlTypeChecker.pagesRequireLoginToAccessDocFiles * 100.0 / inputCheckedUrlNum) + "% (" + UrlTypeChecker.pagesRequireLoginToAccessDocFiles + " urls) were of domains which are known to require login to access docFiles, thus, they were blocked before being connected.");
			logger.debug("About " + df.format(UrlTypeChecker.pagesWithLargerCrawlingDepth * 100.0 / inputCheckedUrlNum) + "% (" + UrlTypeChecker.pagesWithLargerCrawlingDepth + " urls) were docPages which have their docUrl deeper inside their server, thus being currently avoided.");
			logger.debug("About " + df.format(UrlTypeChecker.pangaeaUrls * 100.0 / inputCheckedUrlNum) + "% (" + UrlTypeChecker.pangaeaUrls + " urls) were \"PANGAEA.\" with invalid form and non-docUrls in their internal links.");
			logger.debug("About " + df.format(UrlTypeChecker.urlsWithUnwantedForm * 100.0 / inputCheckedUrlNum) + "% (" + UrlTypeChecker.urlsWithUnwantedForm + " urls) were urls which are plain-domains, have unwanted url-extensions, ect...");
			logger.debug("About " + df.format(LoaderAndChecker.inputDuplicatesNum * 100.0 / inputCheckedUrlNum) + "% (" + LoaderAndChecker.inputDuplicatesNum + " urls) were duplicates in the input file.");

			problematicUrlsNum += UrlTypeChecker.crawlerSensitiveDomains + UrlTypeChecker.javascriptPageUrls + UrlTypeChecker.elsevierUnwantedUrls + UrlTypeChecker.doajResultPageUrls + UrlTypeChecker.pagesWithHtmlDocUrls + UrlTypeChecker.pagesRequireLoginToAccessDocFiles
					+ UrlTypeChecker.pagesWithLargerCrawlingDepth + UrlTypeChecker.pangaeaUrls + UrlTypeChecker.urlsWithUnwantedForm + LoaderAndChecker.inputDuplicatesNum;
		}

		logger.info("From the " + inputCheckedUrlNum + " urls checked from the input, the " + problematicUrlsNum + " of them (about " + df.format(problematicUrlsNum * 100.0 / inputCheckedUrlNum) + "%) were problematic (sum of all of the cases that appear in debug-mode).");
		logger.info("The rest " + (inputCheckedUrlNum + LoaderAndChecker.loadingRetries - UrlUtils.sumOfDocUrlsFound - problematicUrlsNum) + " urls were not docUrls.");

		logger.debug("The number of offline-redirects to HTTPS (reducing the online-redirection-overhead) was: " + HttpConnUtils.timesDidOfflineHTTPSredirect);
		logger.debug("The number of domains blocked due to an \"SSL Exception\" was: " + HttpConnUtils.numOfDomainsBlockedDueToSSLException);
		logger.debug("The number of domains blocked in total was: " + HttpConnUtils.blacklistedDomains.size());

		calculateAndPrintElapsedTime(startTime, Instant.now());


		if ( logger.isDebugEnabled() )
		{
			sortHashMapByValueAndPrint(UrlUtils.domainsAndHits, true);

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


	public static void sortHashMapByValueAndPrint(HashMap<String, Integer> map, boolean descendingOrder)
	{
		List<Map.Entry<String, Integer>> list = new LinkedList<>(map.entrySet());
		list.sort((o1, o2) -> {
			if ( descendingOrder )
				return o2.getValue().compareTo(o1.getValue());
			else
				return o1.getValue().compareTo(o2.getValue());
		});
		logger.debug("The " + list.size() + " domains which gave docUrls and their number:");
		for ( Map.Entry<String, Integer> entry : list )
			logger.debug(entry.getKey() + " : " + entry.getValue());
	}
	
}
