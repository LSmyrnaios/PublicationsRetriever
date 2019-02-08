package eu.openaire.doc_urls_retriever;

import eu.openaire.doc_urls_retriever.util.file.FileUtils;
import eu.openaire.doc_urls_retriever.util.http.HttpConnUtils;
import eu.openaire.doc_urls_retriever.util.url.LoaderAndChecker;
import eu.openaire.doc_urls_retriever.util.url.UrlTypeChecker;
import eu.openaire.doc_urls_retriever.util.url.UrlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;


/**
 * This class contains the entry-point of this program, the "main()" method.
 * The "main()" method calls other methods to set the input/output streams and retrieve the docUrls for each docPage in the inputFile.
 * In the end, the outputFile consists of docPages along with their docUrls.
 * @author Lampros A. Smyrnaios
 */
public class DocUrlsRetriever
{
	private static final Logger logger = LoggerFactory.getLogger(DocUrlsRetriever.class);
	
	private static int initialNumOfDocFile = 0;
	
	public static boolean docFilesStorageGivenByUser = false;
	
	
    public static void main( String[] args )
    {
		parseArgs(args);
		
		// Use standard input/output.
		new FileUtils(System.in, System.out);
	
		Instant startTime = Instant.now();
	
		try {
			new LoaderAndChecker();
		} catch (RuntimeException e) {  // In case there was no input, a RuntimeException will be thrown, after logging the cause.
			String errorMessage = "There was a serious error! Output data is affected! Exiting..";
			System.err.println(errorMessage);
			logger.error(errorMessage);
			System.exit(-5);
		}
		
		Instant finishTime = Instant.now();
		
		showStatistics(startTime, finishTime);
		
		// Close the open streams (imported and exported content).
		FileUtils.closeStreams();
    }
    
	
	public static void parseArgs(String[] mainArgs)
	{
		if ( mainArgs.length > 5 ) {
			String errMessage = "\"DocUrlsRetriever\" expected only up to 5 arguments, while you gave: " + mainArgs.length + "!";
			logger.error(errMessage);
			System.err.println(errMessage
					+ "\nUsage: java -jar doc_urls_retriever-<VERSION>.jar -downloadDocFiles(OPTIONAL) -firstDocFileNum(OPTIONAL) 'num' -docFilesStorage(OPTIONAL) 'storageDir' < 'input' > 'output'");
			System.exit(-7);
		}
		
		boolean downloadDocFiles = false;
		boolean firstNumGiven = false;
		
		for ( short i=0; i < mainArgs.length; i++ )
		{
			switch ( mainArgs[i] )
			{
				case "-downloadDocFiles":
					downloadDocFiles = true;
					FileUtils.shouldDownloadDocFiles = downloadDocFiles;
					break;
				case "-firstDocFileNum":
					try {
						i ++;
						DocUrlsRetriever.initialNumOfDocFile = Integer.parseInt(mainArgs[i]);
						FileUtils.numOfDocFile = DocUrlsRetriever.initialNumOfDocFile;
						firstNumGiven = true;
						break;
					} catch (NumberFormatException nfe) {
						String errorMessage = "Argument \"-firstDocFileNum\" must be followed by an integer value! Given one was: \"" + mainArgs[i] + "\"";
						System.err.println(errorMessage);
						logger.error(errorMessage);
						System.exit(-3);
					}
				case "-docFilesStorage":
					i ++;
					FileUtils.storeDocFilesDir = mainArgs[i];
					DocUrlsRetriever.docFilesStorageGivenByUser = true;
					break;
				default:	// log & ignore the argument
					String errMessage = "Argument: \"" + mainArgs[i] + "\" was not expected!";
					System.err.println(errMessage);
					logger.error(errMessage);
					break;
			}
		}
		
		if ( downloadDocFiles ) {
			if ( !firstNumGiven ) {
				logger.warn("No \"-firstDocFileNum\" argument was given. The original-docFilesNames will be used.");
				FileUtils.shouldUseOriginalDocFileNames = true;
			}
		}
		else
			FileUtils.shouldDownloadDocFiles = false;
	}
	
	
	public static void showStatistics(Instant startTime, Instant finishTime)
	{
		long inputCheckedUrlNum = 0;
		if ( LoaderAndChecker.useIdUrlPairs )
			inputCheckedUrlNum = LoaderAndChecker.numOfIDs;	// For each ID we check only one of its urls anyway.
		else {
			inputCheckedUrlNum = FileUtils.getCurrentlyLoadedUrls();
			if ( (FileUtils.skipFirstRow && (inputCheckedUrlNum < 0)) || (!FileUtils.skipFirstRow && (inputCheckedUrlNum == 0)) ) {
				String errorMessage = "\"FileUtils.getCurrentlyLoadedUrls()\" is unexpectedly reporting that no urls were retrieved from input file! Output data may be affected! Exiting..";
				System.err.println(errorMessage);
				logger.error(errorMessage);
				System.exit(-6);
			}
		}
		
		logger.info("Total num of urls checked from the input was: " + inputCheckedUrlNum);
		logger.info("Total docUrls found: " + UrlUtils.sumOfDocUrlsFound + ". That's about: " + UrlUtils.sumOfDocUrlsFound * (float)100 / inputCheckedUrlNum + "%");
		if ( FileUtils.shouldDownloadDocFiles ) {
			int numOfStoredDocFiles = 0;
			if ( FileUtils.shouldUseOriginalDocFileNames )
				numOfStoredDocFiles = FileUtils.numOfDocFile;
			else
				numOfStoredDocFiles = (FileUtils.numOfDocFile -1) - (initialNumOfDocFile -1);
			logger.info("From which docUrls, we were able to retrieve: " + numOfStoredDocFiles + " distinct docFiles. That's about: " + numOfStoredDocFiles * (float)100 / UrlUtils.sumOfDocUrlsFound +"%."
					+" The un-retrieved docFiles were either belonging to already-found docUrls or they had content-issues.");
		}
		logger.info("About: " + UrlTypeChecker.crawlerSensitiveDomains * (float)100 / inputCheckedUrlNum + "% (" + UrlTypeChecker.crawlerSensitiveDomains  + " urls) were from known crawler-sensitive domains.");
		logger.info("About: " + UrlTypeChecker.javascriptPageUrls * (float)100 / inputCheckedUrlNum + "% (" + UrlTypeChecker.javascriptPageUrls + " urls) were from a JavaScript-powered domain, other than the \"sciencedirect.com\", which has dynamic links.");
		//logger.info("About: " + UrlTypeChecker.sciencedirectUrls * (float)100 / inputCheckedUrlNum + "% (" + UrlTypeChecker.sciencedirectUrls + " urls) were from the \"sciencedirect.com\"-family urls, with dynamic links.");
		logger.info("Αbout: " + UrlTypeChecker.elsevierUnwantedUrls * (float)100 / inputCheckedUrlNum + "% (" + UrlTypeChecker.elsevierUnwantedUrls + " urls) were from, or reached after redirects, the unwanted domain: \"elsevier.com\", which either doesn't provide docUrls in its docPages, or it redirects to \"sciencedirect.com\", thus being avoided to be crawled.");
		logger.info("Αbout: " + UrlTypeChecker.doajResultPageUrls * (float)100 / inputCheckedUrlNum + "% (" + UrlTypeChecker.doajResultPageUrls + " urls) were \"doaj.org/toc/\" urls, which are resultPages, thus being avoided to be crawled.");
		logger.info("Αbout: " + UrlTypeChecker.pagesWithHtmlDocUrls * (float)100 / inputCheckedUrlNum + "% (" + UrlTypeChecker.pagesWithHtmlDocUrls + " urls) were docUrls, but, in HTML, thus being avoided to be crawled.");
		logger.info("About: " + UrlTypeChecker.pagesRequireLoginToAccessDocFiles * (float)100 / inputCheckedUrlNum + "% (" + UrlTypeChecker.pagesRequireLoginToAccessDocFiles + " urls) were of domains which are known to require login to access docFiles, thus, they were blocked before being connected.");
		logger.info("About: " + UrlTypeChecker.pagesWithLargerCrawlingDepth * (float)100 / inputCheckedUrlNum + "% (" + UrlTypeChecker.pagesWithLargerCrawlingDepth + " urls) were docPages which have their docUrl deeper inside their server, thus being currently avoided.");
		logger.info("About: " + UrlTypeChecker.pangaeaUrls * (float)100 / inputCheckedUrlNum + "% (" + UrlTypeChecker.pangaeaUrls + " urls) were \"PANGAEA.\" with invalid form and non-docUrls in their internal links.");
		logger.info("About: " + LoaderAndChecker.connProblematicUrls * (float)100 / inputCheckedUrlNum + "% (" + LoaderAndChecker.connProblematicUrls + " urls) were pages which had connectivity problems.");
		logger.info("About: " + UrlTypeChecker.pagesNotProvidingDocUrls * (float)100 / inputCheckedUrlNum + "% (" + UrlTypeChecker.pagesNotProvidingDocUrls + " urls) were pages which are known to not provide docUrls.");
		logger.info("About: " + UrlTypeChecker.longToRespondUrls * (float)100 / inputCheckedUrlNum + "% (" + UrlTypeChecker.longToRespondUrls + " urls) were urls which belong to domains which take too long to respond.");
		logger.info("About: " + UrlTypeChecker.urlsWithUnwantedForm * (float)100 / inputCheckedUrlNum + "% (" + UrlTypeChecker.urlsWithUnwantedForm + " urls) were urls which are plain-domains, have unwanted url-extensions, ect...");
		logger.info("About: " + LoaderAndChecker.inputDuplicatesNum * (float)100 / inputCheckedUrlNum + "% (" + LoaderAndChecker.inputDuplicatesNum + " urls) were duplicates in the input file.");
		
		long problematicUrlsNum = UrlTypeChecker.crawlerSensitiveDomains + UrlTypeChecker.javascriptPageUrls /*+ UrlTypeChecker.sciencedirectUrls*/ + UrlTypeChecker.elsevierUnwantedUrls + UrlTypeChecker.doajResultPageUrls + UrlTypeChecker.pagesWithHtmlDocUrls + UrlTypeChecker.pagesRequireLoginToAccessDocFiles
				+ UrlTypeChecker.pagesWithLargerCrawlingDepth + UrlTypeChecker.pangaeaUrls + UrlTypeChecker.urlsWithUnwantedForm + LoaderAndChecker.connProblematicUrls + UrlTypeChecker.pagesNotProvidingDocUrls + UrlTypeChecker.longToRespondUrls + LoaderAndChecker.inputDuplicatesNum;
		logger.info("From the " + inputCheckedUrlNum + " urls checked from the input, the " + problematicUrlsNum + " of them (about " + problematicUrlsNum * (float)100 / inputCheckedUrlNum + "%) were problematic (sum of the all of the above cases).");
		
		logger.info("The number of domains blocked due to an \"SSL Exception\" was: " + HttpConnUtils.domainsBlockedDueToSSLException);
		
		calculateAndPrintElapsedTime(startTime, finishTime);
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
	
}
