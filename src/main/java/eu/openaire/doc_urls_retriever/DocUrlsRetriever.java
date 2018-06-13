package eu.openaire.doc_urls_retriever;

import eu.openaire.doc_urls_retriever.crawler.CrawlerController;
import eu.openaire.doc_urls_retriever.util.file.FileUtils;
import eu.openaire.doc_urls_retriever.util.url.UrlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Used for testing with non-standard input/output.
/*import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;*/


/**
 * This class contains the entry-point of this program. It calls methods to receive the url(s) (docPages) from an input file.
 * It then define from its HTTP-Header, if it's a download-ready file, or it needs further research with an html crawler.
 * After crawling is done, the results (docUrls) are written in the output file.
 * @author Lampros A. Smyrnaios
 */
public class DocUrlsRetriever
{
	private static final Logger logger = LoggerFactory.getLogger(DocUrlsRetriever.class);
	private static int initialNumOfDocFile = 0;
	
    public static void main( String[] args )
    {
		// If we will download docFile and use custom-fileNames.. retrieve the starting docFileName for this program's instance.
		if ( FileUtils.shouldDownloadDocFiles && !FileUtils.shouldUseOriginalDocFileNames ) {
			if ( args.length > 1 ) {
				String errorMessage = "You have to give only one argument, to be used as the starting fileName for the docFileNames!";
				System.err.println(errorMessage);
				logger.error(errorMessage);
				System.exit(-1);
			} else if ( args.length == 0 ) {
				initialNumOfDocFile = 1;
				FileUtils.numOfDocFile = initialNumOfDocFile;
			} else {
				try {
					if ( (initialNumOfDocFile = Integer.parseInt(args[0])) <= 0 ) {
						String errorMessage = "The starting-numOfDocFile must be above zero! Given one was: " + FileUtils.numOfDocFile;
						System.err.println(errorMessage);
						logger.error(errorMessage);
						System.exit(-2);
					} else
						FileUtils.numOfDocFile = initialNumOfDocFile;
				} catch (NumberFormatException nfe) {
					String errorMessage = "Argument" + args[0] + " must be an integer!";
					System.err.println(errorMessage);
					logger.error(errorMessage);
					System.exit(-3);
				}
			}
		}
    	
    	// Use testing input/output files.
		/*try {
			new FileUtils(new FileInputStream(new File(System.getProperty("user.dir") + "//src//main//resources//sampleCleanUrls3000.json")),
							new FileOutputStream(new File(System.getProperty("user.dir") + "//src//main//resources//testOutputFile.json")));
		} catch (FileNotFoundException e) {
			String errorMessage = "InputFile not found!";
			System.err.println(errorMessage);
			logger.error(errorMessage, e);
			System.exit(-4);
		}*/
		
		// Use standard input/output.
		new FileUtils(System.in, System.out);
		
		try {
			new CrawlerController();
		} catch (RuntimeException e) {  // In case there was no input, or on Crawler4j's failure to be initialized, there will be thrown a RuntimeException, after logging the cause.
			String errorMessage = "There was a serious error! Output data is affected! Exiting..";
			System.err.println(errorMessage);
			logger.error(errorMessage);
			System.exit(-5);
		}
		
		// Show statistics.
		long inputCheckedUrlNum = 0;
		if ( CrawlerController.useIdUrlPairs )
			inputCheckedUrlNum = UrlUtils.numOfIDs;	// For each ID we check only one of its urls anyway.
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
		logger.info("From which, " + CrawlerController.urlsReachedCrawler + " reached the crawling stage (others were either detected as docUrls or where discarded at loading after matching to unwanted types).");
		logger.info("Total docUrls found: " + UrlUtils.sumOfDocUrlsFound + ". That's about: " + UrlUtils.sumOfDocUrlsFound * (float)100 / inputCheckedUrlNum + "%");
		if ( FileUtils.shouldDownloadDocFiles ) {
			int numOfStoredDocFiles = 0;
			if ( FileUtils.shouldUseOriginalDocFileNames )
				numOfStoredDocFiles = FileUtils.numOfDocFile - (initialNumOfDocFile -1);
			else
				numOfStoredDocFiles = (FileUtils.numOfDocFile -1) - (initialNumOfDocFile -1);
			logger.info("From which docUrls, we were able to retrieve: " + numOfStoredDocFiles + " distinct docFiles. That's about: " + numOfStoredDocFiles * (float)100 / UrlUtils.sumOfDocUrlsFound +"%."
					+" The un-retrieved docFiles were either belonging to already-found docUrls or they had content-issues.");
		}
		logger.info("About: " + UrlUtils.crawlerSensitiveDomains * (float)100 / inputCheckedUrlNum + "% (" + UrlUtils.crawlerSensitiveDomains  + " urls) were from known crawler-sensitive domains.");
		logger.info("About: " + UrlUtils.javascriptPageUrls * (float)100 / inputCheckedUrlNum + "% (" + UrlUtils.javascriptPageUrls + " urls) were from a JavaScript-powered domain, other than the \"sciencedirect.com\", which has dynamic links.");
		logger.info("About: " + UrlUtils.sciencedirectUrls * (float)100 / inputCheckedUrlNum + "% (" + UrlUtils.sciencedirectUrls + " urls) were from the JavaScript-powered domain \"sciencedirect.com\", which has dynamic links.");
		logger.info("About: " + UrlUtils.doiOrgToScienceDirect * (float)100 / inputCheckedUrlNum + "% (" + UrlUtils.doiOrgToScienceDirect + " urls) were of a certain type of \"doi.org\" urls which would redirect to \"sciencedirect.com\", thus being avoided to be crawled.");
		logger.info("Αbout: " + UrlUtils.elsevierUnwantedUrls * (float)100 / inputCheckedUrlNum + "% (" + UrlUtils.elsevierUnwantedUrls + " urls) were from, or reached after redirects, the unwanted domain: \"elsevier.com\", which either doesn't provide docUrls in its docPages, or it redirects to \"sciencedirect.com\", thus being avoided to be crawled.");
		logger.info("Αbout: " + UrlUtils.doajResultPageUrls * (float)100 / inputCheckedUrlNum + "% (" + UrlUtils.doajResultPageUrls + " urls) were \"doaj.org/toc/\" urls, which are resultPages, thus being avoided to be crawled.");
		logger.info("Αbout: " + UrlUtils.pagesWithHtmlDocUrls * (float)100 / inputCheckedUrlNum + "% (" + UrlUtils.pagesWithHtmlDocUrls + " urls) were docUrls, but, in HTML, thus being avoided to be crawled.");
		logger.info("About: " + UrlUtils.pagesRequireLoginToAccessDocFiles * (float)100 / inputCheckedUrlNum + "% (" + UrlUtils.pagesRequireLoginToAccessDocFiles + " urls) were of domains which are known to require login to access docFiles, thus, they were blocked before being connected.");
		logger.info("About: " + UrlUtils.pagesWithLargerCrawlingDepth * (float)100 / inputCheckedUrlNum + "% (" + UrlUtils.pagesWithLargerCrawlingDepth + " urls) were docPages which have their docUrl deeper inside their server, thus being currently avoided.");
		logger.info("About: " + UrlUtils.pangaeaUrls * (float)100 / inputCheckedUrlNum + "% (" + UrlUtils.pangaeaUrls + " urls) were \"PANGAEA.\" with invalid form and non-docUrls in their inner links.");
		logger.info("About: " + UrlUtils.connProblematicUrls * (float)100 / inputCheckedUrlNum + "% (" + UrlUtils.connProblematicUrls + " urls) were pages which had connectivity problems.");
		logger.info("About: " + UrlUtils.pagesNotProvidingDocUrls * (float)100 / inputCheckedUrlNum + "% (" + UrlUtils.pagesNotProvidingDocUrls + " urls) were pages which are known to not provide docUrls.");
		logger.info("About: " + UrlUtils.longToRespondUrls * (float)100 / inputCheckedUrlNum + "% (" + UrlUtils.longToRespondUrls + " urls) were urls which belong to domains which take too long to respond.");
		logger.info("About: " + UrlUtils.urlsWithUnwantedForm * (float)100 / inputCheckedUrlNum + "% (" + UrlUtils.urlsWithUnwantedForm + " urls) were urls which are plain-domains, have unwanted url-extensions, ect...");
		logger.info("About: " + UrlUtils.inputDuplicatesNum * (float)100 / inputCheckedUrlNum + "% (" + UrlUtils.inputDuplicatesNum + " urls) were duplicates in the input file.");
		
		long problematicUrlsNum = UrlUtils.crawlerSensitiveDomains + UrlUtils.javascriptPageUrls + UrlUtils.sciencedirectUrls + UrlUtils.doiOrgToScienceDirect + UrlUtils.elsevierUnwantedUrls + UrlUtils.doajResultPageUrls + UrlUtils.pagesWithHtmlDocUrls + UrlUtils.pagesRequireLoginToAccessDocFiles
									+ UrlUtils.pagesWithLargerCrawlingDepth + UrlUtils.pangaeaUrls + UrlUtils.urlsWithUnwantedForm + UrlUtils.connProblematicUrls + UrlUtils.pagesNotProvidingDocUrls + UrlUtils.longToRespondUrls + UrlUtils.inputDuplicatesNum;
		logger.info("From the " + inputCheckedUrlNum + " urls checked from the input, the " + problematicUrlsNum + " of them (about " + problematicUrlsNum * (float)100 / inputCheckedUrlNum + "%) were problematic (sum of the all of the above cases).");
		
        // Then... just close the open streams (imported and exported content) and exit.
        FileUtils.closeStreams();
    }
	
}
