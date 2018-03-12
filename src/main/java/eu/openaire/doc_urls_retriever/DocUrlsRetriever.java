package eu.openaire.doc_urls_retriever;

import eu.openaire.doc_urls_retriever.crawler.CrawlerController;
import eu.openaire.doc_urls_retriever.util.file.FileUtils;
import eu.openaire.doc_urls_retriever.util.url.UrlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Used for testing with non-standard input/output.
/*
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
*/


/**
 * This class contains the entry-point of this program. It calls methods to receive the url(s) (docPages) from an input file.
 * It then define from its HTTP-Header, if it's a download-ready file, or it needs further research with an html crawler.
 * After crawling is done, the results (docUrls) are written in the output file.
 * @author Lampros A. Smyrnaios
 */
public class DocUrlsRetriever
{
	private static final Logger logger = LoggerFactory.getLogger(DocUrlsRetriever.class);
	
    public static void main( String[] args )
    {
    	// Use testing input/output files.
		/*try {
			new FileUtils(new FileInputStream(new File(System.getProperty("user.dir") + "//src//main//resources//testUrlsFinalRandom5000+1.csv")),
							new FileOutputStream(new File(System.getProperty("user.dir") + "//src//main//resources//testOutputFile.json")));
		} catch (FileNotFoundException e) {
			logger.error("InputFile not found!", e);
			System.exit(-3);
		}*/
		
		// Use standard input/output.
		new FileUtils(System.in, System.out);
		
		try {
			new CrawlerController();
		} catch (RuntimeException e) {  // In case there was no input, or on Crawler4j's failure to be initialized, there will be thrown a RuntimeException, after logging the cause.
			logger.error("There was a serious error! Output data is affected! Exiting..");
			System.exit(-1);
		}
		
		// Show statistics.
		long inputUrlNum = 0;
    	if ( (inputUrlNum = FileUtils.getCurrentlyLoadedUrls()) <= 0 ) {
    		logger.error("\"FileUtils.getCurrentlyLoadedUrls()\" is unexpectedly reporting that no urls were retrieved from input file! Output data may be affected! Exiting..");
    		System.exit(-2);
    	}
		
		logger.info("Total urls number in the input was: " + inputUrlNum);
    	logger.info("From which, " + CrawlerController.urlsReachedCrawler + " reached the crawling stage (others were discarded at loading).");
		logger.info("Total docs found: " + UrlUtils.sumOfDocsFound + " That's about: " + UrlUtils.sumOfDocsFound * (float)100 / inputUrlNum + "%");
		logger.info("About: " + UrlUtils.sciencedirectUrls * (float)100 / inputUrlNum + "% (" + UrlUtils.sciencedirectUrls + " urls) were from the JavaScript-using domain \"sciencedirect.com\", which has dynamic links.");
		logger.info("About: " + UrlUtils.doiOrgToScienceDirect * (float)100 / inputUrlNum + "% (" + UrlUtils.doiOrgToScienceDirect + " urls) were of a certain type of \"doi.org\" urls which redirect to \"sciencedirect.com\", thus being avoided to be crawled.");
		logger.info("Αbout: " + UrlUtils.elsevierUnwantedUrls * (float)100 / inputUrlNum + "% (" + UrlUtils.elsevierUnwantedUrls + " urls) were from, or reached after redirects, the unwanted domain: \"elsevier.com\", which either doesn't provide docUrls in its docPages, or it redirects to \"sciencedirect.com\", thus being avoided to be crawled.");
    	logger.info("Αbout: " + UrlUtils.doajResultPageUrls * (float)100 / inputUrlNum + "% (" + UrlUtils.doajResultPageUrls + " urls) were \"doaj.org/toc/\" urls, which are resultPages, thus being avoided to be crawled.");
		logger.info("Αbout: " + UrlUtils.dlibHtmlDocUrls * (float)100 / inputUrlNum + "% (" + UrlUtils.dlibHtmlDocUrls + " urls) were \"dlib.org\" urls, which are docUrls, but, in HTML, thus being avoided to be crawled.");
		logger.info("About: " + UrlUtils.pagesWithLargerCrawlingDepth * (float)100 / inputUrlNum + "% (" + UrlUtils.pagesWithLargerCrawlingDepth + " urls) were docPages which have their docUrl deeper inside the server, thus being currently avoided.");
		logger.info("About: " + UrlUtils.urlsWithUnwantedForm * (float)100 / inputUrlNum + "% (" + UrlUtils.urlsWithUnwantedForm + " urls) were urls which are plain-domains, have unwanted url-extensions, ect...");
		logger.info("About: " + UrlUtils.inputDuplicatesNum * (float)100 / inputUrlNum + "% (" + UrlUtils.inputDuplicatesNum + " urls) were duplicates in the input file.");
		
		long problematicUrlsNum = UrlUtils.sciencedirectUrls + UrlUtils.doiOrgToScienceDirect + UrlUtils.elsevierUnwantedUrls + UrlUtils.doajResultPageUrls + UrlUtils.dlibHtmlDocUrls + UrlUtils.pagesWithLargerCrawlingDepth + UrlUtils.urlsWithUnwantedForm + UrlUtils.inputDuplicatesNum;
		logger.info("From the " + inputUrlNum + " urls in the input, the " + problematicUrlsNum + " of them (about " + problematicUrlsNum * (float)100 / inputUrlNum + "%) were problematic (sum of the all of the above cases).");
		
        // Then... just close the open streams (imported and exported content) and exit.
        FileUtils.closeStreams();
    }

}
