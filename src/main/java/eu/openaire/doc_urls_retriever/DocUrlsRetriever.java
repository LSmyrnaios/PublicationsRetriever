package eu.openaire.doc_urls_retriever;

import eu.openaire.doc_urls_retriever.crawler.CrawlerController;
import eu.openaire.doc_urls_retriever.util.file.FileUtils;
import eu.openaire.doc_urls_retriever.util.url.UrlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * This class has the responsibility to receive the url(s) from an input file and define from it's header, if it's file-ready-download
 * or, it needs further research with html crawler.
 */
public class DocUrlsRetriever
{
	private static final Logger logger = LoggerFactory.getLogger(DocUrlsRetriever.class);
	
    public static void main( String[] args )
    {
    	// Use test input/output.
		/*try {
			new FileUtils(new FileInputStream(new File(System.getProperty("user.dir") + "//src//main//resources//testUrlsJson6.json")), new FileOutputStream(new File(System.getProperty("user.dir") + "//src//main//resources//testOutputFile.json")));
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
		logger.info("Total docs found: " + UrlUtils.sumOfDocsFound + " That's about: " + UrlUtils.sumOfDocsFound * (float)100 / inputUrlNum + "%");
		logger.info("Αbout: " + UrlUtils.elsevierLinks * (float)100 / inputUrlNum + "% (" + UrlUtils.elsevierLinks + " urls) were redirected to the JavaScript site \"elsevier.com\" and were avoided to be crawled.");
    	logger.info("Αbout: " + UrlUtils.doajResultPageLinks * (float)100 / inputUrlNum + "% (" + UrlUtils.doajResultPageLinks + " urls) were \"doaj.org/toc/\" urls, which are resultPages, thus being avoided to be crawled.");
		logger.info("Αbout: " + UrlUtils.dlibHtmlDocUrls * (float)100 / inputUrlNum + "% (" + UrlUtils.dlibHtmlDocUrls + " urls) were \"dlib.org\" urls, which are docUrls, but, in HTML, thus being avoided to be crawled.");
		logger.info("There were: " + UrlUtils.inputDuplicatesNum + " duplicates in the input file." + " That's about: " + UrlUtils.inputDuplicatesNum * (float)100 / inputUrlNum + "%");
		
        // Then... just close the open streams (imported and exported content) and exit.
        FileUtils.closeStreams();
    }

}
