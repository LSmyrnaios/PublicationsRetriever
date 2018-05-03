package eu.openaire.doc_urls_retriever.crawler;

//import eu.openaire.doc_urls_retriever.util.http.HttpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import eu.openaire.doc_urls_retriever.util.file.FileUtils;
import eu.openaire.doc_urls_retriever.util.url.UrlUtils;


/**
 * @author Lampros A. Smyrnaios
 */
public class CrawlerController
{	
	private static final Logger logger = LoggerFactory.getLogger(CrawlerController.class);
	
	public static long urlsReachedCrawler = 0;	// Potentially useful for statistics.
	public static boolean useIdUrlPairs = true;
	
	
	public CrawlerController() throws RuntimeException
	{
		logger.info("Starting crawler..");
		
		try {
			if ( MachineLearning.useMLA )
				new MachineLearning();
			
			if ( CrawlerController.useIdUrlPairs )
				UrlUtils.loadAndCheckIdUrlPairs();
			else
				UrlUtils.loadAndCheckUrls();
			
			/*
			// Here test individual urls.
			String url = "";	// Give the url to test.
			HttpUtils.connectAndCheckMimeType(url, url, null, true, false);
			*/
			
	        // Write any remaining urls from memory to disk.
	        if ( FileUtils.tripleToBeLoggedOutputList.size() > 0 ) {
	        	logger.debug("Writing last set(s) of (\"SourceUrl\", \"DocUrl\"), to disk.");
	        	FileUtils.writeToFile();
	        }
	        
		} catch (Exception e) {
			logger.error("", e);
			throw new RuntimeException(e);
		}
	}
	
}
