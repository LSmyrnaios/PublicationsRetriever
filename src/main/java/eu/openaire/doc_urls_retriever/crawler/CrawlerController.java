package eu.openaire.doc_urls_retriever.crawler;

import eu.openaire.doc_urls_retriever.util.url.LoadAndCheckUrls;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import eu.openaire.doc_urls_retriever.util.file.FileUtils;


/**
 * @author Lampros A. Smyrnaios
 */
public class CrawlerController
{	
	private static final Logger logger = LoggerFactory.getLogger(CrawlerController.class);
	
	public static boolean useIdUrlPairs = true;
	
	
	public CrawlerController() throws RuntimeException
	{
		logger.info("Starting crawler..");
		
		try {
			if ( CrawlerController.useIdUrlPairs )
				LoadAndCheckUrls.loadAndCheckIdUrlPairs();
			else
				LoadAndCheckUrls.loadAndCheckUrls();
			
		} catch (Exception e) {
			logger.error("", e);
			throw new RuntimeException(e);
		}
		finally {
			// Write any remaining urls from memory to disk.
			if ( !FileUtils.quadrupleToBeLoggedOutputList.isEmpty() ) {
				logger.debug("Writing last set(s) of (\"SourceUrl\", \"DocUrl\"), to disk.");
				FileUtils.writeToFile();
			}
		}
	}
	
}
