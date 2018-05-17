package eu.openaire.doc_urls_retriever.crawler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import eu.openaire.doc_urls_retriever.util.http.HttpUtils;
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
			if ( CrawlerController.useIdUrlPairs )
				UrlUtils.loadAndCheckIdUrlPairs();
			else
				UrlUtils.loadAndCheckUrls();
			
			// Here test individual urls.
			//String url = "http://repositorio.ipen.br:8080/xmlui/bitstream/handle/123456789/11176/09808.pdf?sequence=1&isAllowed=y";
			//String url = "https://ris.utwente.nl/ws/portalfiles/portal/5118887";
			//String url = "http://biblioteca.ucm.es/tesis/19972000/X/0/X0040301.pdf";
			//String url = "http://vddb.library.lt/fedora/get/LT-eLABa-0001:E.02~2008~D_20080618_115819-91936/DS.005.0.02.ETD";
			/*String url = "http://dx.doi.org/10.1111/jora.12209";
			HttpUtils.connectAndCheckMimeType(null, url, url, url, null, true, false);*/
			
	        // Write any remaining urls from memory to disk.
	        if ( FileUtils.quadrupleToBeLoggedOutputList.size() > 0 ) {
	        	logger.debug("Writing last set(s) of (\"SourceUrl\", \"DocUrl\"), to disk.");
	        	FileUtils.writeToFile();
	        }
	        
		} catch (Exception e) {
			logger.error("", e);
			throw new RuntimeException(e);
		}
	}
	
}
