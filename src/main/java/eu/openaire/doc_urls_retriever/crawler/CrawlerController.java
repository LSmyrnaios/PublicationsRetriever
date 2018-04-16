package eu.openaire.doc_urls_retriever.crawler;

import edu.uci.ics.crawler4j.frontier.Frontier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;
import eu.openaire.doc_urls_retriever.util.file.FileUtils;
import eu.openaire.doc_urls_retriever.util.http.HttpUtils;
import eu.openaire.doc_urls_retriever.util.url.UrlUtils;


/**
 * @author Lampros A. Smyrnaios
 */
public class CrawlerController
{	
	private static final Logger logger = LoggerFactory.getLogger(CrawlerController.class);
	
	public static CrawlController controller;
	private CrawlConfig config = new CrawlConfig();
	
	private String crawlStorageFolder = System.getProperty("user.dir") + "//src//main//resources//crawlerStorage";	// Change slashes later for linux..
	
	//public static DocIDServer docIdServer = null;	// Potentially useful when performing checks in urls added in the Crawler.
	public static Frontier frontier = null;	// Potentially useful to know the number of pages (left to be crawled, are in memory waiting, already prosseced).
	public static long urlsReachedCrawler = 0;	// Potentially useful for statistics.
	public static boolean useIdUrlPairs = true;
	
	
	/**
	 * Configures and runs the Crawler.
	 */
	public CrawlerController() throws RuntimeException
	{
		// Configure crawler with some non-default values:
		config.setMaxDepthOfCrawling(0);	// Crawl with a maximum depth of 0. Meaning that no inner link is allowed to be crawled.
		config.setResumableCrawling(false);	// False for testing.. Later it should be set to true.. to handle crawling with crashes, better.
		config.setCrawlStorageFolder(crawlStorageFolder);
		config.setOnlineTldListUpdate(true);
		config.setConnectionTimeout(HttpUtils.maxConnWaitingTime);
		config.setSocketTimeout(HttpUtils.maxConnWaitingTime);
		config.setMaxDownloadSize(HttpUtils.maxDownloadableContentSize);	// Default setting: 1048576 --> 10mb
		config.setPolitenessDelay(HttpUtils.politenessDelay);
		config.setIncludeBinaryContentInCrawling(true);	// Call "visit()" method even on binary content (which is not prohibited by "shouldVisit()" method) to check its contentType.
		config.setProcessBinaryContentInCrawling(true);	// Process more pages.. like xhtml ones (Crawler4j processes only html-ones unless instructed to process all).
		// Binary-content-rules in "shouldVisit()" still apply.
		
		int threadDelaySeconds = 5;	// This is used for Crawler4j to shutdown faster. Default is 10.
		config.setThreadMonitoringDelaySeconds(threadDelaySeconds);
		config.setCleanupDelaySeconds(threadDelaySeconds);
		config.setThreadShutdownDelaySeconds(threadDelaySeconds);
		
		PageFetcher pageFetcher = new PageFetcher(config);
		RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
		RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
		
		robotstxtConfig.setEnabled(false);	// Don't follow websites' instructions for robots (most of them will say no to crawlers except for GoogleBot). (Default value is: true)
		
		try {
			controller = new CrawlController(config, pageFetcher, robotstxtServer);
			
			//CrawlerController.docIdServer = controller.getDocIdServer();	// Enable this code if we need special urls' check from the crawler.
			CrawlerController.frontier = controller.getFrontier();	// Enable this code if we need to check pages' number in the crawler.
			
			if ( CrawlerController.useIdUrlPairs )
				UrlUtils.loadAndCheckIdUrlPairs();
			else
				UrlUtils.loadAndCheckUrls();
			
			CrawlerController.urlsReachedCrawler = CrawlerController.frontier.getNumberOfScheduledPages();	// If wanted for statistics, in the end.
			logger.debug("Urls added in Crawler: " + CrawlerController.urlsReachedCrawler);
			
			if ( MachineLearning.useMLA )
				new MachineLearning();
			
			// Start crawling and wait until finished.
			controller.start(PageCrawler.class, 1);
			
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
