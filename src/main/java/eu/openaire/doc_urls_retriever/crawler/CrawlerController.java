package eu.openaire.doc_urls_retriever.crawler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;
import eu.openaire.doc_urls_retriever.util.file.FileUtils;
import eu.openaire.doc_urls_retriever.util.http.HttpUtils;
import eu.openaire.doc_urls_retriever.util.url.UrlUtils;



public class CrawlerController
{	
	private static final Logger logger = LogManager.getLogger(CrawlerController.class);
	
	public static CrawlController controller;
	private CrawlConfig config = new CrawlConfig();
	
	private String crawlStorageFolder = System.getProperty("user.dir") + "//src//main//resources//crawlerStorage";	// Change slashes later for linux..
	
	//public static DocIDServer docIdServer = null;	// Potentially usable when performing checks in urls added in the Crawler.
	public static long urlsReachedCrawler = 0;
	
	
	
	/**
	 * Configures and runs the Crawler.
	 */
	public CrawlerController() throws RuntimeException
	{
		// Configure crawler with some non-default values:
		config.setMaxDepthOfCrawling(0);	// Crawl with a maximum depth of 0. Meaning that no inner link is allowed to be crawled.
		config.setResumableCrawling(false);	// False for testing.. Later it should be set to true.. to handle crawling with crashes, better.
		config.setCrawlStorageFolder(crawlStorageFolder);
		config.setOnlineTldListUpdate(true);	// Currently this doesn't work (follow issue #282 on Github: https://github.com/yasserg/crawler4j/issues/282).
		config.setConnectionTimeout(HttpUtils.maxConnWaitingTime);
		
		config.setMaxDownloadSize(52428800);	// Max = 50MB (that's larger from the default setting: 1048576)
												// We don't want to miss any page!
		
		config.setPolitenessDelay(HttpUtils.politenessDelay);
		
		PageFetcher pageFetcher = new PageFetcher(config);
		RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
		RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
		
		robotstxtConfig.setEnabled(false);	// Don't follow websites' instructions for robots (most of them will say no to crawlers except for GoogleBot). (Default value is: true)
		
		try {
			controller = new CrawlController(config, pageFetcher, robotstxtServer);
			
			//docIdServer = controller.getDocIdServer();
			
			UrlUtils.loadAndCheckUrls();
	        
	        // After loading, there were some urls that did not matched a docUrl, we can still try to retrieve docUrl by crawling the non-docUrls.
	        urlsReachedCrawler = controller.getFrontier().getQueueLength();	// For statistics.
	        //logger.debug("The number of urls that reached the crawling stage is: " + urlsReachedCrawler);	// DEBUG!
        	
	        // Start crawling and wait until finished.
	        controller.start(PageCrawler.class, 1);
	        
	        
	        // Write any remaining urls from memory to disk.
	        if ( FileUtils.outputEntries.size() > 0 ) {
	        	logger.debug("Writing last set(s) to disk");
	        	FileUtils.writeToFile();
	        }
	        
		} catch (Exception e) {
			logger.error(e);
			throw new RuntimeException(e);
		}
	}
	
}
