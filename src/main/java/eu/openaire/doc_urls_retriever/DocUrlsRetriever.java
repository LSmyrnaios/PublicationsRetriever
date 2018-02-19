package eu.openaire.doc_urls_retriever;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import eu.openaire.doc_urls_retriever.crawler.CrawlerController;
import eu.openaire.doc_urls_retriever.util.file.FileUtils;
import eu.openaire.doc_urls_retriever.util.url.UrlUtils;


/**
 * This class had the responsibility to receive the url(s) and define from it's header, if it's file-ready-download
 * or, it needs further research with html crawler and so on..
 */
@SuppressWarnings("ALL")
public class DocUrlsRetriever
{
	private static final Logger logger = LogManager.getLogger(DocUrlsRetriever.class);
	
    public static void main( String[] args )
    {
    	// Run this currently for tests... But in the end it should work with standard input and output.
    	new FileUtils(System.getProperty("user.dir") + "//src//main//resources//testUniversalNewList100.csv", System.getProperty("user.dir") + "//src//main//resources//testOutputFile.tsv");

		long startTime = System.nanoTime();
		
		try {
			new CrawlerController();
		} catch (RuntimeException e) {  // In case there was no input, or on other errors, there will be thrown a RuntimeException, after logging the cause.
			System.exit(-1);
		}

		// Show statistics.
		long inputUrlNum = 0;
		if ( FileUtils.skipFirstRow )
			inputUrlNum = FileUtils.getFileIndex() - FileUtils.emptyInputLines -1; // -1 to exclude the first line
		else
			inputUrlNum = FileUtils.getFileIndex() - FileUtils.emptyInputLines;

    	if ( inputUrlNum <= 0 ) {
    		logger.error("File indexer is unexpectedly reporting that no urls were retrieved from input file. Exiting..");
    		System.exit(-2);
    	}

		// Currently the below statistics need re-thinking.. they don't work so well.
		logger.info("Total urls number in the input was: " + inputUrlNum);
    	logger.info("From which, the: " + CrawlerController.urlsReachedCrawler + " reached the crawling stage.");
    	logger.info("Total docs found: " + UrlUtils.sumOfDocsFound + " That's about: " + UrlUtils.sumOfDocsFound * 100 / inputUrlNum + "%");
    	logger.info("There were: " + UrlUtils.inputDuplicatesNum + " duplicates in the input file." + " That's about: " + UrlUtils.inputDuplicatesNum * 100 / inputUrlNum + "%");


    	// Debug print of all the docPaths grouped by their domains.
/*		if ( !UrlUtils.successDomainPathsMultiMap.isEmpty() )
		{
			logger.debug("Succesful docPaths grouped by their domains are: ");
			for ( String domain : UrlUtils.successDomainPathsMultiMap.keySet() ) {
				logger.debug("Domain: " + domain);
				Collection<String> paths = UrlUtils.successDomainPathsMultiMap.get(domain);
				for ( String path : paths ) {
					logger.debug("Path: " + path);
				}
				logger.debug("\n");
			}
		}*/

		long endTime = System.nanoTime();
		long elapsedTime = endTime - startTime;
		
		String time = formatTime(elapsedTime);
		logger.debug("MainThread (not counting Crawler's time) was active for: " + time);
    	
        // Then... just close the open files (imported and exported content) and exit.
    	FileUtils.closeStreams();
    }
    
    
    public static String formatTime(long elapsedTime)
    {
    	String formatedTime = String.format("%d min, %d sec", TimeUnit.NANOSECONDS.toHours(elapsedTime),
    			TimeUnit.NANOSECONDS.toSeconds(elapsedTime) - TimeUnit.MINUTES.toSeconds(TimeUnit.NANOSECONDS.toMinutes(elapsedTime)));
    	
    	return formatedTime;
    }
}
