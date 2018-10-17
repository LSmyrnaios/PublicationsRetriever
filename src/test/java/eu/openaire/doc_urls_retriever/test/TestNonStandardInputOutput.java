package eu.openaire.doc_urls_retriever.test;

import eu.openaire.doc_urls_retriever.DocUrlsRetriever;
import eu.openaire.doc_urls_retriever.crawler.CrawlerController;
import eu.openaire.doc_urls_retriever.util.file.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;


public class TestNonStandardInputOutput  {
	
	private static final Logger logger = LoggerFactory.getLogger(DocUrlsRetriever.class);
	
	
	public static void main( String[] args )
	{
		DocUrlsRetriever.parseArgs(args);
		
		// Use testing input/output files.
		try {
			new FileUtils(new FileInputStream(new File(System.getProperty("user.dir") + File.separator + "testing/sampleCleanUrls3000.json")),
							new FileOutputStream(new File(System.getProperty("user.dir") + File.separator + "testing/testOutputFile.json")));
		} catch (FileNotFoundException e) {
			String errorMessage = "InputFile not found!";
			System.err.println(errorMessage);
			logger.error(errorMessage, e);
			System.exit(-4);
		}
		
		try {
			new CrawlerController();
		} catch (RuntimeException e) {  // In case there was no input, or on Crawler4j's failure to be initialized, there will be thrown a RuntimeException, after logging the cause.
			String errorMessage = "There was a serious error! Output data is affected! Exiting..";
			System.err.println(errorMessage);
			logger.error(errorMessage);
			System.exit(-5);
		}
		
		DocUrlsRetriever.showStatistics();
		
		// Close the open streams (imported and exported content).
		FileUtils.closeStreams();
	}
	
}
