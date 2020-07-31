package eu.openaire.doc_urls_retriever.util.signal;

import eu.openaire.doc_urls_retriever.DocUrlsRetriever;
import eu.openaire.doc_urls_retriever.util.file.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;

import java.time.Instant;


/**
 * @author Lampros Smyrnaios
 */
public class SignalUtils {
	
	private static final Logger logger = LoggerFactory.getLogger(SignalUtils.class);
	
	
	public static void setSignalHandlers()
	{
		Signal.handle(new Signal("INT"), sig -> {
			
			// Print the related interrupted-state-message.
			String stopMessage = "The normal program execution was interrupted by a \"SIGINT\"-signal!";
			logger.warn(stopMessage);
			System.err.println(stopMessage);
			
			// Write whatever remaining quadruples exist in memory.
			if ( !FileUtils.quadrupleToBeLoggedList.isEmpty() ) {
				logger.debug("Writing the remaining quadruples to the outputFile.");
				FileUtils.writeToFile();
			}
			
			FileUtils.closeIO();
			
			// If the program managed to set the "startTime" before the signal was received, then show the execution time.
			if ( DocUrlsRetriever.startTime != null )
				DocUrlsRetriever.calculateAndPrintElapsedTime(DocUrlsRetriever.startTime, Instant.now());
			
			System.exit(-12);
		});
	}
	
}
