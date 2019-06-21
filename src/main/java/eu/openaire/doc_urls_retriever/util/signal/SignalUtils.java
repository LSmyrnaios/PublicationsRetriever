package eu.openaire.doc_urls_retriever.util.signal;

import eu.openaire.doc_urls_retriever.DocUrlsRetriever;
import eu.openaire.doc_urls_retriever.util.file.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;

import java.time.Instant;


public class SignalUtils {
	
	private static final Logger logger = LoggerFactory.getLogger(SignalUtils.class);
	
	
	public static void setSignalHandlers()
	{
		Signal.handle(new Signal("INT"), sig -> {
			
			// First write whatever remaining quadruple exist in memory.
			if ( !FileUtils.quadrupleToBeLoggedList.isEmpty() ) {
				logger.debug("Writing last quadruples to disk.");
				FileUtils.writeToFile();
			}
			
			// Close the stream.
			FileUtils.closeStreams();
			String stopMessage = "Program execution stopped because of a \"SIGINT\"-signal!";
			logger.warn(stopMessage);
			System.err.println(stopMessage);
			System.err.flush();
			
			// If the program managed to set the "startTime" before the signal was recieved, then show the execution time.
			if ( DocUrlsRetriever.startTime != null )
				DocUrlsRetriever.calculateAndPrintElapsedTime(DocUrlsRetriever.startTime, Instant.now());
			
			System.exit(-12);
		});
	}
	
}
