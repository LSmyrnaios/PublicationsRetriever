package eu.openaire.publications_retriever.util.signal;

import eu.openaire.publications_retriever.PublicationsRetriever;
import eu.openaire.publications_retriever.util.file.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;


/**
 * @author Lampros Smyrnaios
 */
public class SignalUtils {
	
	private static final Logger logger = LoggerFactory.getLogger(SignalUtils.class);

	public static boolean receivedSIGINT = false;

	
	public static void setSignalHandlers()
	{
		Signal.handle(new Signal("INT"), sig -> {
			try {
				PublicationsRetriever.executor.shutdownNow();

				SignalUtils.receivedSIGINT = true;

				// Print the related interrupted-state-message.
				String stopMessage = "The normal program execution was interrupted by a \"SIGINT\"-signal!";
				logger.warn(stopMessage);
				System.err.println(stopMessage);

				// Write whatever remaining quadruples exist in memory.
				if ( !FileUtils.dataForOutput.isEmpty() ) {
					logger.debug("Writing the remaining quadruples to the outputFile.");
					FileUtils.writeResultsToFile();
				}

				// If the program managed to set the "startTime" before the signal was received, then show the statistics and the elapsed-time.
				if ( PublicationsRetriever.startTime != null )
					PublicationsRetriever.showStatistics(PublicationsRetriever.startTime);

				FileUtils.closeIO();
				System.exit(-12);
			} catch (Exception e) {
				String errMsg = "Unexpected exception in SIGNAL HANDLER! The program will terminate immediately, without any guarantees of data-flushing in the output!\n" + e;
				System.err.println(errMsg);
				logger.error(errMsg);
				System.exit(-133);
			}
		});
	}
	
}
