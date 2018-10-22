package eu.openaire.doc_urls_retriever.test;

import eu.openaire.doc_urls_retriever.DocUrlsRetriever;
import eu.openaire.doc_urls_retriever.util.url.LoaderAndChecker;
import eu.openaire.doc_urls_retriever.util.file.FileUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;


/**
 * This class contains testing for all of the program's functionality, by using non-standard Input/Output.
 * @author Lampros A. Smyrnaios
 */
public class TestNonStandardInputOutput  {
	
	private static final Logger logger = LoggerFactory.getLogger(TestNonStandardInputOutput.class);
	
	
	@Disabled	// as we want to run it only on demand, since it's a huge test. Same for the following tests of this class.
	@Test
	public void testCustomInputOutputWithNums()
	{
		String[] args = new String[3];
		args[0] = "-downloadDocFiles";
		args[1] = "-firstDocFileNum";
		args[2] = "1";
		
		logger.info("Calling main method with these args: ");
		for ( String arg: args )
			logger.info("'" + arg + "'");
		
		main(args);
	}
	
	
	@Disabled
	@Test
	public void testCustomInputOutputWithOriginalDocFileNames()
	{
		String[] args = new String[1];
		args[0] = "-downloadDocFiles";
		
		logger.info("Calling main method with this arg: " + args[0]);
		
		main(args);
	}
	
	
	@Disabled
	@Test
	public void testCustomInputOutputWithoutDownloading()
	{
		String[] args = new String[0];
		
		logger.info("Calling main method without any args");
		
		main(args);
	}
	
	
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
			new LoaderAndChecker();
		} catch (RuntimeException e) {  // In case there was no input, a RuntimeException will be thrown, after logging the cause.
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
