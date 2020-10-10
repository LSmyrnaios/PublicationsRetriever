package eu.openaire.doc_urls_retriever.test;

import eu.openaire.doc_urls_retriever.DocUrlsRetriever;
import eu.openaire.doc_urls_retriever.crawler.MachineLearning;
import eu.openaire.doc_urls_retriever.util.file.FileUtils;
import eu.openaire.doc_urls_retriever.util.signal.SignalUtils;
import eu.openaire.doc_urls_retriever.util.url.LoaderAndChecker;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.time.Instant;


/**
 * This class contains testing for all of the program's functionality, by using non-standard Input/Output.
 * @author Lampros Smyrnaios
 */
public class TestNonStandardInputOutput  {

	private static final Logger logger = LoggerFactory.getLogger(TestNonStandardInputOutput.class);

	private static final String testingSubDir = "idUrlPairs";	// "idUrlPairs" or "justUrls".
	private static final String testingDirectory = System.getProperty("user.dir") + File.separator + "testData" + File.separator + testingSubDir + File.separator;
	private static final String testInputFile = "orderedList1000.json";	// "sampleCleanUrls3000.json", "orderedList1000.json", "orderedList5000.json", "testRandomNewList100.csv", "test.json"

	private static final File inputFile = new File(testingDirectory + testInputFile);
	private static final File outputFile = new File(testingDirectory + "testOutputFile.json");


	@BeforeAll
	private static void setTypeOfInputData()
	{
		LoaderAndChecker.useIdUrlPairs = testingSubDir.equals("idUrlPairs");
		if ( !LoaderAndChecker.useIdUrlPairs )
			FileUtils.skipFirstRow = false;	// Use "true", if we have a "column-name" in our csv file. Default: "false".
		logger.info("Using the inputFile: \"" + inputFile.getName() + "\" and the outputFile: \"" + outputFile.getName() + "\".");
	}
	
	
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
		
		logger.info("Calling main method without any args..");
		
		main(args);
	}
	
	
	public static void main( String[] args )
	{
		SignalUtils.setSignalHandlers();
		
		DocUrlsRetriever.startTime = Instant.now();
		
		DocUrlsRetriever.parseArgs(args);
		
		logger.info("Starting DocUrlsRetriever..");
		
		// Use testing input/output files.
		setInputOutput();
		
		if ( MachineLearning.useMLA )
			new MachineLearning();
		
		try {
			new LoaderAndChecker();
		} catch (RuntimeException e) {  // In case there was no input, a RuntimeException will be thrown, after logging the cause.
			String errorMessage = "There was a serious error! Output data is affected! Exiting..";
			System.err.println(errorMessage);
			logger.error(errorMessage);
			FileUtils.closeIO();
			System.exit(-7);
		}
		
		DocUrlsRetriever.showStatistics(DocUrlsRetriever.startTime);
		
		// Close the open streams (imported and exported content).
		FileUtils.closeIO();
	}
	
	
	public static void setInputOutput()
	{
		setTypeOfInputData();
		try {
			new FileUtils(new FileInputStream(inputFile), new FileOutputStream(outputFile));
		} catch (FileNotFoundException e) {
			String errorMessage = "InputFile not found!";
			System.err.println(errorMessage);
			logger.error(errorMessage, e);
			FileUtils.closeIO();
			System.exit(-4);
		} catch (NullPointerException npe) {
			String errorMessage = "No input and/or output file(s) w(as/ere) given!";
			System.err.println(errorMessage);
			logger.error(errorMessage, npe);
			FileUtils.closeIO();
			System.exit(-5);
		} catch (Exception e) {
			String errorMessage = "Something went totally wrong!";
			System.err.println(errorMessage);
			logger.error(errorMessage, e);
			FileUtils.closeIO();
			System.exit(-6);
		}
	}
	
}
