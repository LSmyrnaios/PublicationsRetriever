package eu.openaire.doc_urls_retriever.test;

import eu.openaire.doc_urls_retriever.DocUrlsRetriever;
import eu.openaire.doc_urls_retriever.crawler.MachineLearning;
import eu.openaire.doc_urls_retriever.util.file.FileUtils;
import eu.openaire.doc_urls_retriever.util.http.ConnSupportUtils;
import eu.openaire.doc_urls_retriever.util.signal.SignalUtils;
import eu.openaire.doc_urls_retriever.util.url.LoaderAndChecker;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


/**
 * This class contains testing for all of the program's functionality, by using non-standard Input/Output.
 * @author Lampros Smyrnaios
 */
public class TestNonStandardInputOutput  {

	private static final Logger logger = LoggerFactory.getLogger(TestNonStandardInputOutput.class);

	private static final String testingSubDir = "idUrlPairs";	// "idUrlPairs" or "justUrls".
	private static final String testingDirectory = System.getProperty("user.dir") + File.separator + "testData" + File.separator + testingSubDir + File.separator;
	private static final String testInputFile = "orderedList1000.json";	//"test_only_ids.json";	//"id_to_url_rand10000_20201015.json";	//"test_non_utf_output.json"; //"around_200k_IDs.json";	// "sampleCleanUrls3000.json", "orderedList1000.json", "orderedList5000.json", "testRandomNewList100.csv", "test.json", "id_to_url_rand10000_20201015.json"

	private static final File inputFile = new File(testingDirectory + testInputFile);
	private static final File outputFile = new File(testingDirectory + "results_" + testInputFile);


	@BeforeAll
	private static void setTypeOfInputData()
	{
		LoaderAndChecker.useIdUrlPairs = testingSubDir.equals("idUrlPairs");
		if ( !LoaderAndChecker.useIdUrlPairs )
			FileUtils.skipFirstRow = false;	// Use "true", if we have a "column-name" in our csv file. Default: "false".

		if ( DocUrlsRetriever.inputFromUrl )
			logger.info("Using the inputFile from URL: \"" + DocUrlsRetriever.inputDataUrl + "\" and the outputFile: \"" + outputFile.getName() + "\".");
		else
			logger.info("Using the inputFile: \"" + inputFile.getName() + "\" and the outputFile: \"" + outputFile.getName() + "\".");
	}


	@Disabled	// as we want to run it only on demand, since it's a huge test. Same for the following tests of this class.
	@Test
	public void testCustomInputOutputWithNums()
	{
		String[] args = new String[7];
		args[0] = "-retrieveDataType";
		args[1] = "document";	// "document" OR "dataset" OR "all"
		args[2] = "-downloadDocFiles";
		args[3] = "-docFilesStorage";
		args[4] = "/storage/docFiles";
		args[5] = "-firstDocFileNum";
		args[6] = "1";

		logger.info("Calling main method with these args: ");
		for ( String arg: args )
			logger.info("'" + arg + "'");

		main(args);
	}


	@Disabled
	@Test
	public void testCustomInputOutputWithOriginalDocFileNames()
	{
		String[] args = new String[5];
		args[0] = "-retrieveDataType";
		args[1] = "document";	// "document" OR "dataset" OR "all"
		args[2] = "-downloadDocFiles";
		args[3] = "-docFilesStorage";
		args[4] = "/storage/docFiles";

		logger.info("Calling main method with these args: ");
		for ( String arg: args )
			logger.info("'" + arg + "'");

		main(args);
	}


	@Disabled
	@Test
	public void testCustomInputOutputWithoutDownloading()
	{
		String[] args = new String[4];
		args[0] = "-retrieveDataType";
		args[1] = "document";	// "document" OR "dataset" OR "all"
		args[2] = "-inputFileFullPath";
		args[3] = "./testData/idUrlPairs/orderedList1000.json";

		logger.info("Calling main method with these args: ");
		for ( String arg: args )
			logger.info("'" + arg + "'");

		main(args);
	}


	@Disabled
	@Test
	public void testCustomInputOutputWithoutDownloadingWithInputFile()
	{
		String[] args = new String[4];
		args[0] = "-retrieveDataType";
		args[1] = "document";	// "document" OR "dataset" OR "all"
		args[2] = "-inputFileFullPath";
		args[3] = "./testData/idUrlPairs/orderedList1000.json";

		logger.info("Calling main method with these args: ");
		for ( String arg: args )
			logger.info("'" + arg + "'");

		main(args);
	}


	@Disabled
	@Test
	public void testCustomInputOutputWithoutDownloadingWithInputUrl()
	{
		String[] args = new String[4];
		args[0] = "-retrieveDataType";
		args[1] = "document";	// "document" OR "dataset" OR "all"
		args[2] = "-inputDataUrl";
		args[3] = "https://drive.google.com/uc?export=download&id=1YIF6EkU-yqlOFnQ73hqy2Dj0GUKEKz-S";	// "orderedList1000.json"
		//args[3] = "http://localhost:8080/api/urls";

		logger.info("Calling main method with these args: ");
		for ( String arg: args )
			logger.info("'" + arg + "'");

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

		int availableThreads = Runtime.getRuntime().availableProcessors();
		availableThreads *= 2;	// Use *3 without downloading docFiles and when having the domains to appear in uniform distribution in the inputFile. Use *2 when downloading.

		// If the domains of the urls in the inputFile, are in "uniform distribution" (each one of them to be equally likely to appear in any place), then the more threads the better (triple the computer's number)
		// Else, if there are far lees domains or/and closely placed inside the inputFile.. then use only the number of threads provided by the computer, since the "politenessDelay" will block them more than the I/O would ever do..
		DocUrlsRetriever.workerThreadsCount = availableThreads;	// Due to I/O, blocking the threads all the time, more threads handle the workload faster..
		logger.info("Use " + DocUrlsRetriever.workerThreadsCount + " worker-threads.");
		DocUrlsRetriever.executor = Executors.newFixedThreadPool(DocUrlsRetriever.workerThreadsCount);	//creating a pool of <processorsCount> threads.

		try {
			new LoaderAndChecker();
		} catch (RuntimeException e) {  // In case there was no input, a RuntimeException will be thrown, after logging the cause.
			String errorMessage = "There was a serious error! Output data is affected! Exiting..";
			System.err.println(errorMessage);
			logger.error(errorMessage);
			FileUtils.closeIO();
			DocUrlsRetriever.executor.shutdownNow();
			System.exit(-7);
		}

		DocUrlsRetriever.executor.shutdown();	// Define that no new tasks will be scheduled.
		try {
			if ( !DocUrlsRetriever.executor.awaitTermination(1, TimeUnit.MINUTES) ) {
				logger.warn("The working threads did not finish on time! Stopping them immediately..");
				DocUrlsRetriever.executor.shutdownNow();
			}
		} catch (InterruptedException e) {
			DocUrlsRetriever.executor.shutdownNow();
		}

		DocUrlsRetriever.showStatistics(DocUrlsRetriever.startTime);
		
		// Close the open streams (imported and exported content).
		FileUtils.closeIO();
	}
	
	
	public static void setInputOutput()
	{
		try {
			// Check if the user gave the input file in the commandLineArgument, if not, then check for other options.
			if ( DocUrlsRetriever.inputStream == null ) {
				if ( DocUrlsRetriever.inputFromUrl )
					DocUrlsRetriever.inputStream = ConnSupportUtils.getInputStreamFromInputDataUrl();
				else
					DocUrlsRetriever.inputStream = new FileInputStream(inputFile);
			} else {
				FileUtils.numOfLines = Files.lines(Paths.get(DocUrlsRetriever.inputFileFullPath)).count();
				logger.info("The numOfLines in the inputFile is " + FileUtils.numOfLines);
			}

			new FileUtils(DocUrlsRetriever.inputStream, new FileOutputStream(outputFile));

			setTypeOfInputData();

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
