package eu.openaire.publications_retriever.test;

import eu.openaire.publications_retriever.PublicationsRetriever;
import eu.openaire.publications_retriever.crawler.MachineLearning;
import eu.openaire.publications_retriever.util.args.ArgsUtils;
import eu.openaire.publications_retriever.util.file.FileUtils;
import eu.openaire.publications_retriever.util.http.ConnSupportUtils;
import eu.openaire.publications_retriever.util.signal.SignalUtils;
import eu.openaire.publications_retriever.util.url.GenericUtils;
import eu.openaire.publications_retriever.util.url.LoaderAndChecker;
import eu.openaire.publications_retriever.util.url.UrlTypeChecker;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;


/**
 * This class contains testing for all the program's functionality, by using non-standard Input/Output.
 * @author Lampros Smyrnaios
 */
public class TestNonStandardInputOutput  {

	private static final Logger logger = LoggerFactory.getLogger(TestNonStandardInputOutput.class);

	private static final String testingSubDir = "idUrlPairs";	// "idUrlPairs" or "justUrls".
	private static final String testingDirectory = FileUtils.workingDir + "testData" + File.separator + testingSubDir + File.separator;
	private static final String testInputFile = "orderedList500.json";	//"test_only_ids.json";	//"id_to_url_rand10000_20201015.json";	//"test_non_utf_output.json"; //"around_200k_IDs.json";	// "sampleCleanUrls3000.json", "orderedList1000.json", "orderedList5000.json", "testRandomNewList100.csv", "test.json", "id_to_url_rand10000_20201015.json"

	// TODO - Add an extra cmd-argument to define the "upper-limit" of the number of records to process from a file.
	// Thus, we will have a single 5000 or 10000 records file and process only the first N records, instead of having multiple files of smaller length.

	private static final File inputFile = new File(testingDirectory + testInputFile);
	private static File outputFile = new File(testingDirectory + "results_" + testInputFile);	// This can change if the user gives the inputFile as a cmd-arg.


	@BeforeAll
	public static void setTypeOfInputData()
	{
		LoaderAndChecker.useIdUrlPairs = testingSubDir.equals("idUrlPairs");
		if ( !LoaderAndChecker.useIdUrlPairs )
			FileUtils.skipFirstRow = false;	// Use "true", if we have a "column-name" in our csv file. Default: "false".

		if ( ArgsUtils.inputFromUrl )
			logger.info("Using the inputFile from URL: \"" + ArgsUtils.inputDataUrl + "\" and the outputFile: \"" + outputFile.getName() + "\".");
		else
			logger.info("Using the inputFile: \"" + inputFile.getName() + "\" and the outputFile: \"" + outputFile.getName() + "\".");
	}


	@Disabled	// as we want to run it only on demand, since it's a huge test. Same for the following tests of this class.
	@Test
	public void testCustomInputOutputWithNums()
	{
		String[] args = new String[9];
		args[0] = "-retrieveDataType";
		args[1] = "document";	// "document" OR "dataset" OR "all"
		args[2] = "-downloadDocFiles";
		args[3] = "-fileNameType";
		args[4] = "numberName";
		args[5] = "-firstFileNum";
		args[6] = "1";
		args[7] = "-docFilesStorage";
		args[8] = "docFiles";
		main(args);
	}


	@Disabled
	@Test
	public void testCustomInputOutputWithOriginalDocFileNames()
	{
		String[] args = new String[7];
		args[0] = "-retrieveDataType";
		args[1] = "document";	// "document" OR "dataset" OR "all"
		args[2] = "-downloadDocFiles";
		args[3] = "-fileNameType";
		args[4] = "originalName";
		args[5] = "-docFilesStorage";
		args[6] = "docFiles";
		main(args);
	}


	@Disabled
	@Test
	public void testCustomInputOutputWithIdDocFileNames()
	{
		String[] args = new String[7];
		args[0] = "-retrieveDataType";
		args[1] = "document";	// "document" OR "dataset" OR "all"
		args[2] = "-downloadDocFiles";
		args[3] = "-fileNameType";
		args[4] = "idName";
		args[5] = "-docFilesStorage";
		args[6] = "docFiles";
		main(args);
	}


	@Disabled
	@Test
	public void testCustomInputOutputWithIdDocFileNamesToS3()
	{
		String[] args = new String[7];
		args[0] = "-retrieveDataType";
		args[1] = "document";	// "document" OR "dataset" OR "all"
		args[2] = "-downloadDocFiles";
		args[3] = "-fileNameType";
		args[4] = "idName";
		args[5] = "-docFilesStorage";
		args[6] = "S3ObjectStore";
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
		main(args);
	}


	@Disabled
	@Test
	public void testCustomInputOutputWithDatasets()
	{
		String[] args = new String[4];
		args[0] = "-retrieveDataType";
		args[1] = "dataset";	// "document" OR "dataset" OR "all"
		args[2] = "-inputFileFullPath";
		args[3] = "./testData/idUrlPairs/datasets_100.json";
		main(args);
	}



	@Disabled
	@Test
	public void testCustomInputOutputWithoutDownloadingWithInputFile()
	{
		String[] args = new String[6];
		args[0] = "-retrieveDataType";
		args[1] = "document";	// "document" OR "dataset" OR "all"
		args[2] = "-inputFileFullPath";
		args[3] = "./testData/idUrlPairs/orderedList1000.json";
		args[4] = "-numOfThreads";
		args[5] = "10";
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
		main(args);
	}


	public static void main( String[] args )
	{
		logger.info("Calling main method with these args: ");
		for ( String arg: args )
			logger.info("'" + arg + "'");

		SignalUtils.setSignalHandlers();
		
		PublicationsRetriever.startTime = Instant.now();
		
		ArgsUtils.parseArgs(args);

		if ( ! GenericUtils.checkInternetConnectivity() ) {
			FileUtils.closeIO();
			System.exit(-44);
		}

		logger.info("Starting PublicationsRetriever..");
		ConnSupportUtils.setKnownMimeTypes();
		UrlTypeChecker.setRuntimeInitializedRegexes();

		// When testing, always delete old files.
		ArgsUtils.shouldDeleteOlderDocFiles = true;

		// Use testing input/output files.
		setInputOutput();

		if ( MachineLearning.useMLA )
			new MachineLearning();

		if ( ArgsUtils.workerThreadsCount == 0 ) {	// If the user did not provide the "workerThreadsCount", then get the available number from the system.
			int availableThreads = Runtime.getRuntime().availableProcessors();
			availableThreads *= ArgsUtils.threadsMultiplier;

			// If the domains of the urls in the inputFile, are in "uniform distribution" (each one of them to be equally likely to appear in any place), then the more threads the better (triple the computer's number)
			// Else, if there are far lees domains or/and closely placed inside the inputFile.. then use only the number of threads provided by the computer, since the "politenessDelay" will block them more than the I/O would ever do..
			ArgsUtils.workerThreadsCount = availableThreads;	// Due to I/O, blocking the threads all the time, more threads handle the workload faster..
		}
		logger.info("Use " + ArgsUtils.workerThreadsCount + " worker-threads.");
		PublicationsRetriever.executor = Executors.newFixedThreadPool(ArgsUtils.workerThreadsCount);

		try {
			new LoaderAndChecker();
		} catch (RuntimeException e) {  // In case there was no input, a RuntimeException will be thrown, after logging the cause.
			String errorMessage = "There was a serious error! Output data is affected! Exiting..";
			System.err.println(errorMessage);
			logger.error(errorMessage);
			FileUtils.closeIO();
			PublicationsRetriever.executor.shutdownNow();
			System.exit(-7);
		}

		logger.info("Shutting down the threads..");
		PublicationsRetriever.executor.shutdown();	// Define that no new tasks will be scheduled.
		try {
			if ( !PublicationsRetriever.executor.awaitTermination(1, TimeUnit.MINUTES) ) {
				logger.warn("The working threads did not finish on time! Stopping them immediately..");
				PublicationsRetriever.executor.shutdownNow();
			}
		} catch (SecurityException se) {
			logger.error("Could not shutdown the threads in any way..!", se);
		} catch (InterruptedException ie) {
			try {
				PublicationsRetriever.executor.shutdownNow();
			} catch (SecurityException se) {
				logger.error("Could not shutdown the threads in any way..!", se);
			}
		}

		PublicationsRetriever.showStatistics(PublicationsRetriever.startTime);
		
		// Close the open streams (imported and exported content).
		FileUtils.closeIO();
	}
	
	
	public static void setInputOutput()
	{
		try {
			// Check if the user gave the input file in the commandLineArgument, if not, then check for other options.
			if ( ArgsUtils.inputStream == null ) {
				if ( ArgsUtils.inputFromUrl )
					ArgsUtils.inputStream = ConnSupportUtils.getInputStreamFromInputDataUrl();
				else
					ArgsUtils.inputStream = new BufferedInputStream(new FileInputStream(inputFile), FileUtils.fiveMb);
			} else {
				try ( Stream<String> linesStream = Files.lines(Paths.get(ArgsUtils.inputFileFullPath), StandardCharsets.UTF_8) ) {
					FileUtils.numOfLines = linesStream.count();
					logger.info("The numOfLines in the inputFile is " + FileUtils.numOfLines);
				} catch (IOException ioe) {
					logger.error("Problem when retrieving the input-\"numOfLines\"!", ioe);
				}
			}

			if ( ArgsUtils.inputFileFullPath != null ) {	// If the user gave the inputFile as a cmd-arg..
				// Extract the path and the file-name. Do a split in reverse order.
				String path = null;
				String inputFileName = null;
				char separatorChar = File.separator.charAt(0);	// The "inputFileFullPath" is guaranteed to have at least one "separator".
				for ( int i = ArgsUtils.inputFileFullPath.length() -1; i >= 0 ; --i ) {
					if ( ArgsUtils.inputFileFullPath.charAt(i) == separatorChar ) {
						i++;	// The following methods need the increased < i >
						path = ArgsUtils.inputFileFullPath.substring(0, i);
						inputFileName = ArgsUtils.inputFileFullPath.substring(i);
						break;
					}
				}
				if ( path != null )
					outputFile = new File(path + "results_" + inputFileName);
			}

			new FileUtils(ArgsUtils.inputStream, new BufferedOutputStream(new FileOutputStream(outputFile), FileUtils.fiveMb));

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
