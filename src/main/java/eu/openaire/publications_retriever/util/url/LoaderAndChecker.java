package eu.openaire.publications_retriever.util.url;

import com.google.common.collect.HashMultimap;
import crawlercommons.filters.basic.BasicURLNormalizer;
import eu.openaire.publications_retriever.PublicationsRetriever;
import eu.openaire.publications_retriever.exceptions.ConnTimeoutException;
import eu.openaire.publications_retriever.exceptions.DomainBlockedException;
import eu.openaire.publications_retriever.exceptions.DomainWithUnsupportedHEADmethodException;
import eu.openaire.publications_retriever.util.args.ArgsUtils;
import eu.openaire.publications_retriever.util.file.FileUtils;
import eu.openaire.publications_retriever.util.http.ConnSupportUtils;
import eu.openaire.publications_retriever.util.http.HttpConnUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.CookieStore;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * This class contains the "loadAndCheck" code for the URLs.
 * @author Lampros Smyrnaios
 */
public class LoaderAndChecker
{
	private static final Logger logger = LoggerFactory.getLogger(LoaderAndChecker.class);
	
	public static boolean useIdUrlPairs = true;
	
	public static final Pattern DOC_URL_FILTER = Pattern.compile(".+(pdf|download|/doc|document|(?:/|[?]|&)file|/fulltext|attachment|/paper|view(?:file|doc)|/get|cgi/viewcontent.cgi\\?|t[ée]l[ée]charger|descargar).*");
	// "DOC_URL_FILTER" works for lowerCase Strings (we make sure they are in lowerCase before we check).
	// Note that we still need to check if it's an alive link and if it's actually a docUrl (though it's mimeType).

	private static final String dataset_formats = "xls[x]?|[ct]sv|tab|(?:geo)?json|xml|ods|ddi|rdf|[g]?zip|[rt]ar|[7x]z|tgz|[gb]z[\\d]*|smi|por|ascii|dta|sav|dat|txt|ti[f]+|tfw|dwg"
			+ "|svg|sas7bdat|spss|sas|stata|(?:my|postgre)?sql(?:ite)?|bigquery|sh[px]|sb[xn]|prj|dbf|(?:m|acc)db|mif|mat|pcd|bt|n[sc]?[\\d]*|h[\\d]+|hdf[\\d]*|trs|opj|jcamp|fcs|fas(?:ta)?|keys|values";
	public static final Pattern DATASET_URL_FILTER = Pattern.compile(".+(?:dataset[s]?/.*|(?:\\.|format=)" + dataset_formats + "(?:\\?.+)?$)");


	public static final BasicURLNormalizer basicURLNormalizer = BasicURLNormalizer.newBuilder().build();

	public static int numOfIDs = 0;	// The number of IDs existing in the input.
	public static AtomicInteger connProblematicUrls = new AtomicInteger(0);	// Urls known to have connectivity problems, such as long conn-times etc.
	public static AtomicInteger inputDuplicatesNum = new AtomicInteger(0);
	public static AtomicInteger numOfIDsWithoutAcceptableSourceUrl = new AtomicInteger(0);	// The number of IDs which failed to give an acceptable sourceUrl.
	public static AtomicInteger loadingRetries = new AtomicInteger(0);
	public static AtomicInteger totalNumFailedTasks = new AtomicInteger(0);


	// The following are set from the user.
	public static boolean retrieveDocuments = true;
	public static boolean retrieveDatasets = true;


	public LoaderAndChecker() throws RuntimeException
	{
		setCouldRetryRegex();
		try {
			if ( useIdUrlPairs )
				loadAndCheckIdUrlPairs();
				//loadAndCheckEachIdUrlPair();
				//loadAndCheckEachIdUrlPairInEntries();
			else
				loadAndCheckUrls();
		} catch (Exception e) {
			logger.error("", e);
			throw new RuntimeException(e);
		}
		finally {
			// Write any remaining quadruples from memory to disk (we normally write every "FileUtils.jasonGroupSize" quadruples, so a few last quadruples might have not be written yet).
			if ( !FileUtils.dataToBeLoggedList.isEmpty() ) {
				logger.debug("Writing last quadruples to the outputFile.");
				FileUtils.writeResultsToFile();
			}
		}
	}
	
	
	/**
	 * This method loads the urls from the input file in memory, in packs.
	 * If the loaded urls pass some checks, then they get connected to retrieve the docUrls
	 * Then, the loaded urls will either reach the connection point, were they will be checked for a docMimeType, or they will be sent directly for crawling.
	 * @throws RuntimeException if no input-urls were retrieved.
	 */
	public static void loadAndCheckUrls() throws RuntimeException
	{
		Collection<String> loadedUrlGroup;
		boolean isFirstRun = true;
		int batchCount = 0;

		CookieStore cookieStore = HttpConnUtils.cookieManager.getCookieStore();
		List<Callable<Boolean>> callableTasks = new ArrayList<>(FileUtils.jsonBatchSize);

		// Start loading and checking urls.
		while ( true )
		{
			loadedUrlGroup = FileUtils.getNextUrlBatchTest();	// Take urls from single-columned (testing) csvFile.
			
			if ( isFinishedLoading(loadedUrlGroup.isEmpty(), isFirstRun) )
				break;
			else
				isFirstRun = false;

			logger.info("Batch counter: " + (++batchCount) + ((ArgsUtils.inputFileFullPath != null) ? (" | progress: " + PublicationsRetriever.df.format(((batchCount-1) * FileUtils.jsonBatchSize) * 100.0 / FileUtils.numOfLines) + "%") : "") + " | every batch contains at most " + FileUtils.jsonBatchSize + " id-url pairs.");

			for ( String retrievedUrl : loadedUrlGroup )
			{
				callableTasks.add(() -> {
					String retrievedUrlToCheck = retrievedUrl;	// This is used because: "local variables referenced from a lambda expression must be final or effectively final".

					if ( (retrievedUrlToCheck = handleUrlChecks("null", retrievedUrlToCheck)) == null )
						return false;

					String urlToCheck = retrievedUrlToCheck;
					if ( (urlToCheck = basicURLNormalizer.filter(retrievedUrlToCheck)) == null ) {
						logger.warn("Could not normalize url: " + retrievedUrlToCheck);
						UrlUtils.logOutputData("null", retrievedUrlToCheck, null, UrlUtils.unreachableDocOrDatasetUrlIndicator, "Discarded at loading time, due to normalization's problems.", null, true, "true", "false", "false", "false", "false", null, "null");
						LoaderAndChecker.connProblematicUrls.incrementAndGet();
						return false;
					}

					if ( UrlUtils.docOrDatasetUrlsWithIDs.containsKey(retrievedUrl) ) {	// If we got into an already-found docUrl, log it and return.
						ConnSupportUtils.handleReCrossedDocUrl("null", retrievedUrl, retrievedUrl, retrievedUrl, true);
						return true;
					}

					boolean isPossibleDocOrDatasetUrl = false;
					String lowerCaseRetrievedUrl = retrievedUrlToCheck.toLowerCase();
					if ( (retrieveDocuments && DOC_URL_FILTER.matcher(lowerCaseRetrievedUrl).matches())
							|| (retrieveDatasets && DATASET_URL_FILTER.matcher(lowerCaseRetrievedUrl).matches()) )
						isPossibleDocOrDatasetUrl = true;

					try {	// We sent the < null > into quotes to avoid causing NPEs in the thread-safe datastructures that do not support null input.
						HttpConnUtils.connectAndCheckMimeType("null", retrievedUrlToCheck, urlToCheck, urlToCheck, null, true, isPossibleDocOrDatasetUrl);
					} catch (Exception e) {
						List<String> list = getWasValidAndCouldRetry(e, urlToCheck);
						String wasUrlValid = list.get(0);
						String couldRetry = list.get(1);
						String errorMsg = "Discarded at loading time, as " + list.get(2);
						UrlUtils.logOutputData("null", retrievedUrlToCheck, null, UrlUtils.unreachableDocOrDatasetUrlIndicator, errorMsg, null, true, "true", wasUrlValid, "false", "false", couldRetry, null, "null");
						return false;
					}
					return true;
				});
			}// end for-loop
			executeTasksAndHandleResults(callableTasks, batchCount, cookieStore);
		}// end while-loop
	}

	
	/**
	 * This method loads the id-url pairs from the input file in memory, in packs.
	 * Then, it groups them per ID and selects the best url -after checks- of each-ID to connect-with and retrieve the docUrl.
	 * @throws RuntimeException if no input-urls were retrieved.
	 */
	public static void loadAndCheckIdUrlPairs() throws RuntimeException
	{
		HashMultimap<String, String> loadedIdUrlPairs;
		boolean isFirstRun = true;
		int batchCount = 0;

		CookieStore cookieStore = HttpConnUtils.cookieManager.getCookieStore();	// This cookie store is a reference to the one used throughout the execution.
		List<Callable<Boolean>> callableTasks = new ArrayList<>(FileUtils.jsonBatchSize);

		// Start loading and checking urls.
		while ( true )
		{
			loadedIdUrlPairs = FileUtils.getNextIdUrlPairBatchFromJson(); // Take urls from jsonFile.
			
			if ( isFinishedLoading(loadedIdUrlPairs.isEmpty(), isFirstRun) )
				break;
			else
				isFirstRun = false;

			logger.info("Batch counter: " + (++batchCount) + ((ArgsUtils.inputFileFullPath != null) ? (" | progress: " + PublicationsRetriever.df.format(((batchCount-1) * FileUtils.jsonBatchSize) * 100.0 / FileUtils.numOfLines) + "%") : "") + " | every batch contains at most " + FileUtils.jsonBatchSize + " id-url pairs.");
			
			Set<String> keys = loadedIdUrlPairs.keySet();
			numOfIDs += keys.size();
			//logger.debug("numOfIDs = " + numOfIDs);	// DEBUG!

			for ( String retrievedId : keys )
			{
				HashMultimap<String, String> finalLoadedIdUrlPairs = loadedIdUrlPairs;

				callableTasks.add(() -> {
					boolean goToNextId = false;
					String possibleDocOrDatasetUrl = null;
					String bestNonDocNonDatasetUrl = null;	// Best-case url
					String nonDoiUrl = null;	// Url which is not a best case, but it's not a slow-doi url either.
					String neutralUrl = null;	// Just a neutral url.
					String urlToCheck = null;

					Set<String> retrievedUrlsOfCurrentId = finalLoadedIdUrlPairs.get(retrievedId);

					boolean isSingleIdUrlPair = (retrievedUrlsOfCurrentId.size() == 1);
					HashSet<String> loggedUrlsOfCurrentId = new HashSet<>();	// New for every ID. It does not need to be synchronized.

					for ( String retrievedUrl : retrievedUrlsOfCurrentId )
					{
						String checkedUrl = retrievedUrl;
						if ( (retrievedUrl = handleUrlChecks(retrievedId, retrievedUrl)) == null ) {
							if ( !isSingleIdUrlPair )
								loggedUrlsOfCurrentId.add(checkedUrl);
							continue;
						}	// The "retrievedUrl" might have changed (inside "handleUrlChecks()").

						if ( UrlUtils.docOrDatasetUrlsWithIDs.containsKey(retrievedUrl) ) {	// If we got into an already-found docUrl, log it and return.
							ConnSupportUtils.handleReCrossedDocUrl(retrievedId, retrievedUrl, retrievedUrl, retrievedUrl, true);
							if ( !isSingleIdUrlPair )
								loggedUrlsOfCurrentId.add(retrievedUrl);
							goToNextId = true;    // Skip the best-url evaluation & connection after this loop.
							break;
						}

						String lowerCaseRetrievedUrl = retrievedUrl.toLowerCase();
						// Check if it's a possible-DocUrl, if so, this is the only url which will be checked from this id-group, unless there's a normalization problem.
						if ( (retrieveDocuments && DOC_URL_FILTER.matcher(lowerCaseRetrievedUrl).matches())
							|| (retrieveDatasets && DATASET_URL_FILTER.matcher(lowerCaseRetrievedUrl).matches()) ) {
							//logger.debug("Possible docUrl or datasetUrl: " + retrievedUrl);
							possibleDocOrDatasetUrl = retrievedUrl;
							break;	// This is the absolute-best-case, we go and connect directly.
						}

						// Use this rule, if we accept the slow "hdl.handle.net"
						if ( retrievedUrl.contains("/handle/") )	// If this url contains "/handle/" we know that it's a bestCaseUrl among urls from the domain "handle.net", which, after redirects, reaches the bestCaseUrl (containing "/handle/").
							bestNonDocNonDatasetUrl = retrievedUrl;	// We can't just connect here, as the next url might be a possibleDocOrDatasetUrl.
						else if ( (bestNonDocNonDatasetUrl == null) && !retrievedUrl.contains("doi.org") )	// If no other preferable url is found, we should prefer the nonDOI-one, if present, as the DOI-urls have lots of redirections.
							nonDoiUrl = retrievedUrl;
						else
							neutralUrl = retrievedUrl;	// If no special-goodCase-url is found, this one will be used. Note that this will be null if no acceptable-url was found.
					}// end-url-for-loop

					if ( goToNextId ) {	// If we found an already-retrieved docUrl.
						if ( !isSingleIdUrlPair )	// Don't forget to write the valid but not-to-be-connected urls to the outputFile.
							handleLogOfRemainingUrls(retrievedId, retrievedUrlsOfCurrentId, loggedUrlsOfCurrentId);
						return false;	// Exit this runnable to go to the next ID.
					}

					boolean isPossibleDocOrDatasetUrl = false;	// Used for specific connection settings.
					// Decide with which url from this id-group we should connect to.
					if ( possibleDocOrDatasetUrl != null ) {
						urlToCheck = possibleDocOrDatasetUrl;
						isPossibleDocOrDatasetUrl = true;
					}
					else if ( bestNonDocNonDatasetUrl != null )
						urlToCheck = bestNonDocNonDatasetUrl;
					else if ( nonDoiUrl != null )
						urlToCheck = nonDoiUrl;
					else if ( neutralUrl != null )
						urlToCheck = neutralUrl;
					else {
						logger.debug("No acceptable sourceUrl was found for ID: \"" + retrievedId + "\".");
						numOfIDsWithoutAcceptableSourceUrl.incrementAndGet();
						return false;	// Exit this runnable to go to the next ID.
					}

					String sourceUrl = urlToCheck;	// Hold it here for the logging-messages.
					if ( (urlToCheck = basicURLNormalizer.filter(sourceUrl)) == null ) {
						logger.warn("Could not normalize url: " + sourceUrl);
						UrlUtils.logOutputData(retrievedId, sourceUrl, null, UrlUtils.unreachableDocOrDatasetUrlIndicator, "Discarded at loading time, due to normalization's problems.", null, true, "true", "false", "false", "false", "false", null, "null");
						LoaderAndChecker.connProblematicUrls.incrementAndGet();

						// If other urls exits, then go and check those.
						if ( !isSingleIdUrlPair ) {    // Don't forget to write the valid but not-to-be-connected urls to the outputFile.
							loggedUrlsOfCurrentId.add(sourceUrl);
							checkRemainingUrls(retrievedId, retrievedUrlsOfCurrentId, loggedUrlsOfCurrentId, isSingleIdUrlPair);	// Go check the other urls because they might not have a normalization problem.
							handleLogOfRemainingUrls(retrievedId, retrievedUrlsOfCurrentId, loggedUrlsOfCurrentId);
						}
						return false;	// Exit this runnable to go to the next ID.
					}

					boolean wasSuccessful = true;
					try {	// Check if it's a docUrl, if not, it gets crawled.
						HttpConnUtils.connectAndCheckMimeType(retrievedId, sourceUrl, urlToCheck, urlToCheck, null, true, isPossibleDocOrDatasetUrl);
						if ( !isSingleIdUrlPair )	// Otherwise it's already logged.
							loggedUrlsOfCurrentId.add(urlToCheck);
						// Here the runnable was successful in any case.
					} catch (Exception e) {
						List<String> list = getWasValidAndCouldRetry(e, urlToCheck);
						String wasUrlValid = list.get(0);
						String couldRetry = list.get(1);
						String errorMsg = "Discarded at loading time, as " + list.get(2);
						UrlUtils.logOutputData(retrievedId, urlToCheck, null, UrlUtils.unreachableDocOrDatasetUrlIndicator, errorMsg, null, true, "true", wasUrlValid, "false", "false", couldRetry, null, "null");
						// This url had connectivity problems.. but the rest might not, go check them out.
						if ( !isSingleIdUrlPair ) {
							loggedUrlsOfCurrentId.add(urlToCheck);
							wasSuccessful = checkRemainingUrls(retrievedId, retrievedUrlsOfCurrentId, loggedUrlsOfCurrentId, isSingleIdUrlPair);	// Go check the other urls because they might not have a connection problem.
						} else
							wasSuccessful = false;
					}

					if ( !isSingleIdUrlPair )	// Don't forget to write the valid but not-to-be-connected urls to the outputFile.
						handleLogOfRemainingUrls(retrievedId, retrievedUrlsOfCurrentId, loggedUrlsOfCurrentId);

					return wasSuccessful;
				});
			}// end id-for-loop
			executeTasksAndHandleResults(callableTasks, batchCount, cookieStore);
		}// end loading-while-loop
	}


	/**
	 * This method loads the id-url pairs from the input file in memory, in packs.
	 * Then, it iterates through the id-url paris and checks them.
	 * @throws RuntimeException if no input-urls were retrieved.
	 */
	public static void loadAndCheckEachIdUrlPairInEntries() throws RuntimeException
	{
		HashMultimap<String, String> loadedIdUrlPairs;
		boolean isFirstRun = true;
		int batchCount = 0;

		CookieStore cookieStore = HttpConnUtils.cookieManager.getCookieStore();	// This cookie store is a reference to the one used throughout the execution.

		List<Callable<Boolean>> callableTasks = new ArrayList<>(FileUtils.jsonBatchSize);

		// Start loading and checking urls.
		while ( true )
		{
			loadedIdUrlPairs = FileUtils.getNextIdUrlPairBatchFromJson(); // Take urls from jsonFile.

			if ( isFinishedLoading(loadedIdUrlPairs.isEmpty(), isFirstRun) )
				break;
			else
				isFirstRun = false;

			logger.info("Batch counter: " + (++batchCount) + ((ArgsUtils.inputFileFullPath != null) ? (" | progress: " + PublicationsRetriever.df.format(((batchCount-1) * FileUtils.jsonBatchSize) * 100.0 / FileUtils.numOfLines) + "%") : "") + " | every batch contains at most " + FileUtils.jsonBatchSize + " id-url pairs.");

			Set<Map.Entry<String, String>> pairs = loadedIdUrlPairs.entries();
			numOfIDs += pairs.size();

			for ( Map.Entry<String,String> pair : pairs )
			{
				callableTasks.add(() -> {
					String retrievedId = pair.getKey();
					String retrievedUrl = pair.getValue();

					if ( (retrievedUrl = handleUrlChecks(retrievedId, retrievedUrl)) == null ) {
						return false;
					}    // The "retrievedUrl" might have changed (inside "handleUrlChecks()").

					String urlToCheck = retrievedUrl;
					String sourceUrl = urlToCheck;    // Hold it here for the logging-messages.
					if ( (urlToCheck = basicURLNormalizer.filter(sourceUrl)) == null ) {
						logger.warn("Could not normalize url: " + sourceUrl);
						UrlUtils.logOutputData(retrievedId, sourceUrl, null, UrlUtils.unreachableDocOrDatasetUrlIndicator, "Discarded at loading time, due to normalization's problems.", null, true, "true", "false", "false", "false", "false", null, "null");
						LoaderAndChecker.connProblematicUrls.incrementAndGet();
						return false;
					}

					if ( UrlUtils.docOrDatasetUrlsWithIDs.containsKey(retrievedUrl) ) {    // If we got into an already-found docUrl, log it and return.
						ConnSupportUtils.handleReCrossedDocUrl(retrievedId, retrievedUrl, retrievedUrl, retrievedUrl, true);
						return true;
					}

					boolean isPossibleDocOrDatasetUrl = false;    // Used for specific connection settings.
					String lowerCaseRetrievedUrl = retrievedUrl.toLowerCase();
					// Check if it's a possible-DocUrl, if so, this info will be used for optimal web-connection later.
					if ( (retrieveDocuments && DOC_URL_FILTER.matcher(lowerCaseRetrievedUrl).matches())
							|| (retrieveDatasets && DATASET_URL_FILTER.matcher(lowerCaseRetrievedUrl).matches()) ) {
						//logger.debug("Possible docUrl or datasetUrl: " + retrievedUrl);
						isPossibleDocOrDatasetUrl = true;
					}

					try {    // Check if it's a docUrl, if not, it gets crawled.
						HttpConnUtils.connectAndCheckMimeType(retrievedId, sourceUrl, urlToCheck, urlToCheck, null, true, isPossibleDocOrDatasetUrl);
					} catch (Exception e) {
						List<String> list = getWasValidAndCouldRetry(e, urlToCheck);
						String wasUrlValid = list.get(0);
						String couldRetry = list.get(1);
						String errorMsg = "Discarded at loading time, as " + list.get(2);
						UrlUtils.logOutputData(retrievedId, urlToCheck, null, UrlUtils.unreachableDocOrDatasetUrlIndicator, errorMsg, null, true, "true", wasUrlValid, "false", "false", couldRetry, null, "null");
						return false;
					}
					return true;
				});
			}// end pairs-for-loop
			executeTasksAndHandleResults(callableTasks, batchCount, cookieStore);
		}// end loading-while-loop
	}


	/**
	 * This method loads the id-url pairs from the input file in memory, in packs.
	 * Then, it groups them per ID and checks all the urls.
	 * @throws RuntimeException if no input-urls were retrieved.
	 */
	public static void loadAndCheckEachIdUrlPair() throws RuntimeException
	{
		HashMultimap<String, String> loadedIdUrlPairs;
		boolean isFirstRun = true;
		int batchCount = 0;

		CookieStore cookieStore = HttpConnUtils.cookieManager.getCookieStore();
		List<Callable<Boolean>> callableTasks = new ArrayList<>(FileUtils.jsonBatchSize);

		// Start loading and checking urls.
		while ( true )
		{
			loadedIdUrlPairs = FileUtils.getNextIdUrlPairBatchFromJson(); // Take urls from jsonFile.

			if ( isFinishedLoading(loadedIdUrlPairs.isEmpty(), isFirstRun) )
				break;
			else
				isFirstRun = false;

			logger.info("Batch counter: " + (++batchCount) + ((ArgsUtils.inputFileFullPath != null) ? (" | progress: " + PublicationsRetriever.df.format(((batchCount-1) * FileUtils.jsonBatchSize) * 100.0 / FileUtils.numOfLines) + "%") : "") + " | every batch contains at most " + FileUtils.jsonBatchSize + " id-url pairs.");

			for ( String retrievedId : loadedIdUrlPairs.keySet() ) {

				Set<String> retrievedUrlsOfCurrentId = loadedIdUrlPairs.get(retrievedId);
				numOfIDs += retrievedUrlsOfCurrentId.size();

				// Each task is handling a different ID, so that the threads will be less blocked due to connecting to the same domain.
				callableTasks.add(() -> {
					for ( String retrievedUrl : retrievedUrlsOfCurrentId )
					{
						if ( (retrievedUrl = handleUrlChecks(retrievedId, retrievedUrl)) == null ) {
							continue;
						}    // The "retrievedUrl" might have changed (inside "handleUrlChecks()").

						String urlToCheck = retrievedUrl;
						String sourceUrl = urlToCheck;    // Hold it here for the logging-messages.
						if ( (urlToCheck = basicURLNormalizer.filter(sourceUrl)) == null ) {
							logger.warn("Could not normalize url: " + sourceUrl);
							UrlUtils.logOutputData(retrievedId, sourceUrl, null, UrlUtils.unreachableDocOrDatasetUrlIndicator, "Discarded at loading time, due to normalization's problems.", null, true, "true", "false", "false", "false", "false", null, "null");
							LoaderAndChecker.connProblematicUrls.incrementAndGet();
							continue;
						}

						if ( UrlUtils.docOrDatasetUrlsWithIDs.containsKey(retrievedUrl) ) {    // If we got into an already-found docUrl, log it and return.
							ConnSupportUtils.handleReCrossedDocUrl(retrievedId, retrievedUrl, retrievedUrl, retrievedUrl, true);
							continue;
						}

						boolean isPossibleDocOrDatasetUrl = false;    // Used for specific connection settings.
						String lowerCaseRetrievedUrl = retrievedUrl.toLowerCase();
						// Check if it's a possible-DocUrl, if so, this info will be used for optimal web-connection later.
						if ( (retrieveDocuments && DOC_URL_FILTER.matcher(lowerCaseRetrievedUrl).matches())
								|| (retrieveDatasets && DATASET_URL_FILTER.matcher(lowerCaseRetrievedUrl).matches()) ) {
							//logger.debug("Possible docUrl or datasetUrl: " + retrievedUrl);
							isPossibleDocOrDatasetUrl = true;
						}

						try {    // Check if it's a docUrl, if not, it gets crawled.
							HttpConnUtils.connectAndCheckMimeType(retrievedId, sourceUrl, urlToCheck, urlToCheck, null, true, isPossibleDocOrDatasetUrl);
						} catch (Exception e) {
							List<String> list = getWasValidAndCouldRetry(e, urlToCheck);
							String wasUrlValid = list.get(0);
							String couldRetry = list.get(1);
							String errorMsg = "Discarded at loading time, as " + list.get(2);
							UrlUtils.logOutputData(retrievedId, urlToCheck, null, UrlUtils.unreachableDocOrDatasetUrlIndicator, errorMsg, null, true, "true", wasUrlValid, "false", "false", couldRetry, null, "null");
							return false;
						}
					}
					return true;
				});
			}// end for-id-loop
			executeTasksAndHandleResults(callableTasks, batchCount, cookieStore);
		}// end loading-while-loop
	}


	public static void executeTasksAndHandleResults(List<Callable<Boolean>> callableTasks, int batchCount, CookieStore cookieStore)
	{
		int numFailedTasks = invokeAllTasksAndWait(callableTasks);
		if ( numFailedTasks == -1 ) {
			FileUtils.writeResultsToFile();	// Writes to the output file
			System.err.println("Invoking and/or executing the callableTasks failed with the exception written in the log files!");
			System.exit(99);
		} else if ( numFailedTasks > 0 ) {
			logger.warn(numFailedTasks + " tasks failed in batch_" + batchCount);
			totalNumFailedTasks.incrementAndGet();
		}

		callableTasks.clear();
		logger.debug("The number of cookies is: " + cookieStore.getCookies().size());
		boolean cookiesDeleted = cookieStore.removeAll();
		logger.debug(cookiesDeleted ? "The cookies where removed!" : "No cookies where removed!");
		FileUtils.writeResultsToFile();	// Writes to the output file
	}


	public static int invokeAllTasksAndWait(List<Callable<Boolean>> callableTasks)
	{
		int numFailedTasks = 0;
		try {	// Invoke all the tasks and wait for them to finish before moving to the next batch.
			List<Future<Boolean>> futures = PublicationsRetriever.executor.invokeAll(callableTasks);
			int sizeOfFutures = futures.size();
			//logger.debug("sizeOfFutures: " + sizeOfFutures);	// DEBUG!
			for ( int i = 0; i < sizeOfFutures; ++i ) {
				try {
					Boolean value = futures.get(i).get();	// Get and see if an exception is thrown..
					// Add check for the value, if wanted.. (we don't care at the moment)
				} catch (ExecutionException ee) {
					String stackTraceMessage = GenericUtils.getSelectedStackTraceForCausedException(ee, "Task_" + i + " failed with: ", null, 15);	// These can be serious errors like an "out of memory exception" (Java HEAP).
					logger.error(stackTraceMessage);
					System.err.println(stackTraceMessage);
					numFailedTasks ++;
				} catch (CancellationException ce) {
					logger.error("Task_" + i + " was cancelled: " + ce.getMessage());
					numFailedTasks ++;
				} catch (IndexOutOfBoundsException ioobe) {
					logger.error("IOOBE for task_" + i + " in the futures-list! " + ioobe.getMessage());
				}
			}
		} catch (InterruptedException ie) {	// In this case, any unfinished tasks are cancelled.
			logger.warn("The main thread was interrupted when waiting for the current batch's worker-tasks to finish: " + ie.getMessage());
		} catch (Exception e) {
			logger.error("", e);
			return -1;
		}
		return numFailedTasks;
	}


	/**
	 * This method is called after a "best-case" url was detected but either had normalization problems or the connection failed.
	 * @param retrievedId
	 * @param retrievedUrlsOfThisId
	 * @param loggedUrlsOfThisId
	 * @param isSingleIdUrlPair
	 * @return
	 */
	private static boolean checkRemainingUrls(String retrievedId, Set<String> retrievedUrlsOfThisId, HashSet<String> loggedUrlsOfThisId, boolean isSingleIdUrlPair)
	{
		for ( String urlToCheck : retrievedUrlsOfThisId )
		{
			// Check this url -before and after normalization- against the logged urls of this ID.
			if ( loggedUrlsOfThisId.contains(urlToCheck)
				|| ( ((urlToCheck = basicURLNormalizer.filter(urlToCheck)) != null) && loggedUrlsOfThisId.contains(urlToCheck) ) )
					continue;

			loadingRetries.incrementAndGet();

			try {	// Check if it's a docUrl, if not, it gets crawled.
				HttpConnUtils.connectAndCheckMimeType(retrievedId, urlToCheck, urlToCheck, urlToCheck, null, true, false);
				if ( !isSingleIdUrlPair )
					loggedUrlsOfThisId.add(urlToCheck);
				return true;	// A url was checked and didn't have any problems, return and log the remaining urls.
			} catch (Exception e) {
				List<String> list = getWasValidAndCouldRetry(e, urlToCheck);
				String wasUrlValid = list.get(0);
				String couldRetry = list.get(1);
				String errorMsg = "Discarded at loading time, in checkRemainingUrls(), as " + list.get(2);
				UrlUtils.logOutputData(retrievedId, urlToCheck, null, UrlUtils.unreachableDocOrDatasetUrlIndicator, errorMsg, null, true, "true", wasUrlValid, "false", "false", couldRetry, null, "null");
				if ( !isSingleIdUrlPair )
					loggedUrlsOfThisId.add(urlToCheck);
				// Try the next url..
			}
		}
		return false;
	}

	
	/**
	 * This method checks if the given url is either of unwantedType or if it's a duplicate in the input, while removing the potential jsessionid from the url.
	 * It returns the givenUrl without the jsessionidPart if this url is accepted for connection/crawling, otherwise, it returns "null".
	 * @param urlId
	 * @param retrievedUrl
	 * @return the non-jsessionid-url-string / null for unwanted-duplicate-url
	 */
	public static String handleUrlChecks(String urlId, String retrievedUrl)
	{
		String urlDomain = UrlUtils.getDomainStr(retrievedUrl, null);
		if ( urlDomain == null ) {    // If the domain is not found, it means that a serious problem exists with this docPage, and we shouldn't crawl it.
			// The reason is already logged.
			UrlUtils.logOutputData(urlId, retrievedUrl, null, UrlUtils.unreachableDocOrDatasetUrlIndicator, "Discarded in 'LoaderAndChecker.handleUrlChecks()' method, after the occurrence of a domain-retrieval error.", null, true, "true", "false", "false", "false", "false", null, "null");
			if ( !useIdUrlPairs )
				connProblematicUrls.incrementAndGet();
			return null;
		}
		
		if ( HttpConnUtils.blacklistedDomains.contains(urlDomain) ) {	// Check if it has been blacklisted after running internal links' checks.
			logger.debug("Avoid connecting to blacklisted domain: \"" + urlDomain + "\" with url: " + retrievedUrl);
			UrlUtils.logOutputData(urlId, retrievedUrl, null, UrlUtils.unreachableDocOrDatasetUrlIndicator, "Discarded in 'LoaderAndChecker.handleUrlChecks()' method, as its domain was found blacklisted.", null, true, "true", "true", "false", "false", "false", null, "null");
			if ( !useIdUrlPairs )
				connProblematicUrls.incrementAndGet();
			return null;
		}
		
		if ( ConnSupportUtils.checkIfPathIs403BlackListed(retrievedUrl, urlDomain) ) {	// The path-extraction is independent of the jsessionid-removal, so this gets executed before.
			logger.debug("Preventing reaching 403ErrorCode with url: \"" + retrievedUrl + "\"!");
			UrlUtils.logOutputData(urlId, retrievedUrl, null, UrlUtils.unreachableDocOrDatasetUrlIndicator, "Discarded in 'LoaderAndChecker.handleUrlChecks()' as it had a blacklisted urlPath.", null, true, "true", "true", "false", "false", "false", null, "null");
			if ( !useIdUrlPairs )
				connProblematicUrls.incrementAndGet();
			return null;
		}
		
		String lowerCaseUrl = retrievedUrl.toLowerCase();
		
		if ( UrlTypeChecker.shouldNotAcceptPageUrl(urlId, retrievedUrl, retrievedUrl, lowerCaseUrl, true) )
			return null;	// The url-logging is happening inside this method (per urlType).
		
		// Remove the "temporalId" from the urls. Most of them, if not all, will already be expired. If an error occurs, the temporalId will remain in the url.
		if ( lowerCaseUrl.contains("token") || lowerCaseUrl.contains("jsessionid") )
			retrievedUrl = UrlUtils.removeTemporalIdentifier(retrievedUrl);	// We send the non-lowerCase-url as we may want to continue with that url in case of an error.

		// Check if it's a duplicate.
		if ( UrlUtils.duplicateUrls.contains(retrievedUrl) ) {
			logger.debug("Skipping non-DocOrDataset-url: \"" + retrievedUrl + "\", at loading, as it has already been checked.");
			UrlUtils.logOutputData(urlId, retrievedUrl, null, UrlUtils.duplicateUrlIndicator, "Discarded in 'LoaderAndChecker.handleUrlChecks()', as it's a duplicate.", null, false, "true", "N/A", "N/A", "N/A", "true", null, "null");
			if ( !useIdUrlPairs )
				inputDuplicatesNum.incrementAndGet();
			return null;
		}
		
		// Handle the weird case of: "ir.lib.u-ryukyu.ac.jp"
		// See: http://ir.lib.u-ryukyu.ac.jp/handle/123456789/8743
		// Note that this is NOT the case for all of the urls containing "/handle/123456789/".. but just for this domain.
		if ( retrievedUrl.contains("ir.lib.u-ryukyu.ac.jp") && retrievedUrl.contains("/handle/123456789/") ) {
			logger.debug("We will handle the weird case of \"" + retrievedUrl + "\".");
			return StringUtils.replace(retrievedUrl, "/123456789/", "/20.500.12000/", -1);
		}
		
		return retrievedUrl;	// The calling method needs the non-jsessionid-string.
	}
	
	
	/**
	 * This method checks if there is no more input-data and returns true in that case.
	 * Otherwise, it returns false, if there is more input-data to be loaded.
	 * A "RuntimeException" is thrown if no input-urls were retrieved in general.
	 * @param isEmptyOfData
	 * @param isFirstRun
	 * @return finished loading / not finished
	 * @throws RuntimeException
	 */
	public static boolean isFinishedLoading(boolean isEmptyOfData, boolean isFirstRun)
	{
		if ( isEmptyOfData ) {
			if ( isFirstRun ) {
				String errorMessage = "Could not retrieve any urls from the inputFile! Exiting..";
				System.err.println(errorMessage);
				logger.error(errorMessage);
				PublicationsRetriever.executor.shutdownNow();
				System.exit(100);
			} else {
				logger.debug("Done processing " + FileUtils.getCurrentlyLoadedUrls() + " urls from the inputFile.");
				return true;
			}
		}
		return false;
	}
	
	
	/**
	 * This method logs the remaining retrievedUrls which were not checked & connected.
	 * The method loadAndCheckIdUrlPairs() picks just one -the best- url from a group of urls belonging to a specific ID.
	 * The rest urls will either get rejected as problematic -and so get logged- or get skipped and be left non-logged.
	 * @param retrievedId
	 * @param retrievedUrlsOfThisId
	 * @param loggedUrlsOfThisId
	 */
	private static void handleLogOfRemainingUrls(String retrievedId, Set<String> retrievedUrlsOfThisId, HashSet<String> loggedUrlsOfThisId)
	{
		for ( String retrievedUrl : retrievedUrlsOfThisId )
		{
			// Some "retrieved-urls" maybe were excluded before the normalization point (e.g. because their domains were blocked or were duplicates).
			// We have to make sure the "contains()" succeed on the same-started-urls.
			String tempUrl = retrievedUrl;
			if ( (retrievedUrl = basicURLNormalizer.filter(retrievedUrl)) == null )
					retrievedUrl = tempUrl;	// Make sure we check the non-normalized version.

			if ( !loggedUrlsOfThisId.contains(retrievedUrl) )
				UrlUtils.logOutputData(retrievedId, retrievedUrl, null, UrlUtils.unreachableDocOrDatasetUrlIndicator,
					"Skipped in LoaderAndChecker, as a better url was selected for id: " + retrievedId, null, true, "false", "N/A", "N/A", "N/A", "true", null, "null");
		}
	}


	public static final Pattern INVALID_URL_HTTP_STATUS = Pattern.compile(".*HTTP 4(?:00|04|10|14|22) Client Error.*");
	public static Pattern COULD_RETRY_HTTP_STATUS;

	public static void setCouldRetryRegex()
	{
		String debugLog;
		String couldRetryRegexString = ".*(?:HTTP 4(?:0[38]|2[569]) Client|";	// This is the "starting" pattern.
		if ( ConnSupportUtils.shouldBlockMost5XXDomains ) {
			couldRetryRegexString += "503";    // Only retry for 503-urls. The 503-domains are also excluded from been blocked.
			debugLog = "Going to block most of the 5XX domains, except from the 503-domains.";
		} else {
			couldRetryRegexString += "(?<!511)";    // Retry for every 5XX url EXCEPT for the 511-urls. The 511-domains are also blocked in this case.
			debugLog = "Going to avoid to block most of the 5XX domains, except from the 511-domains, which will be blocked.";
		}
		couldRetryRegexString += " Server) Error.*";
		logger.debug(debugLog + " The \"couldRetryRegex\" is: " + couldRetryRegexString);
		COULD_RETRY_HTTP_STATUS = Pattern.compile(couldRetryRegexString);
	}


	public static Pattern COULD_RETRY_URLS = Pattern.compile("[^/]+://[^/]*(?:sciencedirect|elsevier).com[^/]*/.*");
	// The urls having the aforementioned domains are likely to be specially-handled in future updates, so we want to keep their urls available for retrying.

	public static List<String> getWasValidAndCouldRetry(Exception e, String url)
	{
		List<String> list = new ArrayList<>(3);
		String wasUrlValid = "true";
		String couldRetry = "false";
		String errorMsg;

		if ( e instanceof RuntimeException ) {	// This check also covers the: (e != null) check.
			String message = e.getMessage();
			if ( message != null) {
				if ( INVALID_URL_HTTP_STATUS.matcher(message).matches() ) {
					wasUrlValid = "false";
					errorMsg = "the url is invalid and lead to http-client-error.";
				} else if ( COULD_RETRY_HTTP_STATUS.matcher(message).matches() ) {
					couldRetry = "true";    // 	We could retry at a later time, since some errors might be temporal.
					errorMsg = "the url had a non-fatal http-error.";
				} else
					errorMsg = "there is a serious unspecified error.";
			} else
				errorMsg = "there is an unspecified runtime error.";
		} else if ( e instanceof ConnTimeoutException ) {
			couldRetry = "true";
			errorMsg = "the url had a connection-timeout.";
		} else if ( e instanceof DomainWithUnsupportedHEADmethodException ) {	// This should never get caught here normally.
			couldRetry = "true";
			errorMsg = "the url does not support HEAD method for checking most of the internal links.";
		} else if ( e instanceof DomainBlockedException ) {
			// the default values apply
			errorMsg = "the url had its initial or redirected domain blocked.";
		} else
			errorMsg = "there is a serious unspecified error.";

		if ( (url != null) && COULD_RETRY_URLS.matcher(url).matches() )
			couldRetry = "true";

		list.add(0, wasUrlValid);
		list.add(1, couldRetry);
		list.add(2, errorMsg);
		return list;
	}
	
}
