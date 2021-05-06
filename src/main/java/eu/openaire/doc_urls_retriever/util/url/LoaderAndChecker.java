package eu.openaire.doc_urls_retriever.util.url;

import com.google.common.collect.HashMultimap;
import edu.uci.ics.crawler4j.url.URLCanonicalizer;
import eu.openaire.doc_urls_retriever.DocUrlsRetriever;
import eu.openaire.doc_urls_retriever.util.file.FileUtils;
import eu.openaire.doc_urls_retriever.util.http.ConnSupportUtils;
import eu.openaire.doc_urls_retriever.util.http.HttpConnUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;


/**
 * This class contains the "loadAndCheck" code for the URLs.
 * @author Lampros Smyrnaios
 */
public class LoaderAndChecker
{
	private static final Logger logger = LoggerFactory.getLogger(LoaderAndChecker.class);
	
	public static boolean useIdUrlPairs = true;
	
	public static final Pattern DOC_URL_FILTER = Pattern.compile(".+(pdf|download|/doc|document|(?:/|[?]|&)file|/fulltext|attachment|/paper|viewfile|viewdoc|/get|cgi/viewcontent.cgi?).*");
	// "DOC_URL_FILTER" works for lowerCase Strings (we make sure they are in lowerCase before we check).
	// Note that we still need to check if it's an alive link and if it's actually a docUrl (though it's mimeType).

	private static final String dataset_formats = "(?:xls[x]?|[ct]sv|tab|(?:geo)?json|xml|ods|ddi|rdf|[g]?zip|[rt]ar|[7x]z|tgz|[gb]z[\\d]*"
			+ "|smi|por|ascii|dta|sav|dat|txt|ti[f]+|tfw|dwg|svg|sas7bdat|spss|sas|stata|(?:my|postgre)?sql(?:ite)?|bigquery|sh[px]|sb[xn]|prj|dbf|(?:m|acc)db|mif|mat|pcd|bt|n[sc]?[\\d]*|h[\\d]+|hdf[\\d]*|trs|opj|jcamp|fcs|fas(?:ta)?|keys|values)";
	public static final Pattern DATASET_URL_FILTER = Pattern.compile(".+(?:dataset[s]?/.*|(?:\\.|format=)" + dataset_formats + "(?:\\?.+)?$)");

	
	public static int numOfIDs = 0;	// The number of IDs existing in the input.
	public static AtomicInteger connProblematicUrls = new AtomicInteger(0);	// Urls known to have connectivity problems, such as long conn-times etc.
	public static AtomicInteger inputDuplicatesNum = new AtomicInteger(0);
	public static AtomicInteger reCrossedDocUrls = new AtomicInteger(0);
	public static AtomicInteger numOfIDsWithoutAcceptableSourceUrl = new AtomicInteger(0);	// The number of IDs which failed to give an acceptable sourceUrl.
	public static AtomicInteger loadingRetries = new AtomicInteger(0);

	// The following are set from the user.
	public static boolean retrieveDocuments = true;
	public static boolean retrieveDatasets = false;


	public LoaderAndChecker() throws RuntimeException
	{
		try {
			if ( useIdUrlPairs )
				loadAndCheckIdUrlPairs();
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
				FileUtils.writeToFile();
			}
		}
	}
	
	
	/**
	 * This method loads the urls from the input file in memory, in packs.
	 * If the loaded urls pass some checks, then they get connected to retrieve the docUrls
	 * Then, the loaded urls will either reach the connection point, were they will be checked for a docMimeType or they will be send directly for crawling.
	 * @throws RuntimeException if no input-urls were retrieved.
	 */
	public static void loadAndCheckUrls() throws RuntimeException
	{
		Collection<String> loadedUrlGroup;
		boolean isFirstRun = true;
		int batchCount = 0;
		
		// Start loading and checking urls.
		while ( true )
		{
			loadedUrlGroup = FileUtils.getNextUrlBatchTest();	// Take urls from single-columned (testing) csvFile.
			
			if ( isFinishedLoading(loadedUrlGroup.isEmpty(), isFirstRun) )	// Throws RuntimeException which is automatically passed on.
				break;
			else
				isFirstRun = false;

			logger.info("Batch counter: " + (++batchCount) + " | every batch contains " + FileUtils.jsonBatchSize + " id-url pairs.");

			List<Callable<Boolean>> callableTasks = new ArrayList<>(loadedUrlGroup.size());

			for ( String retrievedUrl : loadedUrlGroup )
			{
				callableTasks.add(() -> {
					String retrievedUrlToCheck = retrievedUrl;	// This is used because: "local variables referenced from a lambda expression must be final or effectively final".

					if ( (retrievedUrlToCheck = (handleUrlChecks("null", retrievedUrlToCheck))) == null )
						return false;

					String urlToCheck = retrievedUrlToCheck;
					if ( !urlToCheck.contains("#/") && (urlToCheck = URLCanonicalizer.getCanonicalURL(retrievedUrlToCheck, null, StandardCharsets.UTF_8)) == null ) {
						logger.warn("Could not canonicalize url: " + retrievedUrlToCheck);
						UrlUtils.logOutputData("null", retrievedUrlToCheck, null, "unreachable", "Discarded at loading time, due to canonicalization's problems.", null, true, "true", "false", "false", "false");
						LoaderAndChecker.connProblematicUrls.incrementAndGet();
						return false;
					}

					if ( UrlUtils.docOrDatasetUrlsWithIDs.containsKey(retrievedUrl) ) {	// If we got into an already-found docUrl, log it and return.
						ConnSupportUtils.handleReCrossedDocUrl("null", retrievedUrl, retrievedUrl, retrievedUrl, logger, true);
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
						String wasUrlValid = "true";
						if ( e instanceof RuntimeException ) {
							String message = e.getMessage();
							if ( (message != null) && message.contains("HTTP 404 Client Error") )
								wasUrlValid = "false";
						}
						UrlUtils.logOutputData("null", retrievedUrlToCheck, null, "unreachable", "Discarded at loading time, due to connectivity problems.", null, true, "true", wasUrlValid, "false", "false");
					}
					return true;
				});

			}// end for-loop

			invokeAllTasksAndWait(callableTasks);

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

		// Start loading and checking urls.
		while ( true )
		{
			loadedIdUrlPairs = FileUtils.getNextIdUrlPairBatchFromJson(); // Take urls from jsonFile.
			
			if ( isFinishedLoading(loadedIdUrlPairs.isEmpty(), isFirstRun) )	// Throws RuntimeException which is automatically passed on.
				break;
			else
				isFirstRun = false;

			logger.info("Batch counter: " + (++batchCount) + " | every batch contains " + FileUtils.jsonBatchSize + " id-url pairs.");
			
			Set<String> keys = loadedIdUrlPairs.keySet();
			numOfIDs += keys.size();
			//logger.debug("numOfIDs = " + numOfIDs);	// DEBUG!

			List<Callable<Boolean>> callableTasks = new ArrayList<>(numOfIDs);

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
							ConnSupportUtils.handleReCrossedDocUrl(retrievedId, retrievedUrl, retrievedUrl, retrievedUrl, logger, true);
							if ( !isSingleIdUrlPair )
								loggedUrlsOfCurrentId.add(retrievedUrl);
							goToNextId = true;    // Skip the best-url evaluation & connection after this loop.
							break;
						}

						String lowerCaseRetrievedUrl = retrievedUrl.toLowerCase();
						// Check if it's a possible-DocUrl, if so, this is the only url which will be checked from this id-group, unless there's a canonicalization problem.
						if ( (retrieveDocuments && DOC_URL_FILTER.matcher(lowerCaseRetrievedUrl).matches())
							|| (retrieveDatasets && DATASET_URL_FILTER.matcher(lowerCaseRetrievedUrl).matches()) ) {
							//logger.debug("Possible docUrl or datasetUrl: " + retrievedUrl);
							possibleDocOrDatasetUrl = retrievedUrl;
							break;	// This is the absolute-best-case, we go and connect directly.
						}

						// Use this rule, if we accept the slow "hdl.handle.net"
						if ( retrievedUrl.contains("/handle/") )	// If this url contains "/handle/" we know that it's a bestCaseUrl among urls from the domain "handle.net", which after redirects reaches the bestCaseUrl (containing "/handle/").
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
					if ( !sourceUrl.contains("#/") && (urlToCheck = URLCanonicalizer.getCanonicalURL(sourceUrl, null, StandardCharsets.UTF_8)) == null ) {
						logger.warn("Could not canonicalize url: " + sourceUrl);
						UrlUtils.logOutputData(retrievedId, sourceUrl, null, "unreachable", "Discarded at loading time, due to canonicalization's problems.", null, true, "true", "false", "false", "false");
						LoaderAndChecker.connProblematicUrls.incrementAndGet();

						// If other urls exits, then go and check those.
						if ( !isSingleIdUrlPair ) {    // Don't forget to write the valid but not-to-be-connected urls to the outputFile.
							loggedUrlsOfCurrentId.add(sourceUrl);
							checkRemainingUrls(retrievedId, retrievedUrlsOfCurrentId, loggedUrlsOfCurrentId, isSingleIdUrlPair);	// Go check the other urls because they might not have a canonicalization problem.
							handleLogOfRemainingUrls(retrievedId, retrievedUrlsOfCurrentId, loggedUrlsOfCurrentId);
						}
						return false;	// Exit this runnable to go to the next ID.
					}

					try {	// Check if it's a docUrl, if not, it gets crawled.
						HttpConnUtils.connectAndCheckMimeType(retrievedId, sourceUrl, urlToCheck, urlToCheck, null, true, isPossibleDocOrDatasetUrl);
						if ( !isSingleIdUrlPair )	// Otherwise it's already logged.
							loggedUrlsOfCurrentId.add(urlToCheck);
					} catch (Exception e) {
						String wasUrlValid = "true";
						if ( e instanceof RuntimeException ) {
							String message = e.getMessage();
							if ( (message != null) && message.contains("HTTP 404 Client Error") )
								wasUrlValid = "false";
						}
						UrlUtils.logOutputData(retrievedId, urlToCheck, null, "unreachable", "Discarded at loading time, due to connectivity problems.", null, true, "true", wasUrlValid, "false", "false");
						// This url had connectivity problems.. but the rest might not, go check them out.
						if ( !isSingleIdUrlPair ) {
							loggedUrlsOfCurrentId.add(urlToCheck);
							checkRemainingUrls(retrievedId, retrievedUrlsOfCurrentId, loggedUrlsOfCurrentId, isSingleIdUrlPair);	// Go check the other urls because they might not have a canonicalization problem.
						}
					}

					if ( !isSingleIdUrlPair )	// Don't forget to write the valid but not-to-be-connected urls to the outputFile.
						handleLogOfRemainingUrls(retrievedId, retrievedUrlsOfCurrentId, loggedUrlsOfCurrentId);

					return true;
				});

			}// end id-for-loop

			invokeAllTasksAndWait(callableTasks);

		}// end loading-while-loop
	}


	public static void invokeAllTasksAndWait(List<Callable<Boolean>> callableTasks)
	{
		try {	// Invoke all the tasks and wait for them to finish before moving to the next batch.
			List<Future<Boolean>> futures = DocUrlsRetriever.executor.invokeAll(callableTasks);
			int sizeOfFutures = futures.size();
			//logger.debug("sizeOfFutures: " + sizeOfFutures);	// DEBUG!
			for ( int i = 0; i < sizeOfFutures; ++i ) {
				try {
					Boolean value = futures.get(i).get();	// Get and see if an exception is thrown..
					// Add check for the value, if wanted.. (we don't care at the moment)
				} catch (ExecutionException ee) {
					String stackTraceMessage = GenericUtils.getSelectiveStackTrace(ee, null, 15);	// These can be serious errors like an "out of memory exception" (Java HEAP).
					logger.error("Task_" + (i+1) + " failed with: " + ee.getMessage() + "\n" + stackTraceMessage);
					System.err.println(stackTraceMessage);
				}
				catch (CancellationException ce) {
					logger.error("Task_" + (i+1) + " was cancelled: " + ce.getMessage());
				}
			}
		} catch (InterruptedException ie) {
			logger.warn("The main thread was interrupted when waiting for the current batch's worker-tasks to finish: " + ie.getMessage());
		}
		finally {
			FileUtils.writeToFile();	// Writes to the output file
		}
	}


	/**
	 * This method is called after a "best-case" url was detected but either had canonicalization problems or the connection failed.
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
			if ( loggedUrlsOfThisId.contains(urlToCheck)
				|| ( ((urlToCheck = URLCanonicalizer.getCanonicalURL(urlToCheck, null, StandardCharsets.UTF_8)) == null)
					|| loggedUrlsOfThisId.contains(urlToCheck) ))
					continue;

			loadingRetries.incrementAndGet();

			try {	// Check if it's a docUrl, if not, it gets crawled.
				HttpConnUtils.connectAndCheckMimeType(retrievedId, urlToCheck, urlToCheck, urlToCheck, null, true, false);
				if ( !isSingleIdUrlPair )
					loggedUrlsOfThisId.add(urlToCheck);
				return true;	// A url was checked and didn't have any problems, return and log the remaining urls.
			} catch (Exception e) {
				String wasUrlValid = "true";
				if ( e instanceof RuntimeException ) {
					String message = e.getMessage();
					if ( (message != null) && message.contains("HTTP 404 Client Error") )
						wasUrlValid = "false";
				}
				UrlUtils.logOutputData(retrievedId, urlToCheck, null, "unreachable", "Discarded at loading time, in checkRemainingUrls(), due to connectivity problems.", null, true, "true", wasUrlValid, "false", "false");
				if ( !isSingleIdUrlPair )
					loggedUrlsOfThisId.add(urlToCheck);
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
		if ( urlDomain == null ) {    // If the domain is not found, it means that a serious problem exists with this docPage and we shouldn't crawl it.
			logger.warn("Problematic URL in \"LoaderAndChecker.handleUrlChecks()\": \"" + retrievedUrl + "\"");
			UrlUtils.logOutputData(urlId, retrievedUrl, null, "unreachable", "Discarded in 'LoaderAndChecker.handleUrlChecks()' method, after the occurrence of a domain-retrieval error.", null, true, "true", "false", "false", "false");
			if ( !useIdUrlPairs )
				connProblematicUrls.incrementAndGet();
			return null;
		}
		
		if ( HttpConnUtils.blacklistedDomains.contains(urlDomain) ) {	// Check if it has been blackListed after running internal links' checks.
			logger.debug("Avoid connecting to blackListed domain: \"" + urlDomain + "\" with url: " + retrievedUrl);
			UrlUtils.logOutputData(urlId, retrievedUrl, null, "unreachable", "Discarded in 'LoaderAndChecker.handleUrlChecks()' method, as its domain was found blackListed.", null, true, "true", "true", "false", "false");
			if ( !useIdUrlPairs )
				connProblematicUrls.incrementAndGet();
			return null;
		}
		
		if ( ConnSupportUtils.checkIfPathIs403BlackListed(retrievedUrl, urlDomain) ) {	// The path-extraction is independent of the jsessionid-removal, so this gets executed before.
			logger.debug("Preventing reaching 403ErrorCode with url: \"" + retrievedUrl + "\"!");
			UrlUtils.logOutputData(urlId, retrievedUrl, null, "unreachable", "Discarded in 'LoaderAndChecker.handleUrlChecks()' as it had a blackListed urlPath.", null, true, "true", "true", "false", "false");
			if ( !useIdUrlPairs )
				connProblematicUrls.incrementAndGet();
			return null;
		}
		
		String lowerCaseUrl = retrievedUrl.toLowerCase();
		
		if ( UrlTypeChecker.matchesUnwantedUrlType(urlId, retrievedUrl, lowerCaseUrl) )
			return null;	// The url-logging is happening inside this method (per urlType).
		
		// Remove the "temporalId" from the urls. Most of them, if not all, will already be expired. If an error occurs, the temporalId will remain in the url.
		if ( lowerCaseUrl.contains("token") || lowerCaseUrl.contains("jsessionid") )
			retrievedUrl = UrlUtils.removeTemporalIdentifier(retrievedUrl);	// We send the non-lowerCase-url as we may want to continue with that url in case of an error.

		// Check if it's a duplicate.
		if ( UrlUtils.duplicateUrls.contains(retrievedUrl) ) {
			logger.debug("Skipping url: \"" + retrievedUrl + "\", at loading, as it has already be seen!");
			UrlUtils.logOutputData(urlId, retrievedUrl, null, "duplicate", "Discarded in 'LoaderAndChecker.handleUrlChecks()', as it's a duplicate.", null, false, "true", "true", "false", "false");
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
				DocUrlsRetriever.executor.shutdownNow();
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
			// Some of the "retrieved-urls" maybe were excluded before the canonicalization point (e.g. because their domains were blocked or were duplicates).
			// We have to make sure the "equal()" and the "contains()" succeed on the same-started-urls.
			String tempUrl = retrievedUrl;
			if ( !retrievedUrl.contains("#/") )
				if ( (retrievedUrl = URLCanonicalizer.getCanonicalURL(retrievedUrl, null, StandardCharsets.UTF_8)) == null )
					retrievedUrl = tempUrl;	// Make sure we keep it on canonicalization-failure.

			if ( !loggedUrlsOfThisId.contains(retrievedUrl) )
				UrlUtils.logOutputData(retrievedId, retrievedUrl, null, "unreachable",
					"Skipped in LoaderAndChecker, as a better url was selected for id: " + retrievedId, null, true, "false", "N/A", "N/A", "N/A");
		}
	}
	
}
