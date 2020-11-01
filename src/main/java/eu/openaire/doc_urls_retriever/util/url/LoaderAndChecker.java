package eu.openaire.doc_urls_retriever.util.url;

import com.google.common.collect.HashMultimap;
import edu.uci.ics.crawler4j.url.URLCanonicalizer;
import eu.openaire.doc_urls_retriever.util.file.FileUtils;
import eu.openaire.doc_urls_retriever.util.http.ConnSupportUtils;
import eu.openaire.doc_urls_retriever.util.http.HttpConnUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
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
	
	public static int numOfIDs = 0;	// The number of IDs existing in the input.
	public static int connProblematicUrls = 0;	// Urls known to have connectivity problems, such as long conn-times etc.
	public static int inputDuplicatesNum = 0;
	
	public static int numOfIDsWithoutAcceptableSourceUrl = 0;	// The number of IDs which failed to give an acceptable sourceUrl.

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
			if ( !FileUtils.quadrupleToBeLoggedList.isEmpty() ) {
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
		
		// Start loading and checking urls.
		while ( true )
		{
			loadedUrlGroup = FileUtils.getNextUrlGroupTest();	// Take urls from single-columned (testing) csvFile.
			
			if ( isFinishedLoading(loadedUrlGroup.isEmpty(), isFirstRun) )	// Throws RuntimeException which is automatically passed on.
				break;
			else
				isFirstRun = false;
			
			for ( String retrievedUrl : loadedUrlGroup )
			{
				if ( (retrievedUrl = handleUrlChecks(null, retrievedUrl)) == null )
					continue;
				
				boolean isPossibleDocUrl = false;
				if ( DOC_URL_FILTER.matcher(retrievedUrl.toLowerCase()).matches() )
					isPossibleDocUrl = true;

				String urlToCheck;
				if ( (urlToCheck = URLCanonicalizer.getCanonicalURL(retrievedUrl, null, StandardCharsets.UTF_8)) == null ) {
					logger.warn("Could not cannonicalize url: " + retrievedUrl);
					UrlUtils.logQuadruple(null, retrievedUrl, null, "unreachable", "Discarded at loading time, due to cannonicalization's problems.", null);
					LoaderAndChecker.connProblematicUrls ++;
					continue;
				}

				try {
					HttpConnUtils.connectAndCheckMimeType(null, retrievedUrl, urlToCheck, urlToCheck, null, true, isPossibleDocUrl);
				} catch (Exception e) {
					UrlUtils.logQuadruple(null, retrievedUrl, null, "unreachable", "Discarded at loading time, due to connectivity problems.", null);
				}
			}// end for-loop
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
		
		// Start loading and checking urls.
		while ( true )
		{
			loadedIdUrlPairs = FileUtils.getNextIdUrlPairGroupFromJson(); // Take urls from jsonFile.
			
			if ( isFinishedLoading(loadedIdUrlPairs.isEmpty(), isFirstRun) )	// Throws RuntimeException which is automatically passed on.
				break;
			else
				isFirstRun = false;
			
			Set<String> keys = loadedIdUrlPairs.keySet();
			numOfIDs += keys.size();
			
			for ( String retrievedId : keys )
			{
				boolean goToNextId = false;
				String possibleDocUrl = null;
				String bestNonDocUrl = null;	// Best-case url
				String nonDoiUrl = null;	// Url which is not a best case, but it's not a slow-doi url either.
				String neutralUrl = null;	// Just a neutral url.
				String urlToCheck = null;

				Set<String> retrievedUrlsOfCurrentId = loadedIdUrlPairs.get(retrievedId);
				boolean isSingleIdUrlPair = (retrievedUrlsOfCurrentId.size() == 1);
				HashSet<String> loggedUrlsOfCurrentId = new HashSet<>();	// New for every ID.

				for ( String retrievedUrl : retrievedUrlsOfCurrentId )
				{
					String checkedUrl = retrievedUrl;
					if ( (retrievedUrl = handleUrlChecks(retrievedId, retrievedUrl)) == null ) {
						if ( !isSingleIdUrlPair )
							loggedUrlsOfCurrentId.add(checkedUrl);
						continue;
					}	// The "retrievedUrl" might have changed (inside "handleUrlChecks()").
					
					if ( UrlUtils.docUrlsWithIDs.containsKey(retrievedUrl) ) {	// If we got into an already-found docUrl, log it and return.
						logger.info("re-crossed docUrl found: < " + retrievedUrl + " >");
						if ( FileUtils.shouldDownloadDocFiles )
							UrlUtils.logQuadruple(retrievedId, retrievedUrl, retrievedUrl, retrievedUrl, UrlUtils.alreadyDownloadedByIDMessage + UrlUtils.docUrlsWithIDs.get(retrievedUrl), null);
						else
							UrlUtils.logQuadruple(retrievedId, retrievedUrl, retrievedUrl, retrievedUrl, "", null);

						if ( !isSingleIdUrlPair )
							loggedUrlsOfCurrentId.add(retrievedUrl);

						goToNextId = true;    // Skip the best-url evaluation & connection after this loop.
						break;
					}
					
					// Check if it's a possible-DocUrl, if so, this is the only url which will be checked from this group, unless there's a canonicalization problem.
					if ( DOC_URL_FILTER.matcher(retrievedUrl.toLowerCase()).matches() ) {
						//logger.debug("Possible docUrl: " + retrievedUrl);
						possibleDocUrl = retrievedUrl;
						break;	// This is the absolute-best-case, we go and connect directly.
					}
					
					// Use this rule, if we accept the slow "hdl.handle.net"
					if ( retrievedUrl.contains("/handle/") )	// If this url contains "/handle/" we know that it's a bestCaseUrl among urls from the domain "handle.net", which after redirects reaches the bestCaseUrl (containing "/handle/").
						bestNonDocUrl = retrievedUrl;	// We can't just connect here, as the next url might be a possibleDocUrl.
					else if ( (bestNonDocUrl == null) && !retrievedUrl.contains("doi.org") )	// If no other preferable url is found, we should prefer the nonDOI-one, if present, as the DOI-urls have lots of redirections.
						nonDoiUrl = retrievedUrl;
					else
						neutralUrl = retrievedUrl;	// If no special-goodCase-url is found, this one will be used. Note that this will be null if no acceptable-url was found.
				}// end-url-for-loop
				
				if ( goToNextId ) {	// If we found an already-retrieved docUrl.
					if ( !isSingleIdUrlPair )	// Don't forget to write the valid but not-to-be-connected urls to the outputFile.
						handleLogOfRemainingUrls(null, retrievedId, retrievedUrlsOfCurrentId, loggedUrlsOfCurrentId);
					continue;
				}

				boolean isPossibleDocUrl = false;	// Used for specific connection settings.
				// Decide with which url from this id-group we should connect to.
				if ( possibleDocUrl != null ) {
					urlToCheck = possibleDocUrl;
					isPossibleDocUrl = true;
				}
				else if ( bestNonDocUrl != null )
					urlToCheck = bestNonDocUrl;
				else if ( nonDoiUrl != null )
					urlToCheck = nonDoiUrl;
				else if ( neutralUrl != null )
					urlToCheck = neutralUrl;
				else {
					logger.debug("No acceptable sourceUrl was found for ID: \"" + retrievedId + "\".");
					numOfIDsWithoutAcceptableSourceUrl ++;
					continue;
				}

				if ( !isSingleIdUrlPair )	// Don't forget to write the valid but not-to-be-connected urls to the outputFile.
					handleLogOfRemainingUrls(urlToCheck, retrievedId, retrievedUrlsOfCurrentId, loggedUrlsOfCurrentId);

				String sourceUrl = urlToCheck;	// Hold it here for the logging-messages.
				if ( (urlToCheck = URLCanonicalizer.getCanonicalURL(sourceUrl, null, StandardCharsets.UTF_8)) == null ) {
					logger.warn("Could not cannonicalize url: " + sourceUrl);
					UrlUtils.logQuadruple(retrievedId, sourceUrl, null, "unreachable", "Discarded at loading time, due to cannonicalization's problems.", null);
					LoaderAndChecker.connProblematicUrls ++;
					continue;
				}

				try {	// Check if it's a docUrl, if not, it gets crawled.
					HttpConnUtils.connectAndCheckMimeType(retrievedId, sourceUrl, urlToCheck, urlToCheck, null, true, isPossibleDocUrl);
				} catch (Exception e) {
					UrlUtils.logQuadruple(retrievedId, urlToCheck, null, "unreachable", "Discarded at loading time, due to connectivity problems.", null);
				}
			}// end id-for-loop
		}// end loading-while-loop
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
			logger.warn("Problematic URL in \"UrlUtils.handleUrlChecks()\": \"" + retrievedUrl + "\"");
			UrlUtils.logQuadruple(urlId, retrievedUrl, null, "unreachable", "Discarded in 'UrlUtils.handleUrlChecks()' method, after the occurrence of a domain-retrieval error.", null);
			if ( !useIdUrlPairs )
				connProblematicUrls ++;
			return null;
		}
		
		if ( HttpConnUtils.blacklistedDomains.contains(urlDomain) ) {	// Check if it has been blackListed after running internal links' checks.
			logger.debug("Avoid connecting to blackListed domain: \"" + urlDomain + "\"!");
			UrlUtils.logQuadruple(urlId, retrievedUrl, null, "unreachable", "Discarded in 'UrlUtils.handleUrlChecks()' method, as its domain was found blackListed.", null);
			if ( !useIdUrlPairs )
				connProblematicUrls ++;
			return null;
		}
		
		if ( ConnSupportUtils.checkIfPathIs403BlackListed(retrievedUrl, urlDomain) ) {	// The path-extraction is independent of the jsessionid-removal, so this gets executed before.
			logger.debug("Preventing reaching 403ErrorCode with url: \"" + retrievedUrl + "\"!");
			UrlUtils.logQuadruple(urlId, retrievedUrl, null, "unreachable", "Discarded in 'UrlUtils.handleUrlChecks()' as it had a blackListed urlPath.", null);
			if ( !useIdUrlPairs )
				connProblematicUrls ++;
			return null;
		}
		
		String lowerCaseUrl = retrievedUrl.toLowerCase();
		
		if ( UrlTypeChecker.matchesUnwantedUrlType(urlId, retrievedUrl, lowerCaseUrl) )
			return null;	// The url-logging is happening inside this method (per urlType).
		
		// Remove "jsessionid" for urls. Most of them, if not all, will already be expired. If an error occurs, the jsessionid will remain in the url.
		if ( lowerCaseUrl.contains("jsessionid") )
			retrievedUrl = UrlUtils.removeJsessionid(retrievedUrl);
		
		// Check if it's a duplicate.
		if ( UrlUtils.duplicateUrls.contains(retrievedUrl) ) {
			logger.debug("Skipping url: \"" + retrievedUrl + "\", at loading, as it has already be seen!");
			UrlUtils.logQuadruple(urlId, retrievedUrl, null, "duplicate", "Discarded in 'UrlUtils.handleUrlChecks()', as it's a duplicate.", null);
			if ( !useIdUrlPairs )
				inputDuplicatesNum ++;
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
	public static boolean isFinishedLoading(boolean isEmptyOfData, boolean isFirstRun) throws RuntimeException
	{
		if ( isEmptyOfData ) {
			if ( isFirstRun ) {
				String errorMessage = "Could not retrieve any urls from the inputFile!";
				System.err.println(errorMessage);
				logger.error(errorMessage);
				throw new RuntimeException();
			} else {
				logger.debug("Done processing " + FileUtils.getCurrentlyLoadedUrls() + " urls from the inputFile.");
				return true;
			}
		} else
			return false;
	}
	
	
	/**
	 * This method logs the remaining retrievedUrls which were not checked & connected.
	 * The method loadAndCheckIdUrlPairs() pics just one -the best- url from a group of urls belonging to a specific ID.
	 * The rest urls will either get rejected as problematic -and so get logged- or get skipped and be left non-logged.
	 * @param urlToCheck : It may be null, but that's ok, the "equals()"-comparison will return "false".
	 * @param retrievedId
	 * @param retrievedUrlsOfThisId
	 * @param loggedUrlsOfThisId
	 */
	private static void handleLogOfRemainingUrls(String urlToCheck, String retrievedId, Set<String> retrievedUrlsOfThisId, HashSet<String> loggedUrlsOfThisId)
	{
		for ( String retrievedUrl : retrievedUrlsOfThisId )
		{
			if ( !retrievedUrl.equals(urlToCheck) && !loggedUrlsOfThisId.contains(retrievedUrl) )
				UrlUtils.logQuadruple(retrievedId, retrievedUrl, null, "unreachable",
						"Skipped in LoaderAndChecker, as a better url was selected for id: " + retrievedId, null);
		}
	}
	
}
