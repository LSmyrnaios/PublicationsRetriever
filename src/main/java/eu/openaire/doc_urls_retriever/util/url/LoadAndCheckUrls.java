package eu.openaire.doc_urls_retriever.util.url;

import com.google.common.collect.HashMultimap;
import eu.openaire.doc_urls_retriever.util.file.FileUtils;
import eu.openaire.doc_urls_retriever.util.http.ConnSupportUtils;
import eu.openaire.doc_urls_retriever.util.http.HttpConnUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Set;
import java.util.regex.Pattern;


/**
 * This class contains the "loadAndCheck" code for the URLs.
 * @author Lampros A. Smyrnaios
 */
public class LoadAndCheckUrls
{
	private static final Logger logger = LoggerFactory.getLogger(LoadAndCheckUrls.class);
	
	public static final Pattern DOC_URL_FILTER = Pattern.compile(".+(pdf|download|/doc|document|(?:/|[?]|&)file|/fulltext|attachment|/paper|viewfile|viewdoc|/get|cgi/viewcontent.cgi?).*");
	// "DOC_URL_FILTER" works for lowerCase Strings (we make sure they are in lowerCase before we check).
	// Note that we still need to check if it's an alive link and if it's actually a docUrl (though it's mimeType).
	
	public static int numOfIDs = 0;	// The number of IDs existing in the input.
	public static int connProblematicUrls = 0;	// Urls known to have connectivity problems, such as long conn-times etc.
	public static int inputDuplicatesNum = 0;
	
	/**
	 * This method loads the urls from the input file in memory and check their type.
	 * Then, the loaded urls will either reach the connection point, were they will be checked for a docMimeType or they will be send directly for crawling.
	 * @throws RuntimeException
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
				String lowerCaseUrl = retrievedUrl.toLowerCase();	// Only for string checking purposes, not supposed to reach any connection.
				
				if ( (retrievedUrl = handleUrlChecks(null, retrievedUrl, lowerCaseUrl)) == null )
					continue;
				
				boolean isPossibleDocUrl = false;
				if ( DOC_URL_FILTER.matcher(lowerCaseUrl).matches() )
					isPossibleDocUrl = true;
				
				try {
					HttpConnUtils.connectAndCheckMimeType(null, retrievedUrl, retrievedUrl, retrievedUrl, null, true, isPossibleDocUrl);
				} catch (Exception e) {
					UrlUtils.logQuadruple(null, retrievedUrl, null, "unreachable", "Discarded at loading time, due to connectivity problems.", null);
					connProblematicUrls ++;
				}
			}// end for-loop
		}// end while-loop
	}
	
	
	/**
	 * This method loads the id-url pairs from the input file, in memory and check them. TODO - Add/Optimize documentation.
	 * Then, the loaded urls will either reach the connection point, were they will be checked for a docMimeType or they will be send directly for crawling.
	 * @throws RuntimeException
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
			
			boolean goToNextId = false;
			
			Set<String> keys = loadedIdUrlPairs.keySet();
			
			//logger.debug("CurGroup IDs-size: " + keys.size());	// DEBUG!
			
			numOfIDs += keys.size();
			
			for ( String retrievedId : keys )
			{
				//logger.debug("ID: " + retrievedId);	// DEBUG!
				
				String possibleDocUrl = null;
				String bestNonDocUrl = null;	// Best-case url
				String nonDoiUrl = null;	// Url which is not a best case, but it's not a slow-doi url either.
				String neutralUrl = null;	// Just a neutral url.
				String urlToCheck = null;
				
				for ( String retrievedUrl : loadedIdUrlPairs.get(retrievedId) )
				{
					//logger.debug("     URL: " + retrievedUrl);	// DEBUG!
					
					String lowerCaseUrl = retrievedUrl.toLowerCase();    // Only for string checking purposes, not supposed to reach any connection.
					
					if ( (retrievedUrl = handleUrlChecks(retrievedId, retrievedUrl, lowerCaseUrl)) == null )
						continue;
					
					if ( UrlUtils.docUrls.contains(retrievedUrl) ) {	// If we got into an already-found docUrl, log it and return.
						logger.info("re-crossed docUrl found: <" + retrievedUrl + ">");
						if ( FileUtils.shouldDownloadDocFiles )
							UrlUtils.logQuadruple(retrievedId, retrievedUrl, retrievedUrl, retrievedUrl, "This file is probably already downloaded.", null);
						else
							UrlUtils.logQuadruple(retrievedId, retrievedUrl, retrievedUrl, retrievedUrl, "", null);
						goToNextId = true;
						break;
					}
					
					// Check if it's a possible-DocUrl, if so, this is the only url which will be checked from this group, unless there's a canonicalization problem.
					if ( DOC_URL_FILTER.matcher(lowerCaseUrl).matches() ) {
						//logger.debug("Possible docUrl: " + retrievedUrl);
						possibleDocUrl = retrievedUrl;
						break;	// This is the absolute-best-case, we go and connect directly.
					}
					
					// Add this rule, if we accept the slow "hdl.handle.net"
					if ( retrievedUrl.contains("/handle/") )	// If this url contains "/handle/" we know that it's a bestCaseUrl among urls from the domain "handle.net", which after redirects reaches the bestCaseUrl (containing "/handle/").
						bestNonDocUrl = retrievedUrl;	// We can't just connect here, as the next url might be a possibleDocUrl.
					
					if ( (bestNonDocUrl == null) && !retrievedUrl.contains("doi.org") )	// If no other preferable url is found, we should prefer the nonDOI-one, if present, as the DOI-urls have lots of redirections.
						nonDoiUrl = retrievedUrl;
					
					neutralUrl = retrievedUrl;	// If no special-goodCase-url is found, this one will be used.
				}// end-url-for-loop
				
				if ( goToNextId )	// If we found an already-retrieved docUrl.
					continue;
				
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
				else
					continue;	// To the next ID, as no acceptable url was found for the current one.
				
				try {	// Check if it's a docUrl, if not, it gets crawled.
					HttpConnUtils.connectAndCheckMimeType(retrievedId, urlToCheck, urlToCheck, urlToCheck, null, true, isPossibleDocUrl);
				} catch (Exception e) {
					UrlUtils.logQuadruple(retrievedId, urlToCheck, null, "unreachable", "Discarded at loading time, due to connectivity problems.", null);
					connProblematicUrls ++;
				}
			}// end id-for-loop
		}// end loading-while-loop
	}
	
	
	/**
	 * This method checks if the given url is either of unwantedType or if it's a duplicate in the input, while removing the potential jsessionid from the url.
	 * It returns the givenUrl without the jsessionidPart if this url is accepted for connection/crawling, otherwise, it returns "null".
	 * @param urlId
	 * @param retrievedUrl
	 * @param lowerCaseUrl
	 * @return the non-jsessionid-url-string / null for unwanted-duplicate-url
	 */
	public static String handleUrlChecks(String urlId, String retrievedUrl, String lowerCaseUrl)
	{
		String currentUrlDomain = UrlUtils.getDomainStr(retrievedUrl);
		if ( currentUrlDomain == null ) {    // If the domain is not found, it means that a serious problem exists with this docPage and we shouldn't crawl it.
			logger.warn("Problematic URL in \"UrlUtils.handleUrlChecks()\": \"" + retrievedUrl + "\"");
			UrlUtils.logQuadruple(urlId, retrievedUrl, null, retrievedUrl, "Discarded in 'UrlUtils.handleUrlChecks()' method, after the occurrence of a domain-retrieval error.", null);
			return null;
		}
		
		if ( HttpConnUtils.blacklistedDomains.contains(currentUrlDomain) ) {	// Check if it has been blackListed after running inner links' checks.
			logger.debug("We will avoid to connect to blackListed domain: \"" + currentUrlDomain + "\"");
			UrlUtils.logQuadruple(urlId, retrievedUrl, null, "unreachable", "Discarded in 'UrlUtils.handleUrlChecks()' method, as its domain was found blackListed.", null);
			return null;
		}
		
		if ( ConnSupportUtils.checkIfPathIs403BlackListed(retrievedUrl, currentUrlDomain) ) {
			logger.debug("Preventing reaching 403ErrorCode with url: \"" + retrievedUrl + "\"!");
			UrlUtils.logQuadruple(urlId, retrievedUrl, null, retrievedUrl, "Discarded in 'UrlUtils.handleUrlChecks()' as it had a blackListed urlPath.", null);
			return null;
		}
		
		if ( UrlTypeChecker.matchesUnwantedUrlType(urlId, retrievedUrl, lowerCaseUrl) )
			return null;	// The url-logging is happening inside this method (per urlType).
		
		// Remove "jsessionid" for urls. Most of them, if not all, will already be expired.
		if ( lowerCaseUrl.contains("jsessionid") )
			retrievedUrl = UrlUtils.removeJsessionid(retrievedUrl);
		
		// Check if it's a duplicate.
		if ( UrlUtils.duplicateUrls.contains(retrievedUrl) ) {
			logger.debug("Skipping url: \"" + retrievedUrl + "\", at loading, as it has already been seen!");
			LoadAndCheckUrls.inputDuplicatesNum ++;
			UrlUtils.logQuadruple(urlId, retrievedUrl, null, "duplicate", "Discarded in 'UrlUtils.handleUrlChecks()', as it's a duplicate.", null);
			return null;
		}
		
		// Handle the weird-case of: "ir.lib.u-ryukyu.ac.jp"
		// See: http://ir.lib.u-ryukyu.ac.jp/handle/123456789/8743
		// Note that this is NOT the case for all of the urls containing "/handle/123456789/".. but just for this domain.
		if ( retrievedUrl.contains("ir.lib.u-ryukyu.ac.jp") && retrievedUrl.contains("/handle/123456789/") ) {
			logger.debug("We will handle the weird case of \"" + retrievedUrl + "\".");
			return StringUtils.replace(retrievedUrl, "/123456789/", "/20.500.12000/");
		}
		
		return retrievedUrl;	// The calling method needs the non-jsessionid-string.
	}
	
	
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
	
}
