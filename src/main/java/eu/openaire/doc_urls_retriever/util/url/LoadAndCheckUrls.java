package eu.openaire.doc_urls_retriever.util.url;

import com.google.common.collect.HashMultimap;
import eu.openaire.doc_urls_retriever.util.file.FileUtils;
import eu.openaire.doc_urls_retriever.util.http.HttpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Set;


/**
 * This class contains the "loadAndCheck" code for the URLs.
 * @author Lampros A. Smyrnaios
 */
public class LoadAndCheckUrls
{
	private static final Logger logger = LoggerFactory.getLogger(LoadAndCheckUrls.class);
	
	public static int numOfIDs = 0;	// The number of IDs existing in the input.
	
	
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
				
				if ( (retrievedUrl = UrlUtils.handleUrlChecks(null, retrievedUrl, lowerCaseUrl)) == null )
					continue;
				
				boolean isPossibleDocUrl = false;
				if ( UrlUtils.DOC_URL_FILTER.matcher(lowerCaseUrl).matches() )
					isPossibleDocUrl = true;
				
				try {
					HttpUtils.connectAndCheckMimeType(null, retrievedUrl, retrievedUrl, retrievedUrl, null, true, isPossibleDocUrl);
				} catch (Exception e) {
					UrlUtils.logQuadruple(null, retrievedUrl, null, "unreachable", "Discarded at loading time, due to connectivity problems.", null);
					UrlUtils.connProblematicUrls ++;
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
				String bestUrl = null;	// Best-case url
				String nonDoiUrl = null;	// Url which is not a best case, but it's not a slow-doi url either.
				String neutralUrl = null;	// Just a neutral url.
				String urlToCheck = null;
				
				for ( String retrievedUrl : loadedIdUrlPairs.get(retrievedId) )
				{
					//logger.debug("     URL: " + retrievedUrl);	// DEBUG!
					
					String lowerCaseUrl = retrievedUrl.toLowerCase();    // Only for string checking purposes, not supposed to reach any connection.
					
					if ( (retrievedUrl = UrlUtils.handleUrlChecks(retrievedId, retrievedUrl, lowerCaseUrl)) == null )
						continue;
					
					if ( UrlUtils.docUrls.contains(retrievedUrl) ) {	// If we got into an already-found docUrl, log it and return.
						logger.info("re-crossed docUrl found: <" + urlToCheck + ">");
						if ( FileUtils.shouldDownloadDocFiles )
							UrlUtils.logQuadruple(retrievedId, retrievedUrl, retrievedUrl, retrievedUrl, "This file is probably already downloaded.", null);
						else
							UrlUtils.logQuadruple(retrievedId, retrievedUrl, retrievedUrl, retrievedUrl, "", null);
						goToNextId = true;
						break;
					}
					
					// Check if it's a possible-DocUrl, if so, this is the only url which will be checked from this group, unless there's a canonicalization problem.
					if ( UrlUtils.DOC_URL_FILTER.matcher(lowerCaseUrl).matches() )
					{
						//logger.debug("Possible docUrl: " + retrievedUrl);
						possibleDocUrl = retrievedUrl;
						break;
					}
					
					// Add this rule, if we accept the slow "hdl.handle.net"
					if ( retrievedUrl.contains("/handle/") )	// If this url contains "/handle/" we know that it's a bestCaseUrl among urls from the domain "handle.net", which after redirects reaches the bestCaseUrl (containing "/handle/").
						bestUrl = retrievedUrl;	// We can't just connect here, as the next url might be a possibleDocUrl.
					
					if ( (bestUrl == null) && !retrievedUrl.contains("doi.org") )	// If we find a nonDoiUrl keep it for possible later usage.
						nonDoiUrl = retrievedUrl;	// To be un-commented later.. if bestUrl-rules are added.
					
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
				else if ( bestUrl != null )
					urlToCheck = bestUrl;
				else if ( nonDoiUrl != null )
					urlToCheck = nonDoiUrl;
				else if ( neutralUrl != null )
					urlToCheck = neutralUrl;
				else
					continue;
				
				try {	// Check if it's a docUrl, if not, it gets crawled.
					HttpUtils.connectAndCheckMimeType(retrievedId, urlToCheck, urlToCheck, urlToCheck, null, true, isPossibleDocUrl);
				} catch (Exception e) {
					UrlUtils.logQuadruple(retrievedId, urlToCheck, null, "unreachable", "Discarded at loading time, due to connectivity problems.", null);
					UrlUtils.connProblematicUrls ++;
				}
			}// end id-for-loop
		}// end loading-while-loop
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
				logger.debug("Done processing " + FileUtils.getCurrentlyLoadedUrls() + " urls from the inputFile.");	// DEBUG!
				return true;
			}
		} else
			return false;
	}
	
}
