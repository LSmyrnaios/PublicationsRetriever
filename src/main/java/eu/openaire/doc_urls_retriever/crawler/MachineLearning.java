package eu.openaire.doc_urls_retriever.crawler;


import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import eu.openaire.doc_urls_retriever.util.http.HttpUtils;
import eu.openaire.doc_urls_retriever.util.url.UrlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Matcher;


/**
 * This class aims to provide a Machine Learning Algorithm (M.L.A.) with methods to gather important data, use tha data to guess a result and control the execution of the algorithm.
 * Disclaimer: This is still in experimental stage. Some domains cannot be supported.
 * @author Lampros A. Smyrnaios
 */
public class MachineLearning
{
	private static final Logger logger = LoggerFactory.getLogger(MachineLearning.class);
	
	public static final boolean useMLA = true;	// Should we try the experimental-M.L.A.? This is intended to be like a "global switch", to use or not to use the MLA,  throughout the program's execution.
	public static SetMultimap<String, String> successPathsMultiMap = HashMultimap.create();	// Holds multiple values for any key, if a docPagePath(key) has many different docUrlPaths(values) for doc links.
	public static long docUrlsFoundByMLA = 0;
	// If we later want to show statistics, we should take into account only the number of the urls to which the MLA was tested against, not all of the urls in the inputFile.
	
	private static long latestMLADocUrlsFound = 0;
	private static float leastSuccessPercentageForMLA = 60;	// The percentage which we want, in order to continue running the MLA.
	private static int leastNumberOfUrlsToCheck = 1000;	// Least number of URLs to check before deciding if we should continue running it.
	private static int timesToGatherDataBeforeStarting = 10000;	// 5,000 urls.
	private static int urlsToWaitUntilRestartMLA = 100000;	// 100,000 urls
	private static long endOfSleepNumOfUrls = 0;
	private static long latestNumOfUrlsBeforePauseMLA = 0;
	private static long latestSuccessBreakPoint = 0;
	private static long latestUrlsMLAChecked = 0;
	private static boolean isInSleepMode = false;
	private static int timesGatheredData = 0;
	private static long urlsCheckedWithMLA = 0;
	
	private static HashSet<String> domainsBlockedFromMLA = new HashSet<String>();
	private static HashMap<String, Integer> timesDomainsFailedInMLA = new HashMap<String, Integer>();
	private static int timesToFailBeforeBlockedFromMLA = 10;
	
	
	
	/**
	 * Initialize the Machine Learning Algorithm (MLA).
	 * It ensures that for small input (i.e. for testing purposes) the MLA can run properly.
	 */
	public MachineLearning()
	{
		long urlsInCrawler = CrawlerController.urlsReachedCrawler;
		
		// For small input, make sure the least number of urls to check every time is no more than 10% of the input.
		int tenPercentOfUrlsInCrawler = (int)(urlsInCrawler * 10 / 100);
		if ( leastNumberOfUrlsToCheck > tenPercentOfUrlsInCrawler )
			leastNumberOfUrlsToCheck = tenPercentOfUrlsInCrawler;
		
		// For small input, make sure that we gather data for no more than 20% of the input, before starting the MLA.
		int twentyPercentOfUrlsInCrawler = (int)(urlsInCrawler * 20 / 100);
		if ( timesToGatherDataBeforeStarting > twentyPercentOfUrlsInCrawler )
			timesToGatherDataBeforeStarting = twentyPercentOfUrlsInCrawler;
		
		// For small input, make sure the MLA can restart at least one time.
		int thirtyPercentOfUrlsInCrawler = (int)(urlsInCrawler * 30 / 100);
		if ( urlsToWaitUntilRestartMLA > thirtyPercentOfUrlsInCrawler )
			urlsToWaitUntilRestartMLA = thirtyPercentOfUrlsInCrawler;
	}
	
	
	/**
	 * This method checks if we should keep running the MLA.
	 * Since the MLA is still experimental and it doesn't work on all domains, we take measures to stop running it, if it doesn't succeed.
	 * It returns "true", either when we don't have reached a specific testing number, or when the MLA was succesfull for most of the previous cases.
	 * It returns "false", when it still hasn't gathered sufficient data, or when the MLA failed to have a specific success-rate (which leads to "sleep-mode"), or if the MLA is already in "sleep-mode".
	 * @param domainStr
	 * @return true/false
	 */
	public static boolean shouldRunMLA(String domainStr)
	{
		if ( domainsBlockedFromMLA.contains(domainStr) ) {    // Check if this domain is not compatible with the MLA.
			logger.debug("Avoiding the execution of the MLA for incompatible domain: \"" + domainStr + "\".");
			return false;
		}
		
		if ( isInitialLearningPeriod() ) {
			latestSuccessBreakPoint = timesToGatherDataBeforeStarting;
			return false;
		}
		
		if ( isInSleepMode )	// If it's currently in sleepMode, check if it should restart.
		{
			if ( shouldRestartMLA() ) {
				logger.debug("MLA's \"sleepMode\" is finished, it will now restart.");
				isInSleepMode = false;
				return true;
			}
			else
				return false;
		}
		// Note that if it has never entered the sleepMode, the "endOfSleepNumOfUrls" will be 0.
		
		// If we reach here, it means that we are not in the "LearningPeriod", nor in "SleepMode".
		
		// Check if we should immediately continue running the MLA, or it's time to decide depending on the success-rate.
		long nextBreakPoint = latestSuccessBreakPoint + leastNumberOfUrlsToCheck + endOfSleepNumOfUrls;
		if ( PageCrawler.totalPagesReachedCrawling <= nextBreakPoint )
		{
			return true;	// Always continue in this case, as we don't have enough success-rate-data to decide otherwise.
		}
		else	// Decide depending on successPercentage for all of the urls which reached the crawler until now (this will be the case every time this is called, after we exceed the leastNumber)..
		{
			float curSuccessRate = (float)((docUrlsFoundByMLA - latestMLADocUrlsFound) * 100) / (urlsCheckedWithMLA - latestUrlsMLAChecked);
			logger.debug("CurSuccessRate of MLA = " + curSuccessRate + "%");
			
			if ( curSuccessRate >= leastSuccessPercentageForMLA ) {    // After the safe-period, continue as long as the success-rate is high.
				endOfSleepNumOfUrls = 0;	// Stop keeping out-of-date sleep-data.
				latestSuccessBreakPoint = PageCrawler.totalPagesReachedCrawling -1;	// We use <-1>, as we want the latest number for which MLA was tested against.
				return true;
			}
			else {
				logger.debug("MLA's success-rate is lower than the satisfying one (" + leastSuccessPercentageForMLA + "). Entering \"sleepMode\"...");
				latestNumOfUrlsBeforePauseMLA = PageCrawler.totalPagesReachedCrawling -1;
				endOfSleepNumOfUrls = latestNumOfUrlsBeforePauseMLA + urlsToWaitUntilRestartMLA;	// Update num of urls to reach before the "sleep period" ends.
				latestMLADocUrlsFound = docUrlsFoundByMLA;	// Keep latest num of docUrls found by the MLA, in order to calculate the success rate only for up-to-date data.
				latestUrlsMLAChecked = urlsCheckedWithMLA;	// Keep latest num of urls checked by MLA...
				latestSuccessBreakPoint = 0;	// Stop keeping successBreakPoint.
				isInSleepMode = true;
				return false;
			}
		}
	}
	
	
	public static boolean isInitialLearningPeriod()
	{
		return (timesGatheredData <= timesToGatherDataBeforeStarting);
	}
	
	
	public static boolean shouldRestartMLA()
	{
		return (PageCrawler.totalPagesReachedCrawling > endOfSleepNumOfUrls);
	}
	
	
	/**
	 * This method gathers docPagePath and docUrlPath data, for succesfull docUrl-found-cases.
	 * This data is used by "UrlUtils.guessInnerDocUrl()".
	 * @param domain
	 * @param docPage
	 * @param docUrl
	 */
	public static void gatherMLData(String domain, String docPage, String docUrl)
	{
		if ( domainsBlockedFromMLA.contains(domain) )
			return;
		
		if ( docPage.equals(docUrl) )	// It will be equal if the "docPage" is a docUrl itself.
			return;	// No need to log anything.
		
		// Get the paths of the docPage and the docUrl and put them inside "successDomainPathsMultiMap".
		
		String docPagePath = UrlUtils.getPathStr(docPage);
		if ( docPagePath == null )
			return;
		
		String docUrlPath = UrlUtils.getPathStr(docUrl);
		if ( (docUrlPath == null) || docUrlPath.equals(docPagePath) )	// Avoid holding unnecessary/unwanted data.
			return;
		
		MachineLearning.successPathsMultiMap.put(docPagePath, docUrlPath);	// Add this pair in "successPathsMultiMap", if the key already exists then it will just add one more value to that key.
		
		MachineLearning.timesGatheredData ++;
	}
	
	
	/**
	 * This method implements an M.L.A. (Machine Learning Algorithm), which uses previous success cases to predict the docUrl of a page, if this page gives us the ID of the document.
	 * The idea is that we might get a url which shows info about the publication and as the same ID with the wanted docUrl, but ut just happens to be in a different directory (path).
	 * So, before going and checking each and every one of the inner links, we should check if by using known paths that gave docUrls before (for the current spesific domain), we are able to take the docUrl immediately.
	 * Disclaimer: This is still in experimental stage.
	 * @param pageUrl
	 * @param domainStr
	 * @return true / false
	 */
	public static boolean guessInnerDocUrlUsingML(String pageUrl, String domainStr)
	{
		String pagePath = null;
		Matcher urlMatcher = UrlUtils.URL_TRIPLE.matcher(pageUrl);
		if ( !urlMatcher.matches() ) {
			logger.warn("Unexpected URL_TRIPLE's (" + urlMatcher.toString() + ") mismatch for url: \"" + pageUrl + "\"");
			return false;
		}
		
		pagePath = urlMatcher.group(1);	// Group <1> is the PATH.
		if ( (pagePath == null) || pagePath.isEmpty() ) {
			logger.warn("Unexpected null or empty value returned by \"urlMatcher.group(1)\"");
			return false;
		}
		
		if ( successPathsMultiMap.containsKey(pagePath) )	// If this page's path is already logged, go check for previous succesfull docUrl's paths, if not logged, then return..
		{
			Collection<String> knownDocUrlPaths = successPathsMultiMap.get(pagePath);	// Get all available docUrlPaths for this docPagePath, to try them along with current ID.
			int pathsSize = knownDocUrlPaths.size();
			if ( pathsSize > 5 ) {	// Too many docPaths for this pagePath, means that there's probably only one pagePath we get for this domain (paths are not mapped to domains so we can't actually check).
				logger.debug("Domain: \"" + domainStr + "\" was blocked from being accessed again by the MLA, after retrieving a proved-to-be incompatible pagePath.");
				domainsBlockedFromMLA.add(domainStr);
				successPathsMultiMap.removeAll(pagePath);	// This domain was blocked, remove current non-needed paths-data.
				return false;
			}
			else if ( pathsSize > 3 )    // It's not worth risking connecting with more than 3 "guessedDocUrl"s, for which their success is non-granted.
				return false;    // The difference here is that we avoid making the connections but we leave the data as it is.. this way we allow whole domains to be blocked based on docPaths' size.
			
			String docIdStr = urlMatcher.group(3);	// Group <3> is the ID.
			if ( (docIdStr == null) || docIdStr.isEmpty() ) {
				logger.warn("Unexpected null or empty value returned by \"urlMatcher.group(3)\" for url: \"" + pageUrl + "\".");
				return false;
			}
			
			StringBuilder strB = new StringBuilder(150);
			String guessedDocUrl = null;
			
			MachineLearning.urlsCheckedWithMLA ++;
			
			for ( String knownDocUrlPath : knownDocUrlPaths )
			{
				// For every available docPath for this domain construct the expected docLink..
				strB.append(knownDocUrlPath);
				strB.append(docIdStr);
				strB.append(".pdf");
				
				guessedDocUrl = strB.toString();
				
				if ( UrlUtils.docUrls.contains(guessedDocUrl) ) {	// If we got into an already-found docUrl, log it and return true.
					UrlUtils.logTriple(pageUrl, guessedDocUrl, "", null);
					logger.debug("MachineLearningAlgorithm got a hit for: \""+ pageUrl + "\". Resulted docUrl was: \"" + guessedDocUrl + "\"" );	// DEBUG!
					MachineLearning.docUrlsFoundByMLA ++;
					return true;
				}
				
				// Check if it's a truly-alive docUrl.
				try {
					logger.debug("Going to check guessedDocUrl: " + guessedDocUrl +"\", made out from pageUrl: \"" + pageUrl + "\"");
					
					if ( HttpUtils.connectAndCheckMimeType(pageUrl, guessedDocUrl, null, false, true) ) {
						logger.debug("MachineLearningAlgorithm got a hit for: \""+ pageUrl + "\". Resulted docUrl was: \"" + guessedDocUrl + "\"" );	// DEBUG!
						MachineLearning.docUrlsFoundByMLA ++;
						return true;	// Note that we have already add it in the output links inside "connectAndCheckMimeType()".
					}
					logger.debug("Not valid docUrl after trying guessedDocUrl: " + guessedDocUrl + "\"");
				} catch (Exception e) {
					// No special handling here, neither logging.. since it's expected that some "guessedDocUrls" will fail.
				}
				
				strB.setLength(0);	// Clear the buffer before going to check the next path.
				
			}// end for-loop
			
			if ( HttpUtils.countAndBlockDomainAfterTimes(domainsBlockedFromMLA, timesDomainsFailedInMLA, domainStr, timesToFailBeforeBlockedFromMLA) )
			{
				logger.debug("Domain: \"" + domainStr + "\" was blocked from being accessed again by the MLA, after proved to be incompatible "
						+ timesToFailBeforeBlockedFromMLA + " times.");
				
				successPathsMultiMap.removeAll(pagePath);	// This domain was blocked, remove current non-needed paths-data. Note that we can't remove all of this domain's paths, since there is no mapping between a domain and its paths.
			}
			
		}// end if
		
		// If we reach here, it means that either there is not available data to guess the docUrl, or that all of the guesses have failed.
		
		return false;	// We can't find its docUrl.. so we return false and continue by crawling this page.
	}
	
}
