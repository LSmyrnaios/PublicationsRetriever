package eu.openaire.doc_urls_retriever.crawler;


import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import eu.openaire.doc_urls_retriever.util.http.HttpUtils;
import eu.openaire.doc_urls_retriever.util.url.UrlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.regex.Matcher;


/**
 * This class aims to provide a Machine Learning Algorithm (M.L.A.) with methods to gather important data, use tha data to guess a result and control the execution of the algorithm.
 * Disclaimer: This is still in experimental stage. Some domains cannot be supported.
 * @author Lampros A. Smyrnaios
 */
public class MachineLearning
{
	private static final Logger logger = LoggerFactory.getLogger(MachineLearning.class);
	
	public static boolean useMLA = true;	// Should we try the experimental-M.L.A.? This is intended to be like a "global switch", to use or not to use the MLA,  throughout the program's execution.
	public static SetMultimap<String, String> successDomainPathsMultiMap = HashMultimap.create();	// Holds multiple values for any key, if a domain(key) has many different paths (values) for doc links.
	public static long docUrlsFoundByMLA = 0;
	// If we later want to show statistics, we should take into account only the number of the urls to which the MLA was tested against, not all of the urls in the inputFile.
	
	private static float leastSuccessPercentageForMLA = 60;	// The percentage which we want, in order to continue running the MLA.
	private static int leastNumberOfUrlsToCheck = 1000;	// Least number of URLs to check before deciding if we should continue running it.
	private static int timesToGatherDataBeforeStarting = 10000;	// 5,000 urls.
	private static int urlsToWaitUntilRestartMLA = 100000;	// 100,000 urls
	private static long endOfSleepNumOfUrls = 0;
	private static long latestNumOfUrlsBeforePauseMLA = 0;
	private static boolean isInSleepMode = false;
	private static int timesGatheredData = 0;
	//private static long urlsCheckedWithMLA = 0;
	
	
	/**
	 * Initialize the Machine Learning Algorithm (MLA).
	 * It ensures that for small input (i.e. for testing purposes) the MLA can run properly.
	 */
	public MachineLearning()
	{
		long urlsInCrawler = CrawlerController.frontier.getNumberOfScheduledPages();
		logger.debug("Urls added in Crawler: " + urlsInCrawler);
		
		// For small input, make sure the least number of urls to check every time is no more than 10% of the input.
		int tenPercentOfUrlsInCrawler = (int)(10 * 100 / urlsInCrawler);
		if ( leastNumberOfUrlsToCheck > tenPercentOfUrlsInCrawler )
			leastNumberOfUrlsToCheck = tenPercentOfUrlsInCrawler;
		
		// For small input, make sure that we gather data for no more than 30% of the input, before starting the MLA.
		int twentyPercentOfUrlsInCrawler = (int)(20 * 100 / urlsInCrawler);
		if ( timesToGatherDataBeforeStarting > twentyPercentOfUrlsInCrawler )
			timesToGatherDataBeforeStarting = twentyPercentOfUrlsInCrawler;
		
		// For small input, make sure the MLA can restart at least one time.
		int thirtyPercentOfUrlsInCrawler = (int)(30 * 100 / urlsInCrawler);
		if ( urlsToWaitUntilRestartMLA > thirtyPercentOfUrlsInCrawler )
			urlsToWaitUntilRestartMLA = thirtyPercentOfUrlsInCrawler;
	}
	
	
	/**
	 * This method checks if we should keep running the MLA.
	 * Since the MLA is still experimental and it doesn't work on all domains, we take measures to stop running it, if it doesn't succeed.
	 * It returns "true", either when we don't have reached a specific testing number, or when the MLA was succesfull for most of the previous cases.
	 * It returns "false", when it still hasn't gathered sufficient data, or when the MLA failed to have a specific success-rate (which leads to "sleep-mode"), or if the MLA is already in "sleep-mode".
	 * @return true/false
	 */
	public static boolean shouldRunMLA()
	{
		if ( isInitialLearningPeriod() )
			return false;
		
		if ( isInSleepMode )	// If it's currently in sleepMode, check if it should restart.
		{
			endOfSleepNumOfUrls = latestNumOfUrlsBeforePauseMLA + urlsToWaitUntilRestartMLA;	// Update num of urls to reach before the "sleep period" ends.
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
		long nextBreakPoint = timesToGatherDataBeforeStarting + leastNumberOfUrlsToCheck + endOfSleepNumOfUrls;
		if ( PageCrawler.totalPagesReachedCrawling <= nextBreakPoint )
		{
			return true;	// Always continue in this case, as we don't have enough success-rate-data to decide otherwise.
		}
		else	// Decide depending on successPercentage for all of the urls which reached the crawler until now (this will be the case every time this is called, after we exceed the leastNumber)..
		{
			float curSuccessRate = (float)(docUrlsFoundByMLA * 100) / PageCrawler.totalPagesReachedCrawling;
			//logger.debug("CurSuccessRate of MLA = " + curSuccessRate);
			
			if ( curSuccessRate >= leastSuccessPercentageForMLA )	// After the safe-period, continue as long as the success-rate is high.
				return true;
			else {
				logger.debug("MLA's success rate is lower than the satisfying one (" + leastSuccessPercentageForMLA + "). Entering \"sleepMode\"...");
				latestNumOfUrlsBeforePauseMLA = PageCrawler.totalPagesReachedCrawling;
				isInSleepMode = true;
				return false;
			}
		}
	}
	
	
	public static boolean isInitialLearningPeriod()
	{
		// If we haven't yet reached the wanted amount of data gathered.
		if ( timesGatheredData <= timesToGatherDataBeforeStarting )
			return true;
		else
			return false;
	}
	
	
	public static boolean shouldRestartMLA()
	{
		if ( PageCrawler.totalPagesReachedCrawling > endOfSleepNumOfUrls )
			return true;
		else
			return false;
	}
	
	
	/**
	 * This method gathers domain and path data, for succesfull docUrl-found-cases.
	 * This data is used by "UrlUtils.guessInnerDocUrl()" M.L.A. (Machine Learning Algorithm).
	 * @param urlStr
	 */
	public static void gatherMLData(String urlStr)
	{
		// Get its domain and path and put it inside "successDomainPathsMultiMap".
		String domainStr = null;
		String pathStr = null;
		
		Matcher matcher = UrlUtils.URL_TRIPLE.matcher(urlStr);
		if ( matcher.matches() )
		{
			domainStr = matcher.group(2);	// Group <2> is the DOMAIN.
			if ( (domainStr == null) || domainStr.isEmpty() ) {
				logger.warn("Unexpected null or empty value returned by \"URL_TRIPLE.group(2)\"");
				return;
			}
			
			pathStr = matcher.group(1);	// group <1> is the PATH.
			if ( (pathStr == null) || pathStr.isEmpty() ) {
				logger.warn("Unexpected null or empty value returned by \"URL_TRIPLE.group(1)\"");
				return;
			}
			
			MachineLearning.timesGatheredData ++;
			
			successDomainPathsMultiMap.put(domainStr.toLowerCase(), pathStr);	// Add this pair in "successDomainPathsMultiMap", if the key already exists then it will just add one more value to that key.
		}
		else {
			logger.warn("Unexpected matcher's (" + matcher.toString() + ") mismatch for url: \"" + urlStr + "\"");
		}
	}
	
	
	/**
	 * This method implements an M.L.A. (Machine Learning Algorithm), which uses previous success cases to predict the docUrl of a page, if this page gives us the ID of the document.
	 * The idea is that we might get a url which shows info about the publication and as the same ID with the wanted docUrl, but ut just happens to be in a different directory (path).
	 * So, before going and checking each and every one of the inner links, we should check if by using known paths that gave docUrls before (for the current spesific domain), we are able to take the docUrl immediately.
	 * Disclaimer: This is still in experimental stage.
	 * @param pageUrl
	 * @return true / false
	 * @throws RuntimeException
	 */
	public static boolean guessInnerDocUrlUsingML(String pageUrl, String domainStr)
	{
		Collection<String> knownPaths;
		StringBuilder strB = new StringBuilder(150);
		String guessedDocUrl = null;
		String docIdStr = null;
		
		if ( successDomainPathsMultiMap.containsKey(domainStr) )	// If this domain is already logged go check for previous succesfull paths, if not logged, then return..
		{
			Matcher matcher = UrlUtils.URL_TRIPLE.matcher(pageUrl);
			if ( matcher.matches() ) {
				docIdStr = matcher.group(3);	// group <3> is the "ID".
				if ( (docIdStr == null) || docIdStr.isEmpty() ) {
					logger.debug("No available ID information in url: \"" + pageUrl + "\"");
					return false;	// The docUrl can't be guessed in this case.
				}
			}
			else {
				logger.warn("Unexpected matcher's (" + matcher.toString() + ") mismatch for url: \"" + pageUrl + "\"");
				return false;	// We don't add the empty string here, as we are used to handle this in the calling classes.
			}
			
			//MachineLearning.urlsCheckedWithMLA ++;
			
			knownPaths = successDomainPathsMultiMap.get(domainStr);	// Get all available paths for this domain, to try them along with current ID.
			
			for ( String knownPath : knownPaths )
			{
				// For every available docPath for this domain construct the expected docLink..
				strB.append(knownPath);
				strB.append(docIdStr);
				strB.append(".pdf");
				
				guessedDocUrl = strB.toString();
				
				if ( UrlUtils.docUrls.contains(guessedDocUrl) ) {	// If we got into an already-found docUrl, log it and return true.
					UrlUtils.logUrl(pageUrl, guessedDocUrl, "");
					logger.debug("MachineLearningAlgorithm got a hit for: \""+ pageUrl + "\". Resulted docUrl was: \"" + guessedDocUrl + "\"" );	// DEBUG!
					MachineLearning.docUrlsFoundByMLA ++;
					return true;
				}
				
				// Check if it's a truly-alive docUrl.
				try {
					logger.debug("Going to check guessedDocUrl: " + guessedDocUrl +"\", made out from initialUrl: \"" + pageUrl + "\"");
					
					if ( HttpUtils.connectAndCheckMimeType(pageUrl, guessedDocUrl, domainStr) ) {
						logger.debug("MachineLearningAlgorithm got a hit for: \""+ pageUrl + "\". Resulted docUrl was: \"" + guessedDocUrl + "\"" );	// DEBUG!
						MachineLearning.docUrlsFoundByMLA ++;
						return true;	// Note that we have already add it in the output links inside "connectAndCheckMimeType()".
					}
					logger.debug("Not valid docUr after trying guessedDocUrl: " + guessedDocUrl + "\"");
				} catch (Exception e) {
					// No special handling here, neither logging.. since it's expected that some checks will fail.
				}
				
				strB.setLength(0);	// Clear the buffer before going to check the next path.
				
			}// end for-loop
		}// end if
		
		// If we reach here, it means that either there is not available data to guess the docUrl, or that all of the guesses have failed.
		
		return false;	// We can't find its docUrl.. so we return false and continue by crawling this page.
	}
	
}
