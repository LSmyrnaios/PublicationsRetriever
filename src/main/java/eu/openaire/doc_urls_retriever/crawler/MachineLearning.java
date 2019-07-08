package eu.openaire.doc_urls_retriever.crawler;


import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import eu.openaire.doc_urls_retriever.util.file.FileUtils;
import eu.openaire.doc_urls_retriever.util.http.ConnSupportUtils;
import eu.openaire.doc_urls_retriever.util.http.HttpConnUtils;
import eu.openaire.doc_urls_retriever.util.url.LoaderAndChecker;
import eu.openaire.doc_urls_retriever.util.url.UrlTypeChecker;
import eu.openaire.doc_urls_retriever.util.url.UrlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;


/**
 * This class aims to provide a Machine Learning Algorithm (M.L.A.) with methods to gather important data, use tha data to predict a result and control the execution of the algorithm.
 * Disclaimer: This is still in experimental stage. Many domains are not supported.
 * @author Lampros A. Smyrnaios
 */
public class MachineLearning
{
	private static final Logger logger = LoggerFactory.getLogger(MachineLearning.class);
	
	public static final boolean useMLA = false;	// Should we try the experimental-M.L.A.? This is intended to be like a "global switch", to use or not to use the MLA,  throughout the program's execution.
	
	private static final StringBuilder strB = new StringBuilder(200);
	
	private static int latestMLADocUrlsFound = 0;
	private static float leastSuccessPercentageForMLA = 51;	// The percentage which we want, in order to continue running the MLA.
	
	private static int urlsToGatherBeforeStarting = 5000;	// 5,000 urls.
	private static int leastNumOfUrlsToCheckBeforeAccuracyTest = 1000;	// Least number of URLs to check before deciding if we should continue running it.
	private static int urlsToWaitUntilRestartMLA = 30000;	// 30,000 urls
	
	private static boolean mlaStarted = false;	// Useful to know when the MLA started to make predictions (left the learning mode).
	
	private static int endOfSleepNumOfUrls = 0;
	private static int latestSuccessBreakPoint = 0;
	private static int latestUrlsMLAChecked = 0;
	private static int timesGatheredData = 0;
	private static int urlsCheckedWithMLA = 0;
	private static boolean isInSleepMode = false;
	
	/**
	 * From the Docs: The multimap does not store duplicate key-value pairs. Adding a new key-value pair equal to an existing key-value pair has no effect.
	 */
	public static final SetMultimap<String, String> successPathsMultiMap = HashMultimap.create();	// Holds multiple values for any key, if a docPagePath(key) has many different docUrlPaths(values) for doc links.
	
	public static int docUrlsFoundByMLA = 0;
	// If we later want to show statistics, we should take into account only the number of the urls to which the MLA was tested against, not all of the urls in the inputFile.
	
	private static final HashSet<String> domainsBlockedFromMLA = new HashSet<String>();
	private static final HashMap<String, Integer> timesDomainsFailedInMLA = new HashMap<String, Integer>();
	private static final int timesToFailBeforeBlockedFromMLA = 10;
	
	private static List<Float> successRateList = new ArrayList<>();
	
	/**
	 * Initialize the Machine Learning Algorithm (MLA).
	 * It ensures that for small input (i.e. for testing purposes) the MLA can run properly.
	 */
	public MachineLearning()
	{
		logger.debug("Initializing the MLA..");
		
		long approxNumOfTotalUrlsToCheck;
		
		if ( LoaderAndChecker.useIdUrlPairs )
			approxNumOfTotalUrlsToCheck = (long)(FileUtils.numOfLines * 0.7);	// Get the 70%, as the rest will be extra urls for the same id, along with failing urls.
		else
			approxNumOfTotalUrlsToCheck = (long)(FileUtils.numOfLines * 0.85);	// Get the 85%, as the rest will be failing urls.
		
		logger.debug("\"approxNumOfTotalUrlsToCheck\" = " + approxNumOfTotalUrlsToCheck);
		
		// For small input, make sure that we gather data for no more than 20% of the input, before starting the MLA.
		int tenPercentOfInput = (int)(approxNumOfTotalUrlsToCheck * 0.1);
		if ( urlsToGatherBeforeStarting > tenPercentOfInput )
			urlsToGatherBeforeStarting = tenPercentOfInput;
		
		logger.debug("\"urlsToGatherBeforeStarting\" = " + urlsToGatherBeforeStarting);
		
		// For small input, make sure the least number of urls to check every time is no more than 10% of the input.
		int fivePercentOfInput = (int)(approxNumOfTotalUrlsToCheck * 0.05);
		if ( leastNumOfUrlsToCheckBeforeAccuracyTest < fivePercentOfInput )
			leastNumOfUrlsToCheckBeforeAccuracyTest = fivePercentOfInput;
		
		logger.debug("\"leastNumOfUrlsToCheckBeforeAccuracyTest\" = " + leastNumOfUrlsToCheckBeforeAccuracyTest);
		
		// For small input, make sure the MLA can restart at least one time.
		int twentyPercentOfInput = (int)(approxNumOfTotalUrlsToCheck * 0.2);
		if ( urlsToWaitUntilRestartMLA > twentyPercentOfInput )
			urlsToWaitUntilRestartMLA = twentyPercentOfInput;
		
		logger.debug("\"urlsToWaitUntilRestartMLA\" = " + urlsToWaitUntilRestartMLA);
	}
	
	
	/**
	 * This method gathers docPagePath and docUrlPath data, for succesfull docUrl-found-cases.
	 * This data is used by "MachineLearning.predictInternalDocUrl()".
	 * @param domain
	 * @param docPage
	 * @param docUrl
	 */
	public static void gatherMLData(String domain, String docPage, String docUrl)
	{
		if ( domain == null )
			if ( (domain = UrlUtils.getDomainStr(docUrl)) == null )
				return;
		
		if ( domainsBlockedFromMLA.contains(domain) )	// Don't gather data for domains which are proven to not be compatible with the MLA.
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
	
	
	public static float getCurrentSuccessRate()
	{
		return (float)((docUrlsFoundByMLA - latestMLADocUrlsFound) * 100) / (urlsCheckedWithMLA - latestUrlsMLAChecked);
	}
	
	
	/**
	 * This method checks if we should continue running predictions using the MLA.
	 * Since the MLA is still experimental and it doesn't work on all domains, we take measures to stop running it, if it doesn't succeed.
	 * It returns "true", either when we don't have reached a specific testing number, or when the MLA was succesfull for most of the previous cases.
	 * It returns "false", when it still hasn't gathered sufficient data, or when the MLA failed to have a specific success-rate (which leads to "sleep-mode"), or if the MLA is already in "sleep-mode".
	 * @return true/false
	 */
	public static boolean shouldRunPrediction()
	{
		// Check if it's initial learning period, in which the MLA should not run until it reaches a good learning point.
		if ( timesGatheredData <= urlsToGatherBeforeStarting) {
			latestSuccessBreakPoint = urlsToGatherBeforeStarting;
			return false;
		}
		else if ( !mlaStarted && (timesGatheredData == (urlsToGatherBeforeStarting +1)) ) {
			mlaStarted = true;
			logger.info("Starting the MLA..");
		}
		
		// If it's currently in sleepMode, check if it should restart.
		if ( isInSleepMode ) {
			if ( PageCrawler.totalPagesReachedCrawling > endOfSleepNumOfUrls ) {
				logger.debug("MLA's \"sleepMode\" is finished, it will now restart.");
				isInSleepMode = false;
				return true;
			}
			else	// Continue sleeping.
				return false;
		}
		// Note that if it has never entered the sleepMode, the "endOfSleepNumOfUrls" will be 0.
		
		// If we reach here, it means that we are not in the "LearningPeriod", nor in "SleepMode".
		
		// Check if we should immediately continue running the MLA, or it's time to decide depending on the success-rate.
		long nextBreakPoint = latestSuccessBreakPoint + leastNumOfUrlsToCheckBeforeAccuracyTest + endOfSleepNumOfUrls;
		
		if ( PageCrawler.totalPagesReachedCrawling <= nextBreakPoint )
			return true;	// Always continue in this case, as we don't have enough success-rate-data to decide otherwise.
		
		// Else decide depending on successPercentage for all of the urls which reached the "PageCrawler.visit()" until now (this will be the case every time this is called, after we exceed the leastNumber)..
		
		float curSuccessRate = getCurrentSuccessRate();
		logger.debug("CurSuccessRate of MLA = " + curSuccessRate + "%");
		successRateList.add(curSuccessRate);	// Add it to the list in order to calculate the average in the end. It accepts duplicate values, which is important for the calculation of the average value.
		
		if ( curSuccessRate >= leastSuccessPercentageForMLA ) {    // After the safe-period, continue as long as the success-rate is high.
			endOfSleepNumOfUrls = 0;	// Stop keeping out-of-date sleep-data.
			latestSuccessBreakPoint = PageCrawler.totalPagesReachedCrawling -1;	// We use <-1>, as we want the latest number for which MLA was tested against.
			return true;
		}
		
		// Else don't run the MLA, but update the variables.
		logger.debug("MLA's success-rate is lower than the satisfying one (" + leastSuccessPercentageForMLA + "). Entering \"sleepMode\"...");
		int latestNumOfUrlsBeforePauseMLA = PageCrawler.totalPagesReachedCrawling -1;
		endOfSleepNumOfUrls = latestNumOfUrlsBeforePauseMLA + urlsToWaitUntilRestartMLA;	// Update num of urls to reach before the "sleep period" ends.
		latestMLADocUrlsFound = docUrlsFoundByMLA;	// Keep latest num of docUrls found by the MLA, in order to calculate the success rate only for up-to-date data.
		latestUrlsMLAChecked = urlsCheckedWithMLA;	// Keep latest num of urls checked by MLA...
		latestSuccessBreakPoint = 0;	// Stop keeping successBreakPoint.
		isInSleepMode = true;
		return false;
	}
	
	
	/**
	 * This method tries to predict the docUrl of a page, if this page gives us the ID of the document, based on previous success cases.
	 * The idea is that we might get a url which shows info about the publication and has the same ID with the wanted docUrl, but it just happens to be in a different directory (path).
	 * So, before going and checking each and every one of the internal links, we should check if by using known paths that gave docUrls before (for the current specific domain), we are able to retrieve the docUrl immediately.
	 * Note that we don't send the "domainStr" for the guessedDocUrls here, as at the moment an internal link might not be in the same "full-domain". We don't make use of TLDs at the moment. TODO - Investigate their potential.
	 * Disclaimer: This is still in experimental stage. The success-rate might not be high in some cases.
	 * @param urlId
	 * @param sourceUrl
	 * @param pageUrl
	 * @param domainStr
	 * @return true / false
	 */
	public static boolean predictInternalDocUrl(String urlId, String sourceUrl, String pageUrl, String domainStr)
	{
		if ( domainsBlockedFromMLA.contains(domainStr) ) {    // Check if this domain is not compatible with the MLA.
			logger.debug("Avoiding the MLA-prediction for incompatible domain: \"" + domainStr + "\".");
			return false;
		}
		
		Matcher urlMatcher = UrlUtils.URL_TRIPLE.matcher(pageUrl);
		if ( !urlMatcher.matches() ) {
			logger.warn("Unexpected URL_TRIPLE's (" + urlMatcher.toString() + ") mismatch for url: \"" + pageUrl + "\"");
			return false;
		}
		
		String pagePath = null;
		try {
			pagePath = urlMatcher.group(1);	// Group <1> is the PATH.
		} catch (Exception e) { logger.error("", e); return false; }
		if ( (pagePath == null) || pagePath.isEmpty() ) {
			logger.warn("Unexpected null or empty value returned by \"urlMatcher.group(1)\"");
			return false;
		}
		
		// If this page's path is already logged, go check for previous succesfull docUrl's paths, if not logged, then return..
		if ( !successPathsMultiMap.containsKey(pagePath) )
			return false;
		
		Collection<String> knownDocUrlPaths = successPathsMultiMap.get(pagePath);	// Get all available docUrlPaths for this docPagePath, to try them along with current ID.
		int pathsSize = knownDocUrlPaths.size();
		if ( pathsSize > 5 ) {	// Too many docPaths for this pagePath, means that there's probably only one pagePath we get for this domain (paths are not mapped to domains so we can't actually check).
			logger.debug("Domain: \"" + domainStr + "\" was blocked from being accessed again by the MLA, after retrieving a proved-to-be incompatible pagePath.");
			domainsBlockedFromMLA.add(domainStr);
			successPathsMultiMap.removeAll(pagePath);	// This domain was blocked, remove current non-needed paths-data.
			return false;
		}
		else if ( pathsSize > 3 )    // It's not worth risking connecting with more than 3 "predictedDocUrl"s, for which their success is non-granted.
			return false;    // The difference here is that we avoid making the connections but we leave the data as it is.. this way we allow whole domains to be blocked based on docPaths' size.
		
		String docIdStr = null;
		try {
			docIdStr = urlMatcher.group(3);	// Group <3> is the ID.
		} catch (Exception e) { logger.error("", e); return false; }
		if ( (docIdStr == null) || docIdStr.isEmpty() ) {
			logger.warn("Unexpected null or empty value returned by \"urlMatcher.group(3)\" for url: \"" + pageUrl + "\".");
			return false;
		}
		else if ( UrlTypeChecker.PLAIN_PAGE_EXTENSION_FILTER.matcher(docIdStr.toLowerCase()).matches() )
		{	// The docID of this pageUrl contains a page-extension, which we don't want in the docUrl. Thus, we remove the extension from the end.
			docIdStr = docIdStr.replaceAll("(\\.[^.]+)$", "");
		}
		
		MachineLearning.urlsCheckedWithMLA ++;
		
		String predictedDocUrl = null;
		String urlEnding = docIdStr + ".pdf";
		
		for ( String knownDocUrlPath : knownDocUrlPaths )
		{
			// For every available docPath for this domain construct the expected docLink..
			strB.append(knownDocUrlPath).append(urlEnding);
			predictedDocUrl = strB.toString();
			strB.setLength(0);	// Reset the buffer (the same space is still used, no reallocation is made).
			
			if ( UrlUtils.docUrlsWithKeys.containsKey(predictedDocUrl) ) {	// If we got into an already-found docUrl, log it and return true.
				logger.info("MachineLearningAlgorithm got a hit for pageUrl: \""+ pageUrl + "\"! Resulted (already found before) docUrl was: \"" + predictedDocUrl + "\"" );	// DEBUG!
				logger.info("re-crossed docUrl found: <" + predictedDocUrl + ">");
				if ( FileUtils.shouldDownloadDocFiles )
					UrlUtils.logQuadruple(urlId, sourceUrl, pageUrl, predictedDocUrl, UrlUtils.alreadyDownloadedByIDMessage + UrlUtils.docUrlsWithKeys.get(predictedDocUrl), null);
				else
					UrlUtils.logQuadruple(urlId, sourceUrl, pageUrl, predictedDocUrl, "", null);
				MachineLearning.docUrlsFoundByMLA ++;
				return true;
			}
			
			// Check if it's a truly-alive docUrl.
			try {
				logger.debug("Going to check predictedDocUrl: " + predictedDocUrl +"\", made out from pageUrl: \"" + pageUrl + "\"");
				
				if ( HttpConnUtils.connectAndCheckMimeType(urlId, sourceUrl, pageUrl, predictedDocUrl, null, false, true) ) {
					logger.info("MachineLearningAlgorithm got a hit for pageUrl: \""+ pageUrl + "\"! Resulted docUrl was: \"" + predictedDocUrl + "\"" );	// DEBUG!
					MachineLearning.docUrlsFoundByMLA ++;
					return true;	// Note that we have already add it in the output links inside "connectAndCheckMimeType()".
				}
				logger.debug("The predictedDocUrl was not a valid docUrl: \"" + predictedDocUrl + "\"");
			} catch (Exception e) {
				// No special handling here, neither logging.. since it's expected that some "predictedDocUrls" will fail.
			}
		}// end for-loop
		
		if ( ConnSupportUtils.countAndBlockDomainAfterTimes(domainsBlockedFromMLA, timesDomainsFailedInMLA, domainStr, timesToFailBeforeBlockedFromMLA) ) {
			logger.debug("Domain: \"" + domainStr + "\" was blocked from being accessed again by the MLA, after proved to be incompatible "
					+ timesToFailBeforeBlockedFromMLA + " times.");
			
			successPathsMultiMap.removeAll(pagePath);	// This domain was blocked, remove current non-needed paths-data. Note that we can't remove all of this domain's paths, since there is no mapping between a domain and its paths.
		}
		
		// If we reach here, it means that all of the predictions have failed.
		return false;	// We can't find its docUrl.. so we return false and continue by crawling this page.
	}
	
	
	public static float getAverageSuccessRate()
	{
		int sizeOfList = successRateList.size();
		
		if ( sizeOfList == 0 )
			return getCurrentSuccessRate();
		
		float sumOfSuccessRates = 0;
		
		for ( Float curSuccessRate : successRateList )
			sumOfSuccessRates += curSuccessRate;
		
		return (sumOfSuccessRates / sizeOfList);
	}
}
