package eu.openaire.publications_retriever.crawler;


import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import eu.openaire.publications_retriever.PublicationsRetriever;
import eu.openaire.publications_retriever.util.file.FileUtils;
import eu.openaire.publications_retriever.util.http.ConnSupportUtils;
import eu.openaire.publications_retriever.util.http.HttpConnUtils;
import eu.openaire.publications_retriever.util.url.LoaderAndChecker;
import eu.openaire.publications_retriever.util.url.UrlTypeChecker;
import eu.openaire.publications_retriever.util.url.UrlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;


/**
 * This class aims to provide an online Machine Learning Algorithm (M.L.A.) with methods to gather important data, use tha data to predict a result and control the execution of the algorithm.
 * Disclaimer: This is still in experimental stage. Many domains are not supported.
 * @author Lampros Smyrnaios
 */
public class MachineLearning
{
	private static final Logger logger = LoggerFactory.getLogger(MachineLearning.class);

	public static final boolean useMLA = false;	// Should we try the experimental-M.L.A.? This is intended to be like a "global switch", to use or not to use the MLA, throughout the program's execution.

	private static final float leastSuccessPercentageForMLA = 51;	// The percentage which we want, in order to continue running the MLA.
	private static int latestMLADocUrlsFound = 0;

	private static int urlsToGatherBeforeStarting = 5000;	// 5,000 urls.
	private static int leastNumOfUrlsToCheckBeforeAccuracyTest = 1000;	// Least number of URLs to check before deciding if we should continue running it.
	private static int urlsToWaitUntilRestartMLA = 30000;	// 30,000 urls

	private static boolean mlaStarted = false;	// Useful to know when the MLA started to make predictions (left the learning mode).

	private static int endOfSleepNumOfUrls = 0;
	private static int latestSuccessBreakPoint = 0;
	private static int latestUrlsMLAChecked = 0;
	public static final AtomicInteger timesGatheredData = new AtomicInteger(0);	// Used also for statistics.
	private static final AtomicInteger pageUrlsCheckedWithMLA = new AtomicInteger(0);
	private static boolean isInSleepMode = false;

	public static AtomicInteger totalPagesReachedMLAStage = new AtomicInteger(0);	// This counts the pages which reached the crawlingStage, i.e: were not discarded in any case and waited to have their internalLinks checked.

	/**
	 * From the Docs: The multimap does not store duplicate key-value pairs. Adding a new key-value pair equal to an existing key-value pair has no effect.
	 */
	public static final SetMultimap<String, String> successPathsHashMultiMap = Multimaps.synchronizedSetMultimap(HashMultimap.create());	// Holds multiple values for any key, if a docPagePath(key) has many different docUrlPaths(values) for doc links.

	public static final Hashtable<String, String> successDocPathsExtensionHashMap = new Hashtable<String, String>();

	public static AtomicInteger docUrlsFoundByMLA = new AtomicInteger(0);
	// If we later want to show statistics, we should take into account only the number of the urls to which the MLA was tested against, not all the urls in the inputFile.

	private static final Set<String> domainsBlockedFromMLA = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
	static {	// These domains are not compatible with the MLA.
		domainsBlockedFromMLA.add("sciencedirect.com");
	}

	private static final Hashtable<String, Integer> timesDomainsFailedInMLA = new Hashtable<String, Integer>();
	private static final int timesToFailBeforeBlockedFromMLA = 10;

	private static final List<Double> successRateList = Collections.synchronizedList(new ArrayList<>());


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
	 * This method gathers docPagePath and docUrlPath data, for successful docUrl-found-cases.
	 * This data is used by "MachineLearning.predictInternalDocUrl()".
	 * @param docPage
	 * @param docUrl
	 * @param pageDomain (it may be null)
	 */
	public static void gatherMLData(String docPage, String docUrl, String pageDomain)
	{
		if ( docPage.equals(docUrl) )	// It will be equal if the "docPage" is a docUrl itself.
			return;	// No need to log anything.

		Matcher docPageMatcher = null;	// It's ok if it remains "null", the "getPathStr()" will create one then.

		if ( pageDomain == null ) {
			if ( (docPageMatcher = UrlUtils.getUrlMatcher(docPage)) == null )
				return;
			if ( (pageDomain = UrlUtils.getDomainStr(docPage, docPageMatcher)) == null )
				return;
		}

		if ( domainsBlockedFromMLA.contains(pageDomain) )	// Don't gather data for domains which are proven to not be compatible with the MLA.
			return;

		String docPagePath = UrlUtils.getPathStr(docPage, docPageMatcher);	// The "docPageMatcher" might be null, but it's ok.
		if ( docPagePath == null )
			return;

		// DocUrl part-extraction.
		Matcher docUrlMatcher = UrlUtils.getUrlMatcher(docUrl);
		if ( docUrlMatcher == null )
			return;

		String docUrlPath = UrlUtils.getPathStr(docUrl, docUrlMatcher);
		if ( docUrlPath == null )
			return;

		String docUrlID = UrlUtils.getDocIdStr(docUrl, docUrlMatcher);
		if ( docUrlID == null )
			return;

		// Take the Matcher to retrieve the extension and remove it from the docID, also keep it stored elsewhere so that we can use it in prediction later.
		Matcher extensionMatcher = FileUtils.EXTENSION_PATTERN.matcher(docUrlID);
		if ( extensionMatcher.find() ) {
			String extension = null;
			if ( (extension = extensionMatcher.group(0)) != null )	// Keep info about the docUrl, if it has a PDF-extension ending or not..
				MachineLearning.successDocPathsExtensionHashMap.put(docUrlPath, extension);	// If the map previously contained a mapping for the key, the old value is replaced.
			//logger.debug("extension: " + extension);	// DEBUG!
			//docUrlID = extensionMatcher.replaceAll("");	// Remove the extension.	TODO - Keep it here in case an idea comes up about using the docIDs (but they can be non-equal: <docPageId != docUrlID>).
		}

		// Get the paths of the docPage and the docUrl and put them inside "successDomainPathsMultiMap".
		MachineLearning.successPathsHashMultiMap.put(docPagePath, docUrlPath);	// Add this pair in "successPathsMultiMap", if the key already exists then it will just add one more value to that key.
		MachineLearning.timesGatheredData.incrementAndGet();
	}


	/**
	 * Compute the current success rate as follows: (<this round's found docUrls> * 100.0) / <this round's checked pageUrls>)
	 */
	public static double getCurrentSuccessRate()
	{
		return ((docUrlsFoundByMLA.get() - latestMLADocUrlsFound) * 100.0 / (pageUrlsCheckedWithMLA.get() - latestUrlsMLAChecked));
	}


	/**
	 * This method checks if we should continue running predictions using the MLA.
	 * Since the MLA is still experimental, and it doesn't work on all domains, we take measures to stop running it, if it doesn't succeed.
	 * It returns "true", either when we don't have reached a specific testing number, or when the MLA was successful for most of the previous cases.
	 * It returns "false", when it still hasn't gathered sufficient data, or when the MLA failed to have a specific success-rate (which leads to "sleep-mode"), or if the MLA is already in "sleep-mode".
	 * @return true/false
	 */
	public static synchronized boolean shouldRunPrediction()
	{
		// Check if it's initial learning period, in which the MLA should not run until it reaches a good learning point.
		if ( !mlaStarted ) {
			if ( timesGatheredData.get() <= urlsToGatherBeforeStarting ) {	// If we are at the starting point.
				latestSuccessBreakPoint = urlsToGatherBeforeStarting;
				return false;
			} else {	// If we are at the point about to start..
				mlaStarted = true;
				logger.info("Starting the MLA..");
			}
		}

		// If it's currently in sleepMode, check if it should restart.
		if ( isInSleepMode ) {
			if ( totalPagesReachedMLAStage.get() > endOfSleepNumOfUrls ) {
				logger.debug("MLA's \"sleepMode\" is finished, it will now restart.");
				isInSleepMode = false;
				return true;
			}
			else	// Continue sleeping.
				return false;
		}
		// Note that if it has never entered the sleepMode, the "endOfSleepNumOfUrls" will be 0.

		// If we reach here, it means that we are not in the "LearningPeriod", nor in "SleepMode".

		long nextBreakPointForSuccessRate = latestSuccessBreakPoint + leastNumOfUrlsToCheckBeforeAccuracyTest + endOfSleepNumOfUrls;
		//logger.debug("nextBreakPointForSuccessRate = " + nextBreakPointForSuccessRate);	// DEBUG!

		// Check if we should immediately continue running the MLA, or it's time to decide depending on the success-rate.
		if ( totalPagesReachedMLAStage.get() < nextBreakPointForSuccessRate )
			return true;	// Always continue in this case, as we don't have enough success-rate-data to decide otherwise.

		// Else decide depending on successPercentage for all of the urls which reached the "PageCrawler.visit()" in this round.
		double curSuccessRate = getCurrentSuccessRate();
		logger.debug("Breakpoint (urlNum=" + nextBreakPointForSuccessRate + ") reached. Current round's success rate of MLA = " + PublicationsRetriever.df.format(curSuccessRate) + "%");
		successRateList.add(curSuccessRate);	// Add it to the list in order to calculate the average in the end. It accepts duplicate values, which is important for the calculation of the average value: (10+10+20)/3 != (10+20)/2

		if ( curSuccessRate >= leastSuccessPercentageForMLA ) {    // After the safe-period, continue as long as the success-rate is high.
			endOfSleepNumOfUrls = 0;	// Stop keeping sleep-data.
			latestSuccessBreakPoint = totalPagesReachedMLAStage.get();	// The latest number for which MLA was tested against.
			return true;
		}

		// Else enter "sleep-mode" and update the variables.
		logger.debug("MLA's success-rate is lower than the satisfying one (" + leastSuccessPercentageForMLA + "). Entering \"sleep-mode\", but continuing to gather ML-data...");
		endOfSleepNumOfUrls = totalPagesReachedMLAStage.get() + urlsToWaitUntilRestartMLA;	// Update num of urls to reach before the "sleep period" ends.
		latestMLADocUrlsFound = docUrlsFoundByMLA.get();	// Keep latest num of docUrls found by the MLA, in order to calculate the success rate only for up-to-date data.
		latestUrlsMLAChecked = pageUrlsCheckedWithMLA.get();	// Keep latest num of urls checked by MLA...
		latestSuccessBreakPoint ++;	// Stop keeping successBreakPoint as we get in "sleepMode".
		isInSleepMode = true;
		return false;
	}


	/**
	 * This method tries to predict the docUrl of a page, if this page gives us the ID of the document, based on previous success cases.
	 * The idea is that we might get a url which shows info about the publication and has the same ID with the wanted docUrl, but it just happens to be in a different directory (path).
	 * So, before going and checking each and every one of the internal links, we should check if by using known docUrl-paths that gave docUrls before (for the current pageUrl-path), we are able to retrieve the docUrl immediately.
	 * @param urlId
	 * @param sourceUrl
	 * @param pageUrl
	 * @param pageDomain
	 * @return true / false
	 */
	public static boolean predictInternalDocUrl(String urlId, String sourceUrl, String pageUrl, String pageDomain, HashSet<String> currentPageLinks)
	{
		if ( domainsBlockedFromMLA.contains(pageDomain) ) {    // Check if this domain is not compatible with the MLA.
			logger.debug("Avoiding the MLA-prediction for incompatible domain: \"" + pageDomain + "\".");
			return false;
		}

		Matcher urlMatcher = UrlUtils.getUrlMatcher(pageUrl);
		if ( urlMatcher == null )
			return false;

		String pagePath = UrlUtils.getPathStr(pageUrl, urlMatcher);
		if ( pagePath == null )
			return false;

		// If the path can be handled, then go check for previous successful docUrls' paths.
		Collection<String> knownDocUrlPaths = successPathsHashMultiMap.get(pagePath);	// Get all available docUrlPaths for this docPagePath, to try them along with current ID. This returns an empty collection, if the pagePath is not there.
		int pathsSize = knownDocUrlPaths.size();
		if ( pathsSize == 0 )	// If this path cannot be handled by the MLA (no known data in our model), then return.
			return false;
		else if ( pathsSize > 5 ) {	// Too many docPaths for this pagePath, means that there's probably only one pagePath we get for this domain (paths are not mapped to domains, so we can't actually check).
			logger.warn("Domain: \"" + pageDomain + "\" was blocked from being accessed again by the MLA, after retrieving a proved-to-be incompatible pagePath (having more than 5 possible docUrl-paths).");
			domainsBlockedFromMLA.add(pageDomain);
			successPathsHashMultiMap.removeAll(pagePath);	// This domain was blocked, remove current non-needed paths-data.
			return false;
		}

		String docIdStr = UrlUtils.getDocIdStr(pageUrl, urlMatcher);
		if ( docIdStr == null )
			return false;
		else if ( UrlTypeChecker.PLAIN_PAGE_EXTENSION_FILTER.matcher(docIdStr.toLowerCase()).matches() )
		{	// The docID of this pageUrl contains a page-extension (not a document-one), which we don't want in the docUrl. Thus, we remove the extension from the end.
			docIdStr = FileUtils.EXTENSION_PATTERN.matcher(docIdStr).replaceAll("");	// This version of "replaceAll" uses a pre-compiled regex-pattern for better performance.
		}

		MachineLearning.pageUrlsCheckedWithMLA.incrementAndGet();

		String predictedDocUrl = null;
		String extension = null;

		StringBuilder strB = new StringBuilder(300);	// Initialize it here each time for thread-safety.

		for ( String knownDocUrlPath : knownDocUrlPaths )
		{
			// For every available docPath for this domain construct the expected docLink..
			strB.append(knownDocUrlPath).append(docIdStr);

			if ( (extension = successDocPathsExtensionHashMap.get(knownDocUrlPath)) != null )	// Check if a file-extension is registered for this docPath.
				strB.append(extension);

			predictedDocUrl = strB.toString();
			//logger.debug("Constructed \"predictedDocUrl\": " + predictedDocUrl);	// DEBUG!

			// Check if the "predictedDocUrl" exists inside the list of the internal-links, thus avoiding connecting with non-existed urls.
			// There should be no difference in the protocol, since the internal-links are made fully-formed-urls by using the base from the pageUrl.

			// TODO - There's the chance that the "predictedDocUrl" has HTTPS, but the corresponding internalLink has HTTP, so no match will be found.
			// TODO -We were about to make the check more "fair" then more overhead will be added: O(n) instead of O(1), while each check will be expensive.

			strB.setLength(0);	// Reset the buffer (the same space is still used, no reallocation is made).

			if ( !currentPageLinks.contains(predictedDocUrl) )
				continue;

			logger.debug("Found a \"predictedDocUrl\" which exists in the \"currentPageLinks\": " + predictedDocUrl);	// DEBUG!

			// Check if the "predictedDocUrl" has been found before, but only if it exists in the set of this page's internal-links, as we may end up with a "docUrl" which is not related with this pageUrl.
			if ( UrlUtils.docOrDatasetUrlsWithIDs.containsKey(predictedDocUrl) ) {	// If we got into an already-found docUrl, log it and return true.
				logger.info("MachineLearningAlgorithm got a hit for pageUrl: \""+ pageUrl + "\"! Resulted (already found before) docUrl was: \"" + predictedDocUrl + "\"" );	// DEBUG!
				ConnSupportUtils.handleReCrossedDocUrl(urlId, sourceUrl, pageUrl, predictedDocUrl, false);
				MachineLearning.docUrlsFoundByMLA.incrementAndGet();
				return true;
			}

			try {	// Check if it's a truly-alive docUrl.
				logger.debug("Going to connect & check predictedDocUrl: \"" + predictedDocUrl +"\", made out from pageUrl: \"" + pageUrl + "\"");	// DEBUG!
				if ( HttpConnUtils.connectAndCheckMimeType(urlId, sourceUrl, pageUrl, predictedDocUrl, null, false, true) ) {
					logger.info("MachineLearningAlgorithm got a hit for pageUrl: \""+ pageUrl + "\"! Resulted docUrl was: \"" + predictedDocUrl + "\"" );	// DEBUG!
					MachineLearning.docUrlsFoundByMLA.incrementAndGet();
					return true;	// Note that we have already added it in the output links inside "connectAndCheckMimeType()".
				}
				logger.debug("The predictedDocUrl was not a valid docUrl: \"" + predictedDocUrl + "\"");
			} catch (Exception e) {
				// No special handling here, neither logging.. since it's expected that some "predictedDocUrls" will fail. If it has a connection issue, it will be logged anyway.
			}
		}// end for-loop

		// If we reach here, it means that all of the predictions have failed.
		if ( ConnSupportUtils.countAndBlockDomainAfterTimes(domainsBlockedFromMLA, timesDomainsFailedInMLA, pageDomain, timesToFailBeforeBlockedFromMLA, false) ) {
			logger.warn("Domain: \"" + pageDomain + "\" was blocked from being accessed again by the MLA, after proved to be incompatible " + timesToFailBeforeBlockedFromMLA + " times.");

			// This domain was blocked, remove current non-needed paths-data. Note that we can't remove all of this domain's paths, since there is no mapping between a domain and its paths.
			for ( String docPath : successPathsHashMultiMap.get(pagePath) )
				successDocPathsExtensionHashMap.remove(docPath);
			successPathsHashMultiMap.removeAll(pagePath);
		}
		return false;	// We can't find its docUrl.. so we return false and continue by crawling this page.
	}


	public static double getAverageSuccessRate()
	{
		int sizeOfList = successRateList.size();
		if ( sizeOfList == 0 )
			return getCurrentSuccessRate();	// This is the "total success rate" now.

		double sumOfSuccessRates = 0.0;

		Collections.sort(successRateList);	//  This is needed as the precision of doubles might work against our "sum" if larger values are added before smaller ones, as noted here: https://stackoverflow.com/a/10516379
		for ( Double curSuccessRate : successRateList )
			sumOfSuccessRates += curSuccessRate;

		return (sumOfSuccessRates / sizeOfList);
	}


	/**
	 * Print the docPage-paths and their docUrl-paths.
	 * */
	public static void printGatheredData()
	{
		logger.debug("Here is the MLA data gathered throughout the program's execution:");
		Set<String> docPagePaths = successPathsHashMultiMap.keySet();
		logger.debug("Data was gathered and accepted for " + docPagePaths.size() + " docPagePaths:");
		for ( String docPagePath : docPagePaths )
		{
			logger.debug("\nDocPagePath: " + docPagePath + "\n\tdocUrlPaths:");
			for ( String docUrlPath : successPathsHashMultiMap.get(docPagePath) )
			{
				logger.debug("\tDocUrlPath: " + docUrlPath);
			}
		}
	}

}
