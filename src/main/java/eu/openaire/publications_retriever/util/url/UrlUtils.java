package eu.openaire.publications_retriever.util.url;

import eu.openaire.publications_retriever.crawler.MachineLearning;
import eu.openaire.publications_retriever.util.file.FileUtils;
import eu.openaire.publications_retriever.util.file.IdUrlTuple;
import eu.openaire.publications_retriever.util.http.ConnSupportUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Hashtable;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * @author Lampros Smyrnaios
 */
public class UrlUtils
{
	private static final Logger logger = LoggerFactory.getLogger(UrlUtils.class);

	public static final Pattern URL_TRIPLE = Pattern.compile("([^/]+://(?:ww(?:w|\\d)(?:(?:\\w+)?\\.)?)?([\\w.\\-]+)(?:[:\\d]+)?(?:.*/)?)(?:([^/^;?]*)(?:[;?][^/^=]+(?:=.*)?)?)?");
	// URL_TRIPLE regex to group domain, path and ID --> group <1> is the regular PATH, group<2> is the DOMAIN and group <3> is the regular "ID".
	// TODO - Add explanation also for the non-captured groups for better maintenance. For example the "ww(?:w|\\d)" can capture "www", "ww2", "ww3" ect.

	public static final Pattern TEMPORAL_IDENTIFIER_FILTER = Pattern.compile("([^/]+://.+)(?:(?:(?i)(?:\\?|&|;|%3b)(?:.*token|jsessionid)(?:=|%3d))[^?&]+)([?&].+)?");	// Remove the token or the jsessionid (with case-insensitive) but keep the url-params in the end.
	// Simpler, non-encoding-aware regex: ([^/]+://.+)(?:(?:[?&;](?i)(?:.*token|jsessionid))=[^?&]+)([?&].+)?

	public static final Pattern ANCHOR_FILTER = Pattern.compile("(.+)(#(?!/).+)");	// Remove the anchor at the end of the url to avoid duplicate versions. (anchors might exist even in docUrls themselves)
	// Note that we may have this: https://academic.microsoft.com/#/detail/2945595536

	public static AtomicInteger sumOfDocUrlsFound = new AtomicInteger(0);	// Change it back to simple int if finally in singleThread mode

	public static final Set<String> duplicateUrls = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

	public static final Hashtable<String, IdUrlTuple> docOrDatasetUrlsWithIDs = new Hashtable<String, IdUrlTuple>();	// Null keys are allowed (in case they are not available in the input).

	public static final String alreadyDownloadedFromIDMessage = "This file is probably already downloaded from ID=";
	public static final String alreadyDownloadedFromSourceUrlContinuedMessage = " and SourceUrl=";

	public static final Hashtable<String, Integer> domainsAndHits = new Hashtable<>();
	// The data inside HashTable "domainsAndHits" is used to evaluate how good the domain is doing while is having some problems.

	public static final String duplicateUrlIndicator = "duplicate";
	public static final String unreachableDocOrDatasetUrlIndicator = "unreachable";



	/**
     * This method logs the outputEntry to be written, as well as the docUrlPath (if non-empty String) and adds entries in the blackList.
     * @param urlId (it may be null if no id was provided in the input)
     * @param sourceUrl
     * @param pageUrl
     * @param docUrl
     * @param comment
     * @param pageDomain (it may be null)
     * @param isFirstCrossed
     * @param wasUrlChecked
     * @param wasUrlValid
     * @param wasDocumentOrDatasetAccessible
     * @param wasDirectLink
     * @param couldRetry
     * @param fileSize
     * @param fileHash
     */
    public static void logOutputData(String urlId, String sourceUrl, String pageUrl, String docUrl, String comment, String pageDomain,
                                     boolean isFirstCrossed, String wasUrlChecked, String wasUrlValid, String wasDocumentOrDatasetAccessible, String wasDirectLink, String couldRetry, Long fileSize, String fileHash)
    {
        String finalDocUrl = docUrl;

        if ( !finalDocUrl.equals(duplicateUrlIndicator) )
        {
			if ( !finalDocUrl.equals(unreachableDocOrDatasetUrlIndicator) )
			{
				sumOfDocUrlsFound.incrementAndGet();

				// Remove the "temporalId" from urls for "cleaner" output and "already found docUrl"-matching. These IDs will expire eventually anyway.
				String lowerCaseUrl = finalDocUrl.toLowerCase();
				if ( lowerCaseUrl.contains("token") || lowerCaseUrl.contains("jsessionid") )
					finalDocUrl = UrlUtils.removeTemporalIdentifier(finalDocUrl);	// We send the non-lowerCase-url as we may want to continue with that docUrl in case of an error.

				if ( isFirstCrossed )	// Add this id, only if this is a first-crossed docUrl.
					docOrDatasetUrlsWithIDs.put(finalDocUrl, new IdUrlTuple(urlId, sourceUrl));	// Add it here, in order to be able to recognize it and quick-log it later, but also to distinguish it from other duplicates.

				if ( pageDomain == null )
					pageDomain = UrlUtils.getDomainStr(pageUrl, null);

				if ( pageDomain != null )	// It may be null if "UrlUtils.getDomainStr()" failed.
				{
					// Gather data for the MLA, if we decide to have it enabled.
					if ( MachineLearning.useMLA )
						MachineLearning.gatherMLData(pageUrl, finalDocUrl, pageDomain);

					// Add the domains of the pageUrl and the finalDocUrl to the successful domains as both lead in some way to a docUrl.
					// The data inside HashTable "domainsAndHits" is used to evaluate how good the domain is doing while is having some problems.
					// If the "goods" surpass the "bads", then that domain will not get blocked, even if the "minimum-accepted-bad-cases" was exceeded.
					ConnSupportUtils.countInsertAndGetTimes(domainsAndHits, pageDomain);

					// Now if the "finalDocUrl" is different than the "pageUrl", get the domain of the "finalDocUrl" and if it's different, then add it to "domainsAndHits"-HashMap.
					if ( !pageUrl.equals(finalDocUrl) ) {
						String docUrlDomain = UrlUtils.getDomainStr(finalDocUrl, null);
						if ( (docUrlDomain != null) && !docUrlDomain.equals(pageDomain) )
							ConnSupportUtils.countInsertAndGetTimes(domainsAndHits, docUrlDomain);
					}
				}
			}
			else	// Else if this url is not a docUrl and has not been processed before..
				duplicateUrls.add(sourceUrl);	// Add it in duplicates BlackList, in order not to be accessed for 2nd time in the future. We don't add docUrls here, as we want them to be separate for checking purposes.
		}

        FileUtils.dataToBeLoggedList.add(new DataToBeLogged(urlId, sourceUrl, finalDocUrl, wasUrlChecked, wasUrlValid, wasDocumentOrDatasetAccessible, wasDirectLink, couldRetry, fileHash, fileSize, comment));    // Log it to be written later in the outputFile.
    }


	/**
	 * This method returns the domain of the given url, in lowerCase (for better comparison).
	 * @param urlStr
	 * @param matcher
	 * @return domainStr
	 */
	public static String getDomainStr(String urlStr, Matcher matcher)
	{
		if ( matcher == null )
			if ( (matcher = getUrlMatcher(urlStr)) == null )
				return null;

		String domainStr = null;
		try {
			domainStr = matcher.group(2);	// Group <2> is the DOMAIN.
		} catch (Exception e) { logger.error("", e); return null; }
		if ( (domainStr == null) || domainStr.isEmpty() ) {
			logger.warn("No domain was extracted from url: \"" + urlStr + "\".");
			return null;
		}

		return domainStr.toLowerCase();	// We return it in lowerCase as we don't want to store double domains. (it doesn't play any part in connectivity, only the rest of the url is case-sensitive.)
	}


	/**
	 * This method returns the path of the given url.
	 * @param urlStr
	 * @param matcher
	 * @return pathStr
	 */
	public static String getPathStr(String urlStr, Matcher matcher)
	{
		if ( matcher == null )
			if ( (matcher = getUrlMatcher(urlStr)) == null )
				return null;

		String pathStr = null;
		try {
			pathStr = matcher.group(1);	// Group <1> is the PATH.
		} catch (Exception e) { logger.error("", e); return null; }
		if ( (pathStr == null) || pathStr.isEmpty() ) {
			logger.warn("No pathStr was extracted from url: \"" + urlStr + "\".");
			return null;
		}

		return pathStr;
	}


	/**
	 * This method returns the path of the given url.
	 * @param urlStr
	 * @param matcher
	 * @return pathStr
	 */
	public static String getDocIdStr(String urlStr, Matcher matcher)
	{
		if ( matcher == null )
			if ( (matcher = getUrlMatcher(urlStr)) == null )
				return null;

		String docIdStr = null;
		try {
			docIdStr = matcher.group(3);	// Group <3> is the docId.
		} catch (Exception e) { logger.error("", e); return null; }
		if ( (docIdStr == null) || docIdStr.isEmpty() ) {
			logger.warn("No docID was extracted from url: \"" + urlStr + "\".");
			return null;
		}

		return docIdStr;
	}


	public static Matcher getUrlMatcher(String urlStr)
	{
		if ( urlStr == null ) {	// Avoid NPE in "Matcher"
			logger.error("The received \"urlStr\" was null in \"getUrlMatcher()\"!");
			return null;
		}

		// If the url ends with "/" then remove it as its a "mistake" and the last part of it is the "docID" we want.
		if ( urlStr.endsWith("/") )
			urlStr = urlStr.substring(0, (urlStr.length() -1));
		Matcher urlMatcher = UrlUtils.URL_TRIPLE.matcher(urlStr);
		if ( urlMatcher.matches() )
			return urlMatcher;
		else {
			logger.warn("Unexpected URL_TRIPLE's (" + urlMatcher.toString() + ") mismatch for url: \"" + urlStr + "\"");
			return null;
		}
	}


	public static final Pattern TOP_THREE_LEVEL_DOMAIN_FILTER = Pattern.compile("[\\w-.]*?((?:[\\w-]+.)?[\\w-]+.[\\w-]+)$");

	/**
	 * This method received the domain as a parameter and tries to return only the top-three-level domain part.
	 * If the retrieval process fails, it returns the domain as it was given.
	 * @param domainStr
	 * @return the top-three-level-domain
	 */
	public static String getTopThreeLevelDomain(String domainStr)
	{
		Matcher matcher = TOP_THREE_LEVEL_DOMAIN_FILTER.matcher(domainStr);
		if ( matcher.matches() ) {
			try {
				domainStr = matcher.group(1);
			} catch (Exception e) {
				logger.warn("Could not find the group < 1 > when retrieving the top-three-level-domain from \"" + domainStr + "\"");
				return domainStr;	// It's the domain that was given as parameter.
			}
		} else
			logger.warn("Could not retrieve the top-three-level-domain from \"" + domainStr + "\"");

		return domainStr;
	}


	/**
	 * This method is responsible for removing the "temporalId" part of a url.
	 * If no temporalId is found, then it returns the string it received.
	 * @param urlStr
	 * @return urlWithoutTemporalId
	 */
	public static String removeTemporalIdentifier(String urlStr)
	{
		if ( urlStr == null ) {	// Avoid NPE in "Matcher"
			logger.error("The received \"urlStr\" was null in \"removeTemporalIdentifier()\"!");
			return "null";	// Avoid a potential NPE in the caller method, in case this method is used a non-controlled context.
		}

		Matcher temporalIdMatcher = TEMPORAL_IDENTIFIER_FILTER.matcher(urlStr);
		if ( !temporalIdMatcher.matches() )
			return urlStr;

		String preTemporalIdStr = null;
		String afterTemporalIdStr = null;

		try {
			preTemporalIdStr = temporalIdMatcher.group(1);	// Take only the 1st part of the urlStr, without the temporalId.
		} catch (Exception e) { logger.error("", e); return urlStr; }
		if ( (preTemporalIdStr == null) || preTemporalIdStr.isEmpty() ) {
			logger.warn("Unexpected null or empty value returned by \"temporalIdMatcher.group(1)\" for url: \"" + urlStr + "\"");
			return urlStr;
		}

		try {
			afterTemporalIdStr = temporalIdMatcher.group(2);
		} catch (Exception e) { logger.error("", e); return preTemporalIdStr; }
		if ( (afterTemporalIdStr == null) || afterTemporalIdStr.isEmpty() )
			return preTemporalIdStr;	// This is expected in many cases. The "afterTemporalIdStr" might not always exist.
		else {
			if ( afterTemporalIdStr.startsWith("&", 0) && !preTemporalIdStr.contains("?") )	// The "afterTemporalIdStr" should start with "?", if not (because the "token" was the first param) then..
				afterTemporalIdStr = StringUtils.replace(afterTemporalIdStr, "&", "?", 1);	// ..replace only the 1st matching character.

			return preTemporalIdStr + afterTemporalIdStr;
		}
	}


	/**
	 * This method removes the anchor part in the end of the URL, unless a "#" directory is detected.
	 * @param urlStr
	 * @return
	 */
	public static String removeAnchor(String urlStr)
	{
		if ( urlStr == null ) {	// Avoid NPE in "Matcher"
			logger.error("The received \"urlStr\" was null in \"removeAnchor()\"!");
			return null;
		}

		String noAnchorUrl = null;

		Matcher anchorMatcher = ANCHOR_FILTER.matcher(urlStr);
		if ( !anchorMatcher.matches() )
			return urlStr;

		try {
			noAnchorUrl = anchorMatcher.group(1);	// Take only the 1st part of the urlStr, without the anchor.
		} catch (Exception e) { logger.error("", e); return urlStr; }
		if ( (noAnchorUrl == null) || noAnchorUrl.isEmpty() ) {
			logger.warn("Unexpected null or empty value returned by \"anchorMatcher.group(1)\" for url: \"" + urlStr + "\"");
			return urlStr;
		}
		else
			return noAnchorUrl;
	}

}
