package eu.openaire.doc_urls_retriever.util.url;

import eu.openaire.doc_urls_retriever.crawler.MachineLearning;
import eu.openaire.doc_urls_retriever.util.file.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * @author Lampros Smyrnaios
 */
public class UrlUtils
{
	private static final Logger logger = LoggerFactory.getLogger(UrlUtils.class);
	
	public static final Pattern URL_TRIPLE = Pattern.compile("(.+://(?:ww(?:w|\\d)(?:(?:\\w+)?\\.)?)?([\\w.\\-]+)(?:[:\\d]+)?(?:.*/)?)(?:([^/^;^?]*)(?:(?:;|\\?)[^/^=]+(?:=.*)?)?)?");
	// URL_TRIPLE regex to group domain, path and ID --> group <1> is the regular PATH, group<2> is the DOMAIN and group <3> is the regular "ID".
	// TODO - Add explanation also for the uncaptured groups for better maintenance. For example the "ww(?:w|\d)" can capture "www", "ww2", "ww3" ect.
	
    public static final Pattern JSESSIONID_FILTER = Pattern.compile("(.+://.+)(?:;(?:JSESSIONID|jsessionid)=.+)(\\?.+)");
	
	public static int sumOfDocUrlsFound = 0;	// Change it back to simple int if finally in singleThread mode
	
	public static final HashSet<String> duplicateUrls = new HashSet<String>();
	
	public static final HashMap<String, String> docUrlsWithKeys = new HashMap<String, String>();	// Null keys are allowed (in case they are not available in the input).
	
	public static final String alreadyDownloadedByIDMessage = "This file is probably already downloaded from ID=";

	
	/**
     * This method logs the outputEntry to be written, as well as the docUrlPath (if non-empty String) and adds entries in the blackList.
	 * @param urlId (it may be null if no id was provided in the input)
	 * @param sourceUrl
	 * @param pageUrl
	 * @param DocUrl
	 * @param comment
	 * @param domain (it may be null)
	 */
    public static void logQuadruple(String urlId, String sourceUrl, String pageUrl, String DocUrl, String comment, String domain)
    {
        String finalDocUrl = DocUrl;

        if ( !finalDocUrl.equals("duplicate") )
        {
			if ( !finalDocUrl.equals("unreachable") ) {
				sumOfDocUrlsFound ++;

				// Remove "jsessionid" from urls for "cleaner" output.
				String lowerCaseUrl = finalDocUrl.toLowerCase();
				if ( lowerCaseUrl.contains("jsessionid") )
					finalDocUrl = UrlUtils.removeJsessionid(DocUrl);

				// Gather data for the MLA, if we decide to have it enabled.
				if ( MachineLearning.useMLA )
					MachineLearning.gatherMLData(domain, pageUrl, finalDocUrl);

				if ( !comment.contains(UrlUtils.alreadyDownloadedByIDMessage) )	// Add this id, only if this is a first-crossed docUrl.
					docUrlsWithKeys.put(finalDocUrl, urlId);	// Add it here, in order to be able to recognize it and quick-log it later, but also to distinguish it from other duplicates.
			}
			else	// Else if this url is not a docUrl and has not been processed before..
				duplicateUrls.add(sourceUrl);	// Add it in duplicates BlackList, in order not to be accessed for 2nd time in the future. We don't add docUrls here, as we want them to be separate for checking purposes.
		}

		FileUtils.quadrupleToBeLoggedList.add(new QuadrupleToBeLogged(urlId, sourceUrl, finalDocUrl, comment));	// Log it to be written later in the outputFile.
		
        if ( FileUtils.quadrupleToBeLoggedList.size() == FileUtils.jsonGroupSize )	// Write to file every time we have a group of <jsonGroupSize> quadruples.
            FileUtils.writeToFile();
    }
    
	
	/**
	 * This method returns the domain of the given url, in lowerCase (for better comparison).
	 * @param urlStr
	 * @return domainStr
	 */
	public static String getDomainStr(String urlStr)
	{
		if ( urlStr == null ) {	// Avoid NPE in "Matcher"
			logger.error("The received \"urlStr\" was null in \"getDomainStr()\"!");
			return null;
		}
		
		String domainStr = null;
		Matcher matcher = URL_TRIPLE.matcher(urlStr);
		if ( matcher.matches() ) {
			try {
				domainStr = matcher.group(2);	// Group <2> is the DOMAIN.
			} catch (Exception e) { logger.error("", e); }
			if ( (domainStr == null) || domainStr.isEmpty() ) {
				logger.warn("No domain was extracted from url: \"" + urlStr + "\".");
				return null;
			}
		} else {
			logger.error("Unexpected URL_TRIPLE's (" + matcher.toString() + ") mismatch for url: \"" + urlStr + "\"");
			return null;
		}
		
		return domainStr.toLowerCase();	// We return it in lowerCase as we don't want to store double domains. (it doesn't play any part in connectivity, only the rest of the url is case-sensitive.)
	}


	/**
	 * This method returns the path of the given url.
	 * @param urlStr
	 * @return pathStr
	 */
	public static String getPathStr(String urlStr)
	{
		if ( urlStr == null ) {	// Avoid NPE in "Matcher"
			logger.error("The received \"urlStr\" was null in \"getPathStr()\"!");
			return null;
		}
		
		String pathStr = null;
		Matcher matcher = URL_TRIPLE.matcher(urlStr);
		if ( matcher.matches() ) {
			try {
				pathStr = matcher.group(1);	// Group <1> is the PATH.
			} catch (Exception e) { logger.error("", e); }
			if ( (pathStr == null) || pathStr.isEmpty() ) {
				logger.warn("No pathStr was extracted from url: \"" + urlStr + "\".");
				return null;
			}
		} else {
			logger.error("Unexpected URL_TRIPLE's (" + matcher.toString() + ") mismatch for url: \"" + urlStr + "\"");
			return null;
		}
		
		return pathStr;
	}
	
	
	/**
	 * This method returns the path of the given url.
	 * @param urlStr
	 * @return pathStr
	 */
	public static String getDocIdStr(String urlStr)
	{
		if ( urlStr == null ) {	// Avoid NPE in "Matcher"
			logger.error("The received \"urlStr\" was null in \"getDocIdStr()\"!");
			return null;
		}
		
		String docIdStr = null;
		Matcher matcher = URL_TRIPLE.matcher(urlStr);
		if ( matcher.matches() ) {
			try {
				docIdStr = matcher.group(3);	// Group <3> is the docId.
			} catch (Exception e) { logger.error("", e); }
			if ( (docIdStr == null) || docIdStr.isEmpty() ) {
				logger.warn("No docID was extracted from url: \"" + urlStr + "\".");
				return null;
			}
		}
		else {
			logger.error("Unexpected URL_TRIPLE's (" + matcher.toString() + ") mismatch for url: \"" + urlStr + "\"");
			return null;
		}
		
		return docIdStr;
	}
	
	
	/**
	 * This method is responsible for removing the "jsessionid" part of a url.
	 * If no jsessionId is found, then it returns the string it recieved.
	 * @param urlStr
	 * @return urlWithoutJsessionId
	 */
	public static String removeJsessionid(String urlStr)
	{
		if ( urlStr == null ) {	// Avoid NPE in "Matcher"
			logger.error("The received \"urlStr\" was null in \"removeJsessionid()\"!");
			return null;
		}
		
		String finalUrl = urlStr;
		
		String preJsessionidStr = null;
		String afterJsessionidStr = null;
		
		Matcher jsessionidMatcher = JSESSIONID_FILTER.matcher(urlStr);
		if ( jsessionidMatcher.matches() )
		{
			try {
				preJsessionidStr = jsessionidMatcher.group(1);	// Take only the 1st part of the urlStr, without the jsessionid.
			} catch (Exception e) { logger.error("", e); }
		    if ( (preJsessionidStr == null) || preJsessionidStr.isEmpty() ) {
		    	logger.warn("Unexpected null or empty value returned by \"jsessionidMatcher.group(1)\" for url: \"" + urlStr + "\"");
		    	return finalUrl;
		    }
		    finalUrl = preJsessionidStr;
		    
		    try {
		    	afterJsessionidStr = jsessionidMatcher.group(2);
			} catch (Exception e) { logger.error("", e); }
			if ( (afterJsessionidStr == null) || afterJsessionidStr.isEmpty() )
				return finalUrl;
			else
				return finalUrl + afterJsessionidStr;
		}
		else
			return finalUrl;
	}
	
}
