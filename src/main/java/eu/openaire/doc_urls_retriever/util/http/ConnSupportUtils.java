package eu.openaire.doc_urls_retriever.util.http;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import eu.openaire.doc_urls_retriever.exceptions.DocFileNotRetrievedException;
import eu.openaire.doc_urls_retriever.exceptions.DomainBlockedException;
import eu.openaire.doc_urls_retriever.util.file.FileUtils;
import eu.openaire.doc_urls_retriever.util.url.UrlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.HashSet;


/**
 * @author Lampros A. Smyrnaios
 */
public class ConnSupportUtils
{
	private static final Logger logger = LoggerFactory.getLogger(ConnSupportUtils.class);
	
	public static final HashMap<String, Integer> timesDomainsReturned5XX = new HashMap<String, Integer>();	// Domains that have returned HTTP 5XX Error Code, and the amount of times they did.
	public static final HashMap<String, Integer> timesDomainsHadTimeoutEx = new HashMap<String, Integer>();
	public static final HashMap<String, Integer> timesPathsReturned403 = new HashMap<String, Integer>();
	
	public static final SetMultimap<String, String> domainsMultimapWithPaths403BlackListed = HashMultimap.create();	// Holds multiple values for any key, if a domain(key) has many different paths (values) for which there was a 403 errorCode.
	
	private static final int timesPathToHave403errorCodeBeforeBlocked = 3;
	private static final int timesToHave5XXerrorCodeBeforeBlocked = 10;
	private static final int timesToHaveTimeoutExBeforeBlocked = 25;
	private static final int numberOf403BlockedPathsBeforeBlocked = 5;
	
	
	/**
	 * This method first checks which "HTTP METHOD" was used to connect to the docUrl.
	 * If this docUrl was connected using "GET" (i.e. when this docURL was fast-found as a possibleDocUrl), just write the data to the disk.
	 * If it was connected using "HEAD", then, before we can store the data to the disk, we connect again, this time with "GET" in order to download the data.
	 * It returns the docFileName which was produced for this docUrl.
	 * @param conn
	 * @param domainStr
	 * @param docUrl
	 * @return
	 * @throws DocFileNotRetrievedException
	 */
	public static String downloadAndStoreDocFile(HttpURLConnection conn, String domainStr, String docUrl)
			throws DocFileNotRetrievedException
	{
		boolean reconnected = false;
		try {
			if ( conn.getRequestMethod().equals("HEAD") ) {    // If the connection happened with "HEAD" we have to re-connect with "GET" to download the docFile
				// No call of "conn.disconnect()" here, as we will connect to the same server.
				conn = HttpConnUtils.openHttpConnection(docUrl, domainStr, false, true);
				reconnected = true;
				int responceCode = conn.getResponseCode();    // It's already checked for -1 case (Invalid HTTP responce), inside openHttpConnection().
				if ( (responceCode < 200) || (responceCode >= 400) ) {    // If we have unwanted/error codes.
					onErrorStatusCode(conn.getURL().toString(), domainStr, responceCode);
					throw new DocFileNotRetrievedException();
				}
			}
			int contentSize = 0;
			try {
				contentSize = getContentSize(conn);
				if ( (contentSize == 0) || (contentSize > HttpConnUtils.maxAllowedContentSize) ) {
					logger.warn("DocUrl: \"" + docUrl + "\" had a non-acceptable contentSize: " + contentSize + ". The maxAllowed one is: " + HttpConnUtils.maxAllowedContentSize);
					throw new DocFileNotRetrievedException();
				}
			} catch (RuntimeException re) {
				logger.warn("No \"Content-Length\" was retrieved from docUrl: \"" + conn.getURL().toString() + "\"! We will store the docFile anyway..");
			} catch (DocFileNotRetrievedException dfnre) {
				throw dfnre;
			}
			
			// Write the downloaded bytes to the docFile and return the docFileName.
			return FileUtils.storeDocFile(conn.getInputStream(), docUrl, conn.getHeaderField("Content-Disposition"));
			
		} catch (DocFileNotRetrievedException dfnre ) {
			throw dfnre;
		} catch (Exception e) {
			logger.warn("", e);
			throw new DocFileNotRetrievedException();
		} finally {
			if ( reconnected )	// Otherwise the given-previous connection will be closed by the calling method.
				conn.disconnect();
		}
	}
	
	
	/**
	 * This method is called on errorStatusCode only. Meaning any status code not belonging in 2XX or 3XX.
	 * @param urlStr
	 * @param domainStr
	 * @param errorStatusCode
	 * @throws DomainBlockedException
	 */
	public static void onErrorStatusCode(String urlStr, String domainStr, int errorStatusCode) throws DomainBlockedException
	{
		if ( (errorStatusCode == 500) && domainStr.contains("handle.net") ) {    // Don't take the 500 of "handle.net", into consideration, it returns many times 500, where it should return 404.. so don't treat it like a 500.
			//logger.warn("\"handle.com\" returned 500 where it should return 404.. so we will treat it like a 404.");    // See an example: "https://hdl.handle.net/10655/10123".
			errorStatusCode = 404;	// Set it to 404 to be handled as such, if any rule for 404s is to be added later.
		}
		
		if ( (errorStatusCode >= 400) && (errorStatusCode <= 499) ) {	// Client Error.
			logger.warn("Url: \"" + urlStr + "\" seems to be unreachable. Recieved: HTTP " + errorStatusCode + " Client Error.");
			if ( errorStatusCode == 403 ) {
				if ( domainStr == null ) {
					if ( (domainStr = UrlUtils.getDomainStr(urlStr)) != null )
						on403ErrorCode(urlStr, domainStr);	// The "DomainBlockedException" will go up-method by its own, if thrown inside this one.
				} else
					on403ErrorCode(urlStr, domainStr);
			}
		}
		else {	// Other errorCodes. Retrieve the domain and make the required actions.
			domainStr = UrlUtils.getDomainStr(urlStr);
			
			if ( (errorStatusCode >= 500) && (errorStatusCode <= 599) ) {	// Server Error.
				logger.warn("Url: \"" + urlStr + "\" seems to be unreachable. Recieved: HTTP " + errorStatusCode + " Server Error.");
				if ( domainStr != null )
					on5XXerrorCode(domainStr);
			} else {	// Unknown Error (including non-handled: 1XX and the weird one: 999, responceCodes).
				logger.warn("Url: \"" + urlStr + "\" seems to be unreachable. Recieved unexpected responceCode: " + errorStatusCode);
				if ( domainStr != null )
					HttpConnUtils.blacklistedDomains.add(domainStr);
				
				throw new DomainBlockedException();	// Throw this even if there was an error preventing the domain from getting blocked.
			}
		}
	}
	
	
	/**
	 * This method handles the HTTP 403 Error Code.
	 * When a connection returns 403, we take the path of the url and we block it, as the directory which we are trying to connect to, is forbidden to be accessed.
	 * If a domain ends up having more paths blocked than a certain number, we block the whole domain itself.
	 * @param urlStr
	 * @param domainStr
	 * @throws DomainBlockedException
	 */
	public static void on403ErrorCode(String urlStr, String domainStr) throws DomainBlockedException
	{
		String pathStr = UrlUtils.getPathStr(urlStr);
		if ( pathStr == null )
			return;
		
		if ( countAndBlockPathAfterTimes(domainsMultimapWithPaths403BlackListed, timesPathsReturned403, pathStr, domainStr, timesPathToHave403errorCodeBeforeBlocked ) )
		{
			logger.debug("Path: \"" + pathStr + "\" of domain: \"" + domainStr + "\" was blocked after returning 403 Error Code.");
			
			// Block the whole domain if it has more than a certain number of blocked paths.
			if ( domainsMultimapWithPaths403BlackListed.get(domainStr).size() > numberOf403BlockedPathsBeforeBlocked )
			{
				HttpConnUtils.blacklistedDomains.add(domainStr);	// Block the whole domain itself.
				logger.debug("Domain: \"" + domainStr + "\" was blocked, after having more than " + numberOf403BlockedPathsBeforeBlocked + " of its paths 403blackListed.");
				domainsMultimapWithPaths403BlackListed.removeAll(domainStr);	// No need to keep its paths anymore.
				throw new DomainBlockedException();
			}
		}
	}
	
	
	public static boolean countAndBlockPathAfterTimes(SetMultimap<String, String> domainsWithPaths, HashMap<String, Integer> pathsWithTimes, String pathStr, String domainStr, int timesBeforeBlocked)
	{
		if ( countAndGetTimes(pathsWithTimes, pathStr) > timesBeforeBlocked ) {
			domainsWithPaths.put(domainStr, pathStr);	// Add this path in the list of blocked paths of this domain.
			pathsWithTimes.remove(pathStr);	// No need to keep the count for a blocked path.
			return true;
		}
		else
			return false;
	}
	
	
	/**
	 * This method check if there was ever a url from the given/current domain, which returned an HTTP 403 Eroor Code.
	 * If there was, it retrieves the directory path of the given/current url and checks if it caused an 403 Error Code before.
	 * It returns "true" if the given/current path is already blocked,
	 * otherwise, if it's not blocked, or if there was a problem retrieving this path from this url, it returns "false".
	 * @param urlStr
	 * @param domainStr
	 * @return boolean
	 */
	public static boolean checkIfPathIs403BlackListed(String urlStr, String domainStr)
	{
		if ( domainsMultimapWithPaths403BlackListed.containsKey(domainStr) )	// If this domain has returned 403 before, check if we have the same path.
		{
			String pathStr = UrlUtils.getPathStr(urlStr);
			if ( pathStr == null )	// If there is a problem retrieving this athStr, return false;
				return false;
			
			return domainsMultimapWithPaths403BlackListed.get(domainStr).contains(pathStr);
		}
		return false;
	}
	
	
	public static void on5XXerrorCode(String domainStr) throws DomainBlockedException
	{
		if ( countAndBlockDomainAfterTimes(HttpConnUtils.blacklistedDomains, timesDomainsReturned5XX, domainStr, timesToHave5XXerrorCodeBeforeBlocked) ) {
			logger.debug("Domain: \"" + domainStr + "\" was blocked after returning 5XX Error Code " + timesToHave5XXerrorCodeBeforeBlocked + " times.");
			throw new DomainBlockedException();
		}
	}
	
	
	public static void onTimeoutException(String domainStr) throws DomainBlockedException
	{
		if ( countAndBlockDomainAfterTimes(HttpConnUtils.blacklistedDomains, timesDomainsHadTimeoutEx, domainStr, timesToHaveTimeoutExBeforeBlocked) ) {
			logger.debug("Domain: \"" + domainStr + "\" was blocked after causing TimeoutException " + timesToHaveTimeoutExBeforeBlocked + " times.");
			throw new DomainBlockedException();
		}
	}
	
	
	/**
	 * This method handles domains which are reaching cases were they can be blocked.
	 * It calculates the times they did something and if they reached a red line, it adds them in the blackList provided by the caller.
	 * After adding it in the blackList, it removes its countings to free-up memory.
	 * It returns "true", if this domain was blocked, otherwise, "false".
	 * @param blackList
	 * @param domainsWithTimes
	 * @param domainStr
	 * @param timesBeforeBlock
	 * @return boolean
	 */
	public static boolean countAndBlockDomainAfterTimes(HashSet<String> blackList, HashMap<String, Integer> domainsWithTimes, String domainStr, int timesBeforeBlock)
	{
		if ( countAndGetTimes(domainsWithTimes, domainStr) > timesBeforeBlock ) {
			blackList.add(domainStr);    // Block this domain.
			domainsWithTimes.remove(domainStr);	// Remove counting-data.
			return true;	// This domain was blocked.
		}
		else
			return false;	// It wasn't blocked.
	}
	
	
	public static int countAndGetTimes(HashMap<String, Integer> itemWithTimes, String itemToCount)
	{
		int curTimes = 1;
		if ( itemWithTimes.containsKey(itemToCount) )
			curTimes += itemWithTimes.get(itemToCount);
		
		itemWithTimes.put(itemToCount, curTimes);
		
		return curTimes;
	}
	
	
	/**
	 * This method returns the ContentSize of the content of an HttpURLConnection.
	 * @param conn
	 * @return contentSize
	 * @throws RuntimeException
	 */
	public static int getContentSize(HttpURLConnection conn) throws RuntimeException
	{
		try {
			return Integer.parseInt(conn.getHeaderField("Content-Length"));
		} catch (NumberFormatException nfe) {
			//logger.warn("", nfe);
			throw new RuntimeException();
		}
	}
	
	
	public static void blockSharedSiteSessionDomain(String initialUrl, String pageDomain)
	{
		if ( pageDomain == null ) {
			if ( (pageDomain = UrlUtils.getDomainStr(initialUrl)) != null )
				HttpConnUtils.blacklistedDomains.add(pageDomain);
		} else
			HttpConnUtils.blacklistedDomains.add(pageDomain);
		
		logger.debug("Domain: \"" + pageDomain + "\" was blocked after trying to cause a \"sharedSiteSession-redirectionPack\"!");
	}
	
	
	/**
	 * This method print redirectStatistics if the initial url matched to the given wantedUrlType.
	 * It's intended to be used for debugging-only.
	 * @param initialUrl
	 * @param finalUrl
	 * @param wantedUrlType
	 * @param redirectsNum
	 */
	public static void printFinalRedirectDataForWantedUrlType(String initialUrl, String finalUrl, String wantedUrlType, int redirectsNum)
	{
		if ( (wantedUrlType != null) && initialUrl.contains(wantedUrlType) ) {
			logger.debug("\"" + initialUrl + "\" DID: " + redirectsNum + " redirect(s)!");
			logger.debug("Final link is: \"" + finalUrl + "\"");
		}
	}
	
}
