package eu.openaire.doc_urls_retriever.util.http;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import eu.openaire.doc_urls_retriever.crawler.PageCrawler;
import eu.openaire.doc_urls_retriever.exceptions.DocFileNotRetrievedException;
import eu.openaire.doc_urls_retriever.exceptions.DomainBlockedException;
import eu.openaire.doc_urls_retriever.exceptions.JavaScriptDocLinkFoundException;
import eu.openaire.doc_urls_retriever.util.file.FileUtils;
import eu.openaire.doc_urls_retriever.util.url.UrlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * @author Lampros A. Smyrnaios
 */
public class ConnSupportUtils
{
	private static final Logger logger = LoggerFactory.getLogger(ConnSupportUtils.class);
	
	private static StringBuilder strB = new StringBuilder(30000);	// We give an initial size to optimize performance.
	
	public static final Pattern MIME_TYPE_FILTER = Pattern.compile("(?:\\((?:\\')?)?([\\w]+\\/[\\w\\+\\-\\.]+).*");
	
	public static final HashMap<String, Integer> timesDomainsReturned5XX = new HashMap<String, Integer>();	// Domains that have returned HTTP 5XX Error Code, and the amount of times they did.
	public static final HashMap<String, Integer> timesDomainsHadTimeoutEx = new HashMap<String, Integer>();
	public static final HashMap<String, Integer> timesPathsReturned403 = new HashMap<String, Integer>();
	
	public static final SetMultimap<String, String> domainsMultimapWithPaths403BlackListed = HashMultimap.create();	// Holds multiple values for any key, if a domain(key) has many different paths (values) for which there was a 403 errorCode.
	
	private static final int timesPathToHave403errorCodeBeforeBlocked = 3;
	private static final int timesToHave5XXerrorCodeBeforeBlocked = 10;
	private static final int timesToHaveTimeoutExBeforeBlocked = 25;
	private static final int numberOf403BlockedPathsBeforeBlocked = 5;
	
	public static final HashSet<String> knownDocTypes = new HashSet<String>();
	static {
		logger.debug("Setting knownDocTypes. Currently testing only the \".pdf\" type.");
		knownDocTypes.add("application/pdf");	// For the moment we support only the pdf docType.
	}
	
	
	/**
	 * This method takes a url and its mimeType and checks if it's a document mimeType or not.
	 * @param urlStr
	 * @param mimeType
	 * @param contentDisposition
	 * @return boolean
	 */
	public static boolean hasDocMimeType(String urlStr, String mimeType, String contentDisposition, HttpURLConnection conn)
	{
		if ( mimeType != null )
		{
			if ( mimeType.contains("System.IO.FileInfo") ) {	// Check this out: "http://www.esocialsciences.org/Download/repecDownload.aspx?fname=Document110112009530.6423303.pdf&fcategory=Articles&AId=2279&fref=repec", ιt has: "System.IO.FileInfo".
				// In this case, we want first to try the "Content-Disposition", as it's more trustworthy. If that's not available, use the urlStr as the last resort.
				if ( conn != null )	// Just to be sure we avoid an NPE.
					contentDisposition = conn.getHeaderField("Content-Disposition");
				// The "contentDisposition" will be definitely "null", since "mimeType != null" and so, the "contentDisposition" will not have been retrieved.
				
				if ( (contentDisposition != null) && !contentDisposition.equals("attachment") )
					return	contentDisposition.contains("pdf");	// TODO - add more types as needed. Check: "http://www.esocialsciences.org/Download/repecDownload.aspx?qs=Uqn/rN48N8UOPcbSXUd2VFI+dpOD3MDPRfIL8B3DH+6L18eo/yEvpYEkgi9upp2t8kGzrjsWQHUl44vSn/l7Uc1SILR5pVtxv8VYECXSc8pKLF6QJn6MioA5dafPj/8GshHBvLyCex2df4aviMvImCZpwMHvKoPiO+4B7yHRb97u1IHg45E+Z6ai0Z/0vacWHoCsNT9O4FNZKMsSzen2Cw=="
				else
					return	urlStr.toLowerCase().contains("pdf");
			}
			
			String plainMimeType = mimeType;	// Make sure we don't cause any NPE later on..
			if ( mimeType.contains("charset") || mimeType.contains("name")
					|| mimeType.startsWith("(") )	// See: "https://www.mamsie.bbk.ac.uk/articles/10.16995/sim.138/galley/134/download/" -> "Content-Type: ('application/pdf', none)"
			{
				plainMimeType = getPlainMimeType(mimeType);
				
				if ( plainMimeType == null ) {    // If there was any error removing the charset, still try to save any docMimeType (currently pdf-only).
					logger.warn("Url with problematic mimeType was: " + urlStr);
					return	urlStr.toLowerCase().contains("pdf");
				}
			}
			
			//logger.debug("Url: " + urlStr);	// DEBUG!
			//logger.debug("PlainMimeType: " + plainMimeType);	// DEBUG!
			
			if ( knownDocTypes.contains(plainMimeType) )
				return true;
			else if ( plainMimeType.contains("application/octet-stream") || plainMimeType.contains("application/x-octet-stream")
					|| plainMimeType.contains("application/save") || plainMimeType.contains("application/force-download")
					|| plainMimeType.contains("unknown") )
			{	// TODO - Optimize the performance for this check, probably use a regex.
				contentDisposition = conn.getHeaderField("Content-Disposition");
				if ( (contentDisposition != null) && !contentDisposition.equals("attachment") )
					return	contentDisposition.contains("pdf");
				else
					return	urlStr.toLowerCase().contains("pdf");
			}
			else
				return false;
			// This is a special case. (see: "https://kb.iu.edu/d/agtj" for "octet" info.
			// and an example for "unknown" : "http://imagebank.osa.org/getExport.xqy?img=OG0kcC5vZS0yMy0xNy0yMjE0OS1nMDAy&xtype=pdf&article=oe-23-17-22149-g002")
			// TODO - When we will accept more docTypes, match it also against other docTypes, not just "pdf".
		}
		else if ( (contentDisposition != null) && !contentDisposition.equals("attachment") ) {	// If the mimeType was not retrieved, then try the "Content Disposition".
				// TODO - When we will accept more docTypes, match it also against other docTypes instead of just "pdf".
			return	contentDisposition.contains("pdf");
		}
		else {	// This is not expected to be reached. Keep it for method-reusability.
			logger.warn("No mimeType, nor Content-Disposition, were able to be retrieved for url: " + urlStr);
			return false;
		}
	}
	
	
	/**
	 * This method receives the mimeType and returns it without the "parentheses" ot the "charset" part.
	 * If there is any error, it returns null.
	 * @param mimeType
	 * @return charset-free mimeType
	 */
	public static String getPlainMimeType(String mimeType)
	{
		String plainMimeType = null;
		Matcher mimeMatcher = null;
		
		try {
			mimeMatcher = MIME_TYPE_FILTER.matcher(mimeType);
		} catch (NullPointerException npe) {	// There should never be an NPE...
			logger.debug("NPE was thrown after calling \"Matcher\" in \"getPlainMimeType()\" with \"null\" value!");
			return null;
		}
		
		if ( mimeMatcher.matches() ) {
			plainMimeType = mimeMatcher.group(1);
			if ( plainMimeType == null || plainMimeType.isEmpty() ) {
				logger.warn("Unexpected null or empty value returned by \"mimeMatcher.group(1)\" for mimeType: \"" + mimeType + "\".");
				return null;
			}
		} else {
			logger.warn("Unexpected MIME_TYPE_FILTER's (" + mimeMatcher.toString() + ") mismatch for mimeType: \"" + mimeType + "\"");
			return null;
		}
		
		return plainMimeType;
	}
	
	
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
			} catch (NumberFormatException nfe) {
				logger.warn("No \"Content-Length\" was retrieved from docUrl: \"" + conn.getURL().toString() + "\"! We will store the docFile anyway..");	// No action is needed.
			}
			
			// Write the downloaded bytes to the docFile and return the docFileName.
			return FileUtils.storeDocFile(conn.getInputStream(), docUrl, conn.getHeaderField("Content-Disposition"));
			
		} catch (DocFileNotRetrievedException dfnre ) {	// Catch it here, otherwise it will be caught as a general exception.
			throw dfnre;	// Avoid creating a new "DocFileNotRetrievedException" if it's already created. By doing this we have a better stack-trace if we decide to log it in the caller-method.
		} catch (Exception e) {
			logger.warn("", e);
			throw new DocFileNotRetrievedException();
		} finally {
			if ( reconnected )	// Otherwise the given-previous connection will be closed by the calling method.
				conn.disconnect();
		}
	}
	
	
	/**
	 * This method recieves a pageUrl which gave an HTTP-300-code and extracts an internalLink out of the multiple choices provided.
	 * @param conn
	 * @return
	 */
	public static String getInternalLinkFromHTTP300Page(HttpURLConnection conn)
	{
		try {
			return new ArrayList<>(PageCrawler.extractInternalLinksFromHtml(getHtmlString(conn))).get(0);
		} catch (JavaScriptDocLinkFoundException jsdlfe) {
			return jsdlfe.getMessage();	// Return the Javascript link.
		} catch (Exception e) {
			logger.error("", e);
			return null;
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
	
	
	public static void blockSharedSiteSessionDomain(String initialUrl, String pageDomain)
	{
		if ( pageDomain == null ) {
			if ( (pageDomain = UrlUtils.getDomainStr(initialUrl)) != null )
				HttpConnUtils.blacklistedDomains.add(pageDomain);
		} else
			HttpConnUtils.blacklistedDomains.add(pageDomain);
		
		logger.debug("Domain: \"" + pageDomain + "\" was blocked after trying to cause a \"sharedSiteSession-redirectionPack\"!");
	}
	
	
	public static String getHtmlString(HttpURLConnection conn) throws Exception
	{
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			
			String inputLine;
			while ( (inputLine = br.readLine()) != null ) {
				//logger.debug(inputLine);	// DEBUG!
				strB.append(inputLine);
			}
			br.close();
			
			return strB.toString();
			
		} catch (Exception e) {
			logger.error("", e);
			throw e;
		}
		finally {
			strB.setLength(0);	// Reset "StringBuilder" WITHOUT re-allocating.
		}
	}
	
	
	/**
	 * This method returns the ContentSize of the content of an HttpURLConnection.
	 * @param conn
	 * @return contentSize
	 * @throws NumberFormatException
	 */
	public static int getContentSize(HttpURLConnection conn) throws NumberFormatException
	{
		return Integer.parseInt(conn.getHeaderField("Content-Length"));
	}
	
	
	/**
	 * This method constructs fully-formed urls which are connection-ready, as the retrieved links may be relative-links.
	 * @param pageUrl
	 * @param currentLink
	 * @param URLTypeUrl
	 * @return
	 */
	public static String getFullyFormedUrl(String pageUrl, String currentLink, URL URLTypeUrl)
	{
		try {
			URL base = null;
			
			if ( URLTypeUrl != null )
				base = URLTypeUrl;
			else if ( pageUrl != null )
				base = new URL(pageUrl);
			// Else -> there will be an exception in the next command..
			
			return	new URL(base, currentLink).toString();	// Return the TargetUrl.
			
		} catch (Exception e) {
			logger.error("Error when producing fully-formedUrl for: " + currentLink);
			return null;
		}
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
