package eu.openaire.doc_urls_retriever.util.http;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import eu.openaire.doc_urls_retriever.exceptions.*;
import eu.openaire.doc_urls_retriever.crawler.PageCrawler;
import eu.openaire.doc_urls_retriever.util.file.FileUtils;
import eu.openaire.doc_urls_retriever.util.url.UrlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.net.*;
import java.util.HashMap;
import java.util.HashSet;


/**
 * @author Lampros A. Smyrnaios
 */
public class HttpUtils
{
	private static final Logger logger = LoggerFactory.getLogger(HttpUtils.class);
	
	public static final HashSet<String> domainsWithUnsupportedHeadMethod = new HashSet<String>();
	public static final HashSet<String> blacklistedDomains = new HashSet<String>();	// Domains with which we don't want to connect again.
	public static final HashMap<String, Integer> timesPathsReturned403 = new HashMap<String, Integer>();
	public static final HashMap<String, Integer> timesDomainsReturned5XX = new HashMap<String, Integer>();	// Domains that have returned HTTP 5XX Error Code, and the amount of times they did.
	public static final HashMap<String, Integer> timesDomainsHadTimeoutEx = new HashMap<String, Integer>();
	public static final HashMap<String, Integer> timesDomainsReturnedNoType = new HashMap<String, Integer>();	// Domain which returned no content-type not content disposition in their responce and amount of times they did.
	public static final HashMap<String, Integer> timesDomainsHadInputNotBeingDocNorPage = new HashMap<String, Integer>();
	public static final SetMultimap<String, String> domainsMultimapWithPaths403BlackListed = HashMultimap.create();	// Holds multiple values for any key, if a domain(key) has many different paths (values) for which there was a 403 errorCode.
	
	public static int domainsBlockedDueToSSLException = 0;
	
	public static String lastConnectedHost = "";
	public static final int politenessDelay = 0;	// Time to wait before connecting to the same host again.
	public static final int maxConnHEADWaitingTime = 10000;	// Max time (in ms) to wait for a connection, using "HTTP HEAD".
	public static final int maxConnGETWaitingTime = 15000;	// Max time (in ms) to wait for a connection, using "HTTP GET".
	
	private static final int maxRedirectsForPageUrls = 7;// The usual redirect times for doi.org urls is 3, though some of them can reach even 5 (if not more..)
	private static final int maxRedirectsForInnerLinks = 2;	// Inner-DOC-Links shouldn't take more than 2 redirects.
	
	private static final int timesPathToHave403errorCodeBeforeBlocked = 3;
	private static final int timesToHave5XXerrorCodeBeforeBlocked = 10;
    private static final int timesToHaveTimeoutExBeforeBlocked = 10;
    private static final int numberOf403BlockedPathsBeforeBlocked = 5;
    private static final int timesToReturnNoTypeBeforeBlocked = 10;
	private static final int timesToHaveNoDocNorPageInputBeforeBlocked = 10;
    
	public static final int maxAllowedContentSize = 1073741824;	// 1Gb
	private static final boolean shouldNOTacceptGETmethodForUncategorizedInnerLinks = true;
	
	
	/**
	 * This method checks if a certain url can give us its mimeType, as well as if this mimeType is a docMimeType.
	 * It automatically calls the "logUrl()" method for the valid docUrls, while it doesn't call it for non-success cases, thus allowing calling method to handle the case.
	 * @param urlId
	 * @param sourceUrl	// The inputUrl
	 * @param pageUrl	// May be the inputUrl or a redirected version of it.
	 * @param resourceURL	// May be the inputUrl or an innerLink of that inputUrl.
	 * @param domainStr
	 * @param calledForPageUrl
	 * @param calledForPossibleDocUrl
	 * @return "true", if it's a docMimeType, otherwise, "false", if it has a different mimeType.
	 * @throws RuntimeException (when there was a network error).
	 * @throws ConnTimeoutException
	 * @throws DomainBlockedException
	 * @throws DomainWithUnsupportedHEADmethodException
	 */
	public static boolean connectAndCheckMimeType(String urlId, String sourceUrl, String pageUrl, String resourceURL, String domainStr, boolean calledForPageUrl, boolean calledForPossibleDocUrl)
													throws RuntimeException, ConnTimeoutException, DomainBlockedException, DomainWithUnsupportedHEADmethodException
	{
		HttpURLConnection conn = null;
		try {
			if ( domainStr == null )	// No info about domainStr from the calling method.. we have to find it here.
				if ( (domainStr = UrlUtils.getDomainStr(resourceURL)) == null )
					throw new RuntimeException();	// The cause it's already logged inside "getDomainStr()".
			
			conn = handleConnection(urlId, sourceUrl, pageUrl, resourceURL, domainStr, calledForPageUrl, calledForPossibleDocUrl);
			
			// Check if we are able to find the mime type, if not then try "Content-Disposition".
			String mimeType = conn.getContentType();
			String contentDisposition = null;
			
			if ( mimeType == null ) {
				contentDisposition = conn.getHeaderField("Content-Disposition");
				if ( contentDisposition == null ) {
					logger.warn("No ContentType nor ContentDisposition, were able to be retrieved from url: " + conn.getURL().toString());
					if ( countAndBlockDomainAfterTimes(HttpUtils.blacklistedDomains, HttpUtils.timesDomainsReturnedNoType, domainStr, HttpUtils.timesToReturnNoTypeBeforeBlocked) ) {
						logger.warn("Domain: " + domainStr + " was blocked after returning no Type-info more than " + HttpUtils.timesToReturnNoTypeBeforeBlocked + " times.");
						throw new DomainBlockedException();
					}
					else
						throw new RuntimeException();	// We can't retrieve any clue. This is not desired.
				}
			}
			
			String finalUrlStr = conn.getURL().toString();
			
			//logger.debug("Url: " + finalUrlStr);	// DEBUG!
			//logger.debug("MimeType: " + mimeType);	// DEBUG!
			
			if ( UrlUtils.hasDocMimeType(finalUrlStr, mimeType, contentDisposition, conn) ) {
				logger.info("docUrl found: <" + finalUrlStr + ">");
				String fullPathFileName = "";
				if ( FileUtils.shouldDownloadDocFiles ) {
					try {
						fullPathFileName = downloadAndStoreDocFile(conn, domainStr, finalUrlStr);
						logger.info("DocFile: \"" + fullPathFileName + "\" has been downloaded.");
					} catch (DocFileNotRetrievedException dfnde) {
						fullPathFileName = "DocFileNotRetrievedException was thrown before the docFile could be stored.";
						logger.warn(fullPathFileName, dfnde);
					}
				}
				UrlUtils.logQuadruple(urlId, sourceUrl, pageUrl, finalUrlStr, fullPathFileName, domainStr);	// we send the urls, before and after potential redirections.
				return true;
			}
			else if ( calledForPageUrl ) {    // Visit this url only if this method was called for an inputUrl.
				if ( finalUrlStr.contains("viewcontent.cgi") ) {	// If this "viewcontent.cgi" isn't a docUrl, then don't check its innerLinks. Check this: "https://docs.lib.purdue.edu/cgi/viewcontent.cgi?referer=&httpsredir=1&params=/context/physics_articles/article/1964/type/native/&path_info="
					logger.warn("Unwanted pageUrl: \"" + finalUrlStr + "\" will not be visited!");
					UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "It was discarded in 'HttpUtils.connectAndCheckMimeType()', after matching to a non-docUrl with 'viewcontent.cgi'.", domainStr);
					return false;
				}
				else if ( (mimeType != null) && (mimeType.contains("htm") || mimeType.contains("text")) )	// The content-disposition is non-usable in the case of pages.. it's probably not provided anyway.
					PageCrawler.visit(urlId, sourceUrl, finalUrlStr, conn);
				else {
					logger.warn("Non-pageUrl: \"" + finalUrlStr + "\" with mimeType: \"" + mimeType + "\" will not be visited!");
					UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "It was discarded in 'HttpUtils.connectAndCheckMimeType()', after not matching to a docUrl nor to an htm/text-like page.", domainStr);
					if ( countAndBlockDomainAfterTimes(HttpUtils.blacklistedDomains, HttpUtils.timesDomainsHadInputNotBeingDocNorPage, domainStr, HttpUtils.timesToHaveNoDocNorPageInputBeforeBlocked) )
						logger.warn("Domain: " + domainStr + " was blocked after having no Doc nor Pages in the input more than " + HttpUtils.timesToReturnNoTypeBeforeBlocked + " times.");
				}	// We log the quadruple here, as there is connection-kind-of problem here.. it's just us considering it an unwanted case. We don't throw "DomainBlockedException()", as we don't handle it for inputUrls (it would also log the quadruple twice with diff comments).
			}
		} catch (AlreadyFoundDocUrlException afdue) {	// An already-found docUrl was discovered during redirections.
			return true;	// It's already logged for the outputFile.
		} catch (RuntimeException re) {
			if ( calledForPageUrl )	// Log this error only for docPages, not innerLinks.
				logger.warn("Could not handle connection for \"" + resourceURL + "\". MimeType not retrieved!");
			throw re;
		} catch (DomainBlockedException | DomainWithUnsupportedHEADmethodException | ConnTimeoutException e) {
			throw e;
		} catch (Exception e) {
			if ( calledForPageUrl )	// Log this error only for docPages.
				logger.warn("Could not handle connection for \"" + resourceURL + "\". MimeType not retrieved!");
			throw new RuntimeException();
		} finally {
			if ( conn != null )
				conn.disconnect();
		}
		
		return false;
	}

	
	public static HttpURLConnection handleConnection(String urlId, String sourceUrl, String pageUrl, String resourceURL, String domainStr, boolean calledForPageUrl, boolean calledForPossibleDocUrl)
										throws AlreadyFoundDocUrlException, RuntimeException, ConnTimeoutException, DomainBlockedException, DomainWithUnsupportedHEADmethodException, IOException
	{
		HttpURLConnection conn = HttpUtils.openHttpConnection(resourceURL, domainStr, calledForPageUrl, calledForPossibleDocUrl);
		
		int responceCode = conn.getResponseCode();	// It's already checked for -1 case (Invalid HTTP responce), inside openHttpConnection().
		if ( (responceCode >= 300) && (responceCode <= 399) ) {   // If we have redirections..
			conn = HttpUtils.handleRedirects(urlId, sourceUrl, pageUrl, resourceURL, conn, responceCode, domainStr, calledForPageUrl, calledForPossibleDocUrl);    // Take care of redirects.
		}
		else if ( (responceCode < 200) || (responceCode >= 400) ) {	// If we have error codes.
			onErrorStatusCode(resourceURL, domainStr, responceCode);
			throw new RuntimeException();	// This is only thrown if a "DomainBlockedException" is catched.
		}
		// Else it's an HTTP 2XX SUCCESS CODE.
		
		return conn;
	}
	

	/**
     * This method sets up a connection with the given url, using the "HEAD" method. If the server doesn't support "HEAD", it logs it, then it resets the connection and tries again using "GET".
     * The "domainStr" may be either null, if the calling method doesn't know this String (then openHttpConnection() finds it on its own), or an actual "domainStr" String.
	 * @param resourceURL
	 * @param domainStr
	 * @param calledForPageUrl
	 * @param calledForPossibleDocUrl
	 * @return HttpURLConnection
	 * @throws RuntimeException
	 * @throws ConnTimeoutException
	 * @throws DomainBlockedException
	 * @throws DomainWithUnsupportedHEADmethodException
     */
	public static HttpURLConnection openHttpConnection(String resourceURL, String domainStr, boolean calledForPageUrl, boolean calledForPossibleDocUrl)
									throws RuntimeException, ConnTimeoutException, DomainBlockedException, DomainWithUnsupportedHEADmethodException
    {
    	URL url = null;
		HttpURLConnection conn = null;
		int responceCode = 0;
		
		try {
			if ( blacklistedDomains.contains(domainStr) ) {
		    	logger.debug("Preventing connecting to blacklistedHost: \"" + domainStr + "\"!");
		    	throw new RuntimeException();
			}
			
			// Check whether we don't accept "GET" method for uncategorizedInnerLinks and if this url is such a case.
			if ( shouldNOTacceptGETmethodForUncategorizedInnerLinks
				&& !calledForPossibleDocUrl && domainsWithUnsupportedHeadMethod.contains(domainStr) )
				throw new DomainWithUnsupportedHEADmethodException();
			
			if ( checkIfPathIs403BlackListed(resourceURL, domainStr) ) {
				logger.debug("Preventing reaching 403ErrorCode with url: \"" + resourceURL + "\"!");
				throw new RuntimeException();
			}
			
			url = new URL(resourceURL);
			
			conn = (HttpURLConnection) url.openConnection();
			
			conn.setInstanceFollowRedirects(false);	// We manage redirects on our own, in order to control redirectsNum, as well as to avoid redirecting to unwantedUrls.
			
			if ( (calledForPageUrl && !calledForPossibleDocUrl)	// Either for just-webPages or for docUrls, we want to use "GET" in order to download the content.
					|| (calledForPossibleDocUrl && FileUtils.shouldDownloadDocFiles) ) {
				conn.setRequestMethod("GET");    // Go directly with "GET".
				conn.setConnectTimeout(HttpUtils.maxConnGETWaitingTime);
				conn.setReadTimeout(HttpUtils.maxConnGETWaitingTime);
			}
			else {
				conn.setRequestMethod("HEAD");    // Else, try "HEAD" (it may be either a domain that supports "HEAD", or a new domain, for which we have no info yet).
				conn.setConnectTimeout(maxConnHEADWaitingTime);
				conn.setReadTimeout(maxConnHEADWaitingTime);
			}
			
			if ( (politenessDelay > 0) && domainStr.contains(lastConnectedHost) )	// If this is the last-visited domain, sleep a bit before re-connecting to it.
				Thread.sleep(politenessDelay);	// Avoid server-overloading for the same host.
			
			conn.connect();	// Else, first connect and if there is no error, log this domain as the last one.
			lastConnectedHost = domainStr;
			
			if ( (responceCode = conn.getResponseCode()) == -1 ) {
				logger.warn("Invalid HTTP response for \"" + resourceURL + "\"");
				throw new RuntimeException();
			}
			
			if ( (responceCode == 405 || responceCode == 501) && conn.getRequestMethod().equals("HEAD") )	// If this SERVER doesn't support "HEAD" method or doesn't allow us to use it..
			{
				//logger.debug("HTTP \"HEAD\" method is not supported for: \"" + resourceURL +"\". Server's responceCode was: " + responceCode);
				
				// This domain doesn't support "HEAD" method, log it and then check if we can retry with "GET" or not.
				domainsWithUnsupportedHeadMethod.add(domainStr);
				
				if ( shouldNOTacceptGETmethodForUncategorizedInnerLinks && !calledForPossibleDocUrl )	// If we set not to retry with "GET" when we try uncategorizedInnerLinks, throw the related exception and stop the crawling of this page.
					throw new DomainWithUnsupportedHEADmethodException();
				
				// If we accept connection's retrying, using "GET", move on reconnecting.
				// No call of "conn.disconnect()" here, as we will connect to the same server.
				conn = (HttpURLConnection) url.openConnection();
				
				conn.setRequestMethod("GET");	// To reach here, it means that the HEAD method is unsupported.
				conn.setConnectTimeout(maxConnGETWaitingTime);
				conn.setReadTimeout(maxConnGETWaitingTime);
				conn.setInstanceFollowRedirects(false);
				
				if ( politenessDelay > 0 )
					Thread.sleep(politenessDelay);	// Avoid server-overloading for the same host.
				
				conn.connect();
				//logger.debug("ResponceCode for \"" + resourceURL + "\", after setting conn-method to: \"" + conn.getRequestMethod() + "\" is: " + conn.getResponseCode());
				
				if ( conn.getResponseCode() == -1 ) {	// Make sure we throw a RunEx on invalidHTTP.
					logger.warn("Invalid HTTP response for \"" + resourceURL + "\"");
					throw new RuntimeException();
				}
			}
		} catch (RuntimeException re) {    // The cause it's already logged.
			if ( conn != null )
				conn.disconnect();
			throw re;
		} catch (DomainWithUnsupportedHEADmethodException dwuhe) {
			throw dwuhe;
		} catch (UnknownHostException uhe) {
			logger.debug("A new \"Unknown Network\" Host was found and blacklisted: \"" + domainStr + "\"");
			if ( conn != null )
				conn.disconnect();
			blacklistedDomains.add(domainStr);    //Log it to never try connecting with it again.
			throw new DomainBlockedException();
		} catch (SocketTimeoutException ste) {
			logger.debug("Url: \"" + resourceURL + "\" failed to respond on time!");
			if ( conn != null )
				conn.disconnect();
			try { onTimeoutException(domainStr); }
			catch (DomainBlockedException dbe) { throw dbe; }
			throw new ConnTimeoutException();
		} catch (ConnectException ce) {
			if ( conn != null )
				conn.disconnect();
			String eMsg = ce.getMessage();
			if ( (eMsg != null) && eMsg.toLowerCase().contains("timeout") ) {    // If it's a "connection timeout" type of exception, treat it like it.
				try { onTimeoutException(domainStr); }
				catch (DomainBlockedException dbe) { throw dbe; }
				throw new ConnTimeoutException();
			}
			throw new RuntimeException();
		} catch (SSLException ssle) {
			logger.warn("No Secure connection was able to be negotiated with the domain: \"" + domainStr + "\".", ssle.getMessage());
			if ( conn != null )
				conn.disconnect();
			// TODO - For "SSLProtocolException", see more about it's possible handling here: https://stackoverflow.com/questions/7615645/ssl-handshake-alert-unrecognized-name-error-since-upgrade-to-java-1-7-0/14884941#14884941
			// TODO - Maybe I should make another list where only urls in https, from these domains, would be blocked.
			blacklistedDomains.add(domainStr);
			domainsBlockedDueToSSLException ++;
			throw new DomainBlockedException();
		} catch (SocketException se) {
			String seMsg = se.getMessage();
			if ( seMsg != null )
				logger.warn("\"" + se.getMessage() + "\". This SocketException was recieved after trying to connect with the domain: \"" + domainStr + "\"");
			if ( conn != null )
				conn.disconnect();
			blacklistedDomains.add(domainStr);
			throw new DomainBlockedException();
    	} catch (Exception e) {
			logger.error("", e);
			if ( conn != null )
				conn.disconnect();
			throw new RuntimeException();
		}
		
		return conn;
    }
    
	
    /**
     * This method takes an open connection for which there is a need for redirections.
     * It opens a new connection every time, up to the point we reach a certain number of redirections defined by "maxRedirects".
	 * @param urlId
	 * @param sourceUrl
	 * @param pageUrl
	 * @param innerLink
	 * @param conn
	 * @param responceCode
	 * @param domainStr
	 * @param calledForPageUrl
	 * @param calledForPossibleDocUrl
	 * @return Last open connection. If there was any problem, it returns "null".
	 * @throws AlreadyFoundDocUrlException
	 * @throws RuntimeException
	 * @throws ConnTimeoutException
	 * @throws DomainBlockedException
	 * @throws DomainWithUnsupportedHEADmethodException
	 */
	public static HttpURLConnection handleRedirects(String urlId, String sourceUrl, String pageUrl, String innerLink, HttpURLConnection conn, int responceCode, String domainStr, boolean calledForPageUrl, boolean calledForPossibleDocUrl)
																			throws AlreadyFoundDocUrlException, RuntimeException, ConnTimeoutException, DomainBlockedException, DomainWithUnsupportedHEADmethodException
	{
		int curRedirectsNum = 0;
		int maxRedirects = 0;
		String initialUrl = null;
		String urlType = null;	// Used for logging.
		
		if ( calledForPageUrl ) {
			maxRedirects = maxRedirectsForPageUrls;
			initialUrl = sourceUrl;// Keep initialUrl for logging and debugging.
			urlType = "pageUrl";
		} else {
			maxRedirects = maxRedirectsForInnerLinks;
			initialUrl = innerLink;
			urlType = "innerLink";
		}
		
		try {
			while ( true )
			{
				if ( responceCode >= 300 && responceCode <= 307 && responceCode != 306 && responceCode != 304 )    // Redirect code.
				{
					curRedirectsNum ++;
					if ( curRedirectsNum > maxRedirects ) {
						logger.debug("Redirects exceeded their limit (" + maxRedirects + ") for " + urlType + ": \"" + initialUrl + "\"");
						throw new RuntimeException();
					}
					
					String location = conn.getHeaderField("Location");
					if ( location == null ) {
						logger.warn("No \"Location\" field was found in the HTTP Header of \"" + conn.getURL().toString() + "\", after recieving an \"HTTP " + responceCode + "\" Redirect Code.");
						throw new RuntimeException();
					}
					
					String lowerCaseLocation = location.toLowerCase();
					if ( calledForPageUrl ) {
						if ( UrlUtils.shouldNotAcceptPageUrl(location, lowerCaseLocation) ) {
							logger.debug("Url: \"" + initialUrl + "\" was prevented to redirect to the unwanted url: \"" + location + "\", after recieving an \"HTTP " + responceCode + "\" Redirect Code.");
							throw new RuntimeException();
						}
					}
					else if ( PageCrawler.shouldNotAcceptInnerLink(location, lowerCaseLocation) ) {	// Else we are redirecting an innerPageLink.
						logger.debug("Url: \"" + initialUrl + "\" was prevented to redirect to the unwanted location: \"" + location + "\", after recieving an \"HTTP " + responceCode + "\" Redirect Code.");
						throw new RuntimeException();
					} else if ( lowerCaseLocation.contains("sharedsitesession") ) {    // either "getSharedSiteSession" or "consumeSharedSiteSession".
						HttpUtils.blockSharedSiteSessionDomain(initialUrl, domainStr);
						throw new DomainBlockedException();
					}
					
					String targetUrl = UrlUtils.getFullyFormedUrl(null, location, conn.getURL());
					if ( targetUrl == null )
						throw new RuntimeException();
					
					// FOR DEBUG -> Check to see what's happening with the redirect urls (location field types, as well as potential error redirects).
					// Some domains use only the target-ending-path in their location field, while others use full target url.
					//if ( conn.getURL().toString().contains("<urlType>") ) {	// Debug a certain domain.
						/*logger.debug("\n");
						logger.debug("Redirect(s) num: " + curRedirectsNum);
						logger.debug("Redirect code: " + conn.getResponseCode());
						logger.debug("Base: " + conn.getURL());
						logger.debug("Location: " + location);
						logger.debug("Target: " + targetUrl + "\n");*/
					//}
					
					if ( UrlUtils.docUrls.contains(targetUrl) ) {	// If we got into an already-found docUrl, log it and return.
						logger.info("re-crossed docUrl found: <" + targetUrl + ">");
						if ( FileUtils.shouldDownloadDocFiles )
							UrlUtils.logQuadruple(urlId, sourceUrl, pageUrl, targetUrl, "This file is probably already downloaded.", domainStr);
						else
							UrlUtils.logQuadruple(urlId, sourceUrl, pageUrl, targetUrl, "", domainStr);
						throw new AlreadyFoundDocUrlException();
					}
					/*else if ( calledForPageUrl && targetUrl.contains("elsevier.com") ) {	// Avoid pageUrls redirecting to "elsevier.com" (mostly "doi.org"-urls).
						logger.debug("Url: \"" + initialUrl + "\" was prevented to redirect to the unwanted url: \"" + targetUrl + "\", after recieving an \"HTTP " + responceCode + "\" Redirect Code.");
						throw new RuntimeException();
					}*/
					
					if ( !targetUrl.contains(HttpUtils.lastConnectedHost) )    // If the next page is not in the same domain as the "lastConnectedHost", we have to find the domain again inside "openHttpConnection()" method.
						if ( (domainStr = UrlUtils.getDomainStr(targetUrl)) == null )
							throw new RuntimeException();    // The cause it's already logged inside "getDomainStr()".
					
					conn.disconnect();
					conn = HttpUtils.openHttpConnection(targetUrl, domainStr, calledForPageUrl, calledForPossibleDocUrl);
					
					responceCode = conn.getResponseCode();    // It's already checked for -1 case (Invalid HTTP), inside openHttpConnection().
					
					if ( (responceCode >= 200) && (responceCode <= 299) ) {
						//printFinalRedirectDataForWantedUrlType(initialUrl, conn.getURL().toString(), null, curRedirectsNum);	// DEBUG!
						return conn;    // It's an "HTTP SUCCESS", return immediately.
					}
				} else {
					onErrorStatusCode(conn.getURL().toString(), domainStr, responceCode);
					throw new RuntimeException();    // This is not thrown if a "DomainBlockedException" was thrown first.
				}
			}//while-loop.
			
		} catch (AlreadyFoundDocUrlException | RuntimeException | ConnTimeoutException | DomainBlockedException | DomainWithUnsupportedHEADmethodException e) {    // We already logged the right messages.
			conn.disconnect();
			throw e;
		} catch (Exception e) {
			logger.warn("", e);
			conn.disconnect();
			throw new RuntimeException();
		}
	}
	
	
	public static void blockSharedSiteSessionDomain(String initialUrl, String pageDomain)
	{
		if ( pageDomain == null ) {
			if ( (pageDomain = UrlUtils.getDomainStr(initialUrl)) != null )
				HttpUtils.blacklistedDomains.add(pageDomain);
		} else
			HttpUtils.blacklistedDomains.add(pageDomain);
		
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
				conn = openHttpConnection(docUrl, domainStr, false, true);
				reconnected = true;
				int responceCode = conn.getResponseCode();    // It's already checked for -1 case (Invalid HTTP responce), inside openHttpConnection().
				if ( (responceCode < 200) || (responceCode >= 400) ) {    // If we have unwanted/error codes.
					onErrorStatusCode(conn.getURL().toString(), domainStr, responceCode);
					throw new DocFileNotRetrievedException();
				}
			}
			int contentSize = 0;
			try {
				contentSize = HttpUtils.getContentSize(conn);
				if ( (contentSize == 0) || (contentSize > HttpUtils.maxAllowedContentSize) ) {
					logger.warn("DocUrl: \"" + docUrl + "\" had a non-acceptable contentSize: " + contentSize + ". The maxAllowed one is: " + HttpUtils.maxAllowedContentSize);
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
					blacklistedDomains.add(domainStr);
				
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
		
		if ( HttpUtils.countAndBlockPathAfterTimes(domainsMultimapWithPaths403BlackListed, timesPathsReturned403, pathStr, domainStr, timesPathToHave403errorCodeBeforeBlocked ) )
		{
			logger.debug("Path: \"" + pathStr + "\" of domain: \"" + domainStr + "\" was blocked after returning 403 Error Code.");
			
			// Block the whole domain if it has more than a certain number of blocked paths.
			if ( HttpUtils.domainsMultimapWithPaths403BlackListed.get(domainStr).size() > HttpUtils.numberOf403BlockedPathsBeforeBlocked )
			{
				HttpUtils.blacklistedDomains.add(domainStr);	// Block the whole domain itself.
				logger.debug("Domain: \"" + domainStr + "\" was blocked, after having more than " + HttpUtils.numberOf403BlockedPathsBeforeBlocked + " of its paths 403blackListed.");
				HttpUtils.domainsMultimapWithPaths403BlackListed.removeAll(domainStr);	// No need to keep its paths anymore.
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
		if ( countAndBlockDomainAfterTimes(HttpUtils.blacklistedDomains, HttpUtils.timesDomainsReturned5XX, domainStr, HttpUtils.timesToHave5XXerrorCodeBeforeBlocked) ) {
			logger.debug("Domain: \"" + domainStr + "\" was blocked after returning 5XX Error Code " + HttpUtils.timesToHave5XXerrorCodeBeforeBlocked + " times.");
			throw new DomainBlockedException();
		}
	}

	
	public static void onTimeoutException(String domainStr) throws DomainBlockedException
	{
		if ( countAndBlockDomainAfterTimes(HttpUtils.blacklistedDomains, HttpUtils.timesDomainsHadTimeoutEx, domainStr, HttpUtils.timesToHaveTimeoutExBeforeBlocked) ) {
			logger.debug("Domain: \"" + domainStr + "\" was blocked after causing TimeoutException " + HttpUtils.timesToHaveTimeoutExBeforeBlocked + " times.");
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
	
}
