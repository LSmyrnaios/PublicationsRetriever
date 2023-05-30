package eu.openaire.publications_retriever.util.http;

import edu.uci.ics.crawler4j.url.URLCanonicalizer;
import eu.openaire.publications_retriever.PublicationsRetriever;
import eu.openaire.publications_retriever.crawler.PageCrawler;
import eu.openaire.publications_retriever.crawler.SpecialUrlsHandler;
import eu.openaire.publications_retriever.exceptions.*;
import eu.openaire.publications_retriever.util.file.DocFileData;
import eu.openaire.publications_retriever.util.file.FileUtils;
import eu.openaire.publications_retriever.util.url.LoaderAndChecker;
import eu.openaire.publications_retriever.util.url.UrlTypeChecker;
import eu.openaire.publications_retriever.util.url.UrlUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.BufferedReader;
import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;


/**
 * @author Lampros Smyrnaios
 */
public class HttpConnUtils
{
	private static final Logger logger = LoggerFactory.getLogger(HttpConnUtils.class);

	public static final Set<String> domainsWithUnsupportedHeadMethod = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
	static {	// Add domains which were manually observed to act strangely and cannot be detected automatically at run-time.
		domainsWithUnsupportedHeadMethod.add("os.zhdk.cloud.switch.ch");	// This domain returns "HTTP-403-ERROR" when it does not support the "HEAD" method, at least when checking an actual file.
		// More domains are automatically detected and added to this Set.
	}

	public static final Set<String> domainsWithUnsupportedAcceptLanguageParameter = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

	public static final Set<String> blacklistedDomains = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());	// Domains with which we don't want to connect again.

	public static final ConcurrentHashMap<String, Integer> timesDomainsHadInputNotBeingDocNorPage = new ConcurrentHashMap<String, Integer>();
	public static final ConcurrentHashMap<String, Integer> timesDomainsReturnedNoType = new ConcurrentHashMap<String, Integer>();	// Domain which returned no content-type not content disposition in their response and amount of times they did.

	public static AtomicInteger numOfDomainsBlockedDueToSSLException = new AtomicInteger(0);

	public static String userAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/112.0";	// This should not be "final", another program, using this software as a library, should be able to set its own "UserAgent".
	public static String acceptLanguage = "en-US,en;q=0.5";

	public static final int maxConnGETWaitingTime = 15_000;	// Max time (in ms) to wait for a connection, using "HTTP GET".
	public static final int maxConnHEADWaitingTime = 10_000;	// Max time (in ms) to wait for a connection, using "HTTP HEAD".

	private static final int maxRedirectsForPageUrls = 7;// The usual redirect times for doi.org urls is 3, though some of them can reach even 5 (if not more..)
	private static final int maxRedirectsForInternalLinks = 2;	// Internal-DOC-Links shouldn't take more than 2 redirects.

	private static final int timesToHaveNoDocNorPageInputBeforeBlocked = 10;

	public static final int maxAllowedContentSize = 1073741824;	// 1Gb ; yes some publications can be huge..
	private static final boolean shouldNOTacceptGETmethodForUncategorizedInternalLinks = true;

	public static final Set<String> domainsSupportingHTTPS = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

	public static final Set<String> domainsWithSlashRedirect = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());


	public static final Pattern ENDING_WITH_SLASH_OR_EXTENSION_FILTER = Pattern.compile(".*(?:(?:/|\\.[^.?&/_-]{1,7})(?:\\?.+)?|(?:\\?.+))$");
	// The above regex, assumes file-extensions up to 7-chars long. Rare file-extensions with app to 10 or more characters exist,
	// but then the risk of identifying irrelevant dot-prepended strings as extensions, increases (some urls end with: "<other chars>.NOTEXTENSIONSTRING").

	public static AtomicInteger timesDidOfflineHTTPSredirect = new AtomicInteger(0);

	public static AtomicInteger timesDidOfflineSlashRedirect = new AtomicInteger(0);

	public static ThreadLocal<Boolean> isSpecialUrl = new ThreadLocal<Boolean>();	// Every Thread has its own variable.

	public static final String docFileNotRetrievedMessage = DocFileNotRetrievedException.class.getSimpleName() + " was thrown before the docFile could be stored. ";  // Get the class-name programmatically, in order to easily spot the error if the exception-name changes.

	public static final CookieManager cookieManager = new java.net.CookieManager();
	static {
		cookieManager.setCookiePolicy(CookiePolicy.ACCEPT_ORIGINAL_SERVER);
		CookieHandler.setDefault(cookieManager);
	}


	/**
	 * This method checks if a certain url can give us its mimeType, as well as if this mimeType is a docMimeType.
	 * It automatically calls the "logUrl()" method for the valid docOrDatasetUrls, while it doesn't call it for non-success cases, thus allowing calling method to handle the case.
	 * @param urlId
	 * @param sourceUrl	// The inputUrl
	 * @param pageUrl	// May be the inputUrl or a redirected version of it.
	 * @param resourceURL	// May be the inputUrl or an internalLink of that inputUrl.
	 * @param domainStr
	 * @param calledForPageUrl
	 * @param calledForPossibleDocOrDatasetUrl
	 * @return "true", if it's a docMimeType, otherwise, "false", if it has a different mimeType.
	 * @throws RuntimeException (when there was a network error).
	 * @throws ConnTimeoutException
	 * @throws DomainBlockedException
	 * @throws DomainWithUnsupportedHEADmethodException
	 */
	public static boolean connectAndCheckMimeType(String urlId, String sourceUrl, String pageUrl, String resourceURL, String domainStr, boolean calledForPageUrl, boolean calledForPossibleDocOrDatasetUrl)
													throws RuntimeException, ConnTimeoutException, DomainBlockedException, DomainWithUnsupportedHEADmethodException
	{
		HttpURLConnection conn = null;
		try {
			if ( domainStr == null )	// No info about domainStr from the calling method.. we have to find it here.
				if ( (domainStr = UrlUtils.getDomainStr(resourceURL, null)) == null )
					throw new RuntimeException("Unable to obtain the domain!");	// The cause it's already logged inside "getDomainStr()".

			conn = handleConnection(urlId, sourceUrl, pageUrl, resourceURL, domainStr, calledForPageUrl, calledForPossibleDocOrDatasetUrl);

			// Check if we are able to find the mime type, if not then try "Content-Disposition".
			String mimeType = conn.getContentType();
			String contentDisposition = null;

			String finalUrlStr = conn.getURL().toString();

			if ( !finalUrlStr.contains(domainStr) )	// Get the new domain after possible change from redirects.
				if ( (domainStr = UrlUtils.getDomainStr(finalUrlStr, null)) == null )
					throw new RuntimeException("Unable to obtain the domain!");	// The cause it's already logged inside "getDomainStr()".

			boolean foundDetectedContentType = false;
			String firstHtmlLine = null;
			BufferedReader bufferedReader = null;

			///////////////////////////
			//mimeType = null;	// DEBUG!
			///////////////////////////

			if ( mimeType == null ) {
				contentDisposition = conn.getHeaderField("Content-Disposition");
				if ( contentDisposition == null ) {
					ArrayList<Object> detectionList = ConnSupportUtils.detectContentTypeFromResponseBody(finalUrlStr, domainStr, conn, calledForPageUrl);
					mimeType = (String)detectionList.get(0);
					foundDetectedContentType = (boolean) detectionList.get(1);
					firstHtmlLine = (String)detectionList.get(2);
					bufferedReader = (BufferedReader) detectionList.get(3);	// This can be reused when getting the html of the page.
					calledForPossibleDocOrDatasetUrl = (boolean) detectionList.get(4);
					//logger.debug(mimeType); logger.debug(String.valueOf(foundDetectedContentType)); logger.debug(firstHtmlLine); logger.debug(String.valueOf(bufferedReader)); logger.debug(String.valueOf(calledForPossibleDocUrl));	// DEBUG!
				} else
					contentDisposition = contentDisposition.toLowerCase();
			}

			String lowerCaseMimeType = mimeType;
			if ( mimeType != null && !foundDetectedContentType )	// It may have gained a value after auto-detection, so we check again. If it was auto-detected, then it's already in lowercase.
				lowerCaseMimeType = mimeType.toLowerCase();	// I found the rare case of "Application/pdf", so we need lowercase as any mimeType could have either uppercase or lowercase version..

			//logger.debug("Url: " + finalUrlStr);	// DEBUG!
			//logger.debug("MimeType: " + mimeType);	// DEBUG!
			String returnedType = ConnSupportUtils.hasDocOrDatasetMimeType(finalUrlStr, lowerCaseMimeType, contentDisposition, conn, calledForPageUrl, calledForPossibleDocOrDatasetUrl);
			if ( (returnedType != null) )
			{
				if ( LoaderAndChecker.retrieveDocuments && returnedType.equals("document") ) {
					logger.info("docUrl found: < " + finalUrlStr + " >");
					String fullPathFileName = "";
					String wasDirectLink = ConnSupportUtils.getWasDirectLink(sourceUrl, pageUrl, calledForPageUrl, finalUrlStr);
					DocFileData docFileData = null;
					if ( FileUtils.shouldDownloadDocFiles ) {
						if ( foundDetectedContentType ) {	// If we went and detected the pdf from the request-code, then reconnect and proceed with downloading (reasons explained elsewhere).
							conn = handleConnection(urlId, sourceUrl, pageUrl, finalUrlStr, domainStr, calledForPageUrl, calledForPossibleDocOrDatasetUrl);	// No need to "conn.disconnect()" before, as we are re-connecting to the same domain.
						}
						try {
							docFileData = ConnSupportUtils.downloadAndStoreDocFile(conn, urlId, domainStr, finalUrlStr, calledForPageUrl);	// It does not return "null".
							fullPathFileName = docFileData.getLocation();
							logger.info("DocFile: \"" + fullPathFileName + "\" has been downloaded.");
							UrlUtils.logOutputData(urlId, sourceUrl, pageUrl, finalUrlStr, fullPathFileName, null, true, "true", "true", "true", wasDirectLink, "true", docFileData.getSize(), docFileData.getHash());	// we send the urls, before and after potential redirections.
							return true;
						} catch (DocFileNotRetrievedException dfnde) {
							fullPathFileName = docFileNotRetrievedMessage + dfnde.getMessage();
						}	// We log below and then return.
					}
					UrlUtils.logOutputData(urlId, sourceUrl, pageUrl, finalUrlStr, fullPathFileName, null, true, "true", "true", "true", wasDirectLink, "true", null, "null");	// we send the urls, before and after potential redirections.
					return true;
				}
				else if ( LoaderAndChecker.retrieveDatasets && returnedType.equals("dataset") ) {
					logger.info("datasetUrl found: < " + finalUrlStr + " >");
					// TODO - handle possible download and improve logging... The dataset might have huge size each. Downloading these, isn't a requirement at the moment.
					String fullPathFileName = FileUtils.shouldDownloadDocFiles ? "It's a dataset-url. The download is not supported." : "It's a dataset-url.";
					String wasDirectLink = ConnSupportUtils.getWasDirectLink(sourceUrl, pageUrl, calledForPageUrl, finalUrlStr);
					UrlUtils.logOutputData(urlId, sourceUrl, pageUrl, finalUrlStr, fullPathFileName, null, true, "true", "true", "true", wasDirectLink, "true", null, "null");	// we send the urls, before and after potential redirections.
					return true;
				}
				else {	// Either "document" or "dataset", but the user specified that he doesn't want it.
					//logger.debug("Type \"" + returnedType + "\", which was specified that it's unwanted in this run, was found for url: < " + finalUrlStr + " >");	// DEBUG!
					if ( calledForPageUrl )
						UrlUtils.logOutputData(urlId, sourceUrl, null, UrlUtils.unreachableDocOrDatasetUrlIndicator, "It was discarded in 'HttpConnUtils.connectAndCheckMimeType()', after matching to an unwanted mimeType: " + returnedType, null, true, "true", "true", "false", "false", "true", null, "null");
					return false;
				}
			}
			else if ( calledForPageUrl ) {	// Visit this url only if this method was called for an inputUrl.
				if ( finalUrlStr.contains("viewcontent.cgi") ) {	// If this "viewcontent.cgi" isn't a docUrl, then don't check its internalLinks. Check this: "https://docs.lib.purdue.edu/cgi/viewcontent.cgi?referer=&httpsredir=1&params=/context/physics_articles/article/1964/type/native/&path_info="
					logger.warn("Unwanted pageUrl: \"" + finalUrlStr + "\" will not be visited!");
					UrlUtils.logOutputData(urlId, sourceUrl, null, UrlUtils.unreachableDocOrDatasetUrlIndicator, "It was discarded in 'HttpConnUtils.connectAndCheckMimeType()', after matching to a non-" + PublicationsRetriever.targetUrlType + " with 'viewcontent.cgi'.", null, true, "true", "true", "false", "false", "false", null, "null");
					UrlTypeChecker.pagesNotProvidingDocUrls.incrementAndGet();
					return false;
				}
				else if ( (lowerCaseMimeType != null) && ((lowerCaseMimeType.contains("htm") || (lowerCaseMimeType.contains("text") && !lowerCaseMimeType.contains("xml") && !lowerCaseMimeType.contains("csv") && !lowerCaseMimeType.contains("tsv")))) )	// The content-disposition is non-usable in the case of pages.. it's probably not provided anyway.
					// TODO - Better make a regex for the above checks..
					PageCrawler.visit(urlId, sourceUrl, finalUrlStr, mimeType, conn, firstHtmlLine, bufferedReader);
				else {
					logger.warn("Non-pageUrl: \"" + finalUrlStr + "\" with mimeType: \"" + mimeType + "\" will not be visited!");
					UrlUtils.logOutputData(urlId, sourceUrl, null, UrlUtils.unreachableDocOrDatasetUrlIndicator, "It was discarded in 'HttpConnUtils.connectAndCheckMimeType()', after not matching to a " + PublicationsRetriever.targetUrlType + " nor to an htm/text-like page.", null, true, "true", "true", "false", "false", "false", null, "null");
					if ( ConnSupportUtils.countAndBlockDomainAfterTimes(blacklistedDomains, timesDomainsHadInputNotBeingDocNorPage, domainStr, HttpConnUtils.timesToHaveNoDocNorPageInputBeforeBlocked, true) )
						logger.warn("Domain: \"" + domainStr + "\" was blocked after having no Doc nor Pages in the input more than " + HttpConnUtils.timesToHaveNoDocNorPageInputBeforeBlocked + " times.");
				}	// We log the quadruple here, as there is connection-kind-of problem here.. it's just us considering it an unwanted case. We don't throw "DomainBlockedException()", as we don't handle it for inputUrls (it would also log the quadruple twice with diff comments).
			}
		} catch (AlreadyFoundDocUrlException afdue) {	// An already-found docUrl was discovered during redirections.
			return true;	// It's already logged for the outputFile.
		} catch (RuntimeException re) {
			if ( re instanceof NullPointerException )
				logger.error("", re);

			if ( calledForPageUrl ) {
				LoaderAndChecker.connProblematicUrls.incrementAndGet();
				ConnSupportUtils.printEmbeddedExceptionMessage(re, resourceURL);
			}	// Log this error only for docPages or possibleDocOrDatasetUrls, not other internalLinks.
			else if ( calledForPossibleDocOrDatasetUrl )
				ConnSupportUtils.printEmbeddedExceptionMessage(re, resourceURL);
			throw re;
		} catch (ConnTimeoutException cte) {
			if ( calledForPageUrl )
				UrlTypeChecker.longToRespondUrls.incrementAndGet();
			throw cte;
		} catch (DomainBlockedException | DomainWithUnsupportedHEADmethodException e) {
			if ( calledForPageUrl )
				LoaderAndChecker.connProblematicUrls.incrementAndGet();
			throw e;
		} catch (Exception e) {
			if ( calledForPageUrl ) {	// Log this error only for docPages.
				logger.warn("Could not handle connection for \"" + resourceURL + "\"!");
				LoaderAndChecker.connProblematicUrls.incrementAndGet();
			}
			throw new RuntimeException(e.getMessage());
		} finally {
			if ( conn != null )
				conn.disconnect();
		}
		return false;
	}


	public static HttpURLConnection handleConnection(String urlId, String sourceUrl, String pageUrl, String resourceURL, String domainStr, boolean calledForPageUrl, boolean calledForPossibleDocUrl)
										throws AlreadyFoundDocUrlException, RuntimeException, ConnTimeoutException, DomainBlockedException, DomainWithUnsupportedHEADmethodException, IOException
	{
		if ( (domainStr == null) && (domainStr = UrlUtils.getDomainStr(resourceURL, null)) == null )
			throw new RuntimeException("Unable to obtain the domain!");

		HttpURLConnection conn = openHttpConnection(resourceURL, domainStr, calledForPageUrl, calledForPossibleDocUrl);
		// The "resourceUrl" might have changed (due to special-handling of some pages), but it doesn't cause any problem. It's only used with "internalLinks" which are not affected by the special handling.

		//ConnSupportUtils.printConnectionDebugInfo(conn, true);	// DEBUG!

		int responseCode = conn.getResponseCode();	// It's already checked for -1 case (Invalid HTTP response), inside openHttpConnection().
		if ( (responseCode >= 300) && (responseCode <= 399) && (responseCode != 304) ) {   // If we have redirections..
			conn = handleRedirects(urlId, sourceUrl, pageUrl, resourceURL, conn, responseCode, domainStr, calledForPageUrl, calledForPossibleDocUrl);    // Take care of redirects.
		}
		else if ( (responseCode < 200) || (responseCode >= 400) ) {	// If we have error codes.
			String errorMessage = ConnSupportUtils.onErrorStatusCode(conn.getURL().toString(), domainStr, responseCode, calledForPageUrl);
			throw new RuntimeException(errorMessage);	// This is not thrown, if a "DomainBlockedException" is thrown from the previous method-call.
		}
		// Else it's an HTTP 2XX SUCCESS CODE or an HTTP 304 NOT MODIFIED
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
		HttpURLConnection conn = null;
		int responseCode = 0;

		try {
			if ( blacklistedDomains.contains(domainStr) )
				throw new RuntimeException("Avoid connecting to blackListed domain: \"" + domainStr + "\" with url: " + resourceURL);

			// Check whether we don't accept "GET" method for uncategorizedInternalLinks and if this url is such a case.
			if ( !calledForPageUrl && shouldNOTacceptGETmethodForUncategorizedInternalLinks
					&& !calledForPossibleDocUrl && domainsWithUnsupportedHeadMethod.contains(domainStr) )	// Exclude the possibleDocUrls and the ones which cannot connect with "HEAD".
				throw new DomainWithUnsupportedHEADmethodException();

			if ( ConnSupportUtils.checkIfPathIs403BlackListed(resourceURL, domainStr) )
				throw new RuntimeException("Avoid reaching 403ErrorCode with url: \"" + resourceURL + "\"!");

			if ( !resourceURL.startsWith("https:", 0) && domainsSupportingHTTPS.contains(domainStr) ) {
				resourceURL = StringUtils.replace(resourceURL, "http:", "https:", 1);
				timesDidOfflineHTTPSredirect.incrementAndGet();
			}

			if ( !ENDING_WITH_SLASH_OR_EXTENSION_FILTER.matcher(resourceURL).matches() && domainsWithSlashRedirect.contains(domainStr) ) {
				resourceURL += "/";
				timesDidOfflineSlashRedirect.incrementAndGet();
			}

			// For the urls which has reached this point, make sure no weird "ampersand"-anomaly blocks us...
			boolean weirdMetaDocUrlWhichNeedsGET = false;
			if ( calledForPossibleDocUrl && resourceURL.contains("amp%3B") ) {
				//logger.debug("Just arrived weirdMetaDocUrl: " + resourceURL);
				resourceURL = StringUtils.replace(resourceURL, "amp%3B", "", -1);
				//logger.debug("After replacement in the weirdMetaDocUrl: " + resourceURL);
				weirdMetaDocUrlWhichNeedsGET = true;
			}

			isSpecialUrl.set(false);	// It will be false until proven to not pass through the "SpecialUrls"
			if ( calledForPageUrl || calledForPossibleDocUrl ) {
				String tempURL = SpecialUrlsHandler.checkAndHandleSpecialUrls(resourceURL);	// May throw a "RuntimeException".
				isSpecialUrl.set( !tempURL.equals(resourceURL) );
				resourceURL = tempURL;
			}

			URL url = new URL(resourceURL);
			conn = (HttpURLConnection) url.openConnection();
			conn.setRequestProperty("User-Agent", userAgent);
			if ( !domainsWithUnsupportedAcceptLanguageParameter.contains(domainStr) )
				conn.setRequestProperty("Accept-Language", acceptLanguage);
			conn.setInstanceFollowRedirects(false);	// We manage redirects on our own, in order to control redirectsNum, avoid redirecting to unwantedUrls and handling errors.

			boolean useHttpGetMethod = false;

			if ( (calledForPageUrl && !calledForPossibleDocUrl)	// For just-webPages, we want to use "GET" in order to download the content.
				|| (calledForPossibleDocUrl && FileUtils.shouldDownloadDocFiles)	// For docUrls, if we should download them.
				|| weirdMetaDocUrlWhichNeedsGET	// If we have a weirdMetaDocUrl-case then we need "GET".
				|| domainsWithUnsupportedHeadMethod.contains(domainStr)	// If the domain doesn't support "HEAD", then we only do "GET".
				|| domainStr.contains("meetingorganizer.copernicus.org") )	// This domain has pdf-urls which are discovered (via their ContentType) only when using "GET".
			{
				conn.setRequestMethod("GET");	// Go directly with "GET".
				conn.setConnectTimeout(maxConnGETWaitingTime);
				conn.setReadTimeout(maxConnGETWaitingTime);
				useHttpGetMethod = true;
			} else {
				conn.setRequestMethod("HEAD");	// Else, try "HEAD" (it may be either a domain that supports "HEAD", or a new domain, for which we have no info yet).
				conn.setConnectTimeout(maxConnHEADWaitingTime);
				conn.setReadTimeout(maxConnHEADWaitingTime);
			}

			ConnSupportUtils.applyPolitenessDelay(domainStr);

			conn.connect();	// Else, first connect and if there is no error, log this domain as the last one.

			if ( (responseCode = conn.getResponseCode()) == -1 )
				throw new RuntimeException("Invalid HTTP response for \"" + resourceURL + "\"");

			if ( responseCode == 406 )	// It's possible that the server does not support the "Accept-Language" parameter. Try again without it.
			{
				logger.warn("The server \"" + domainStr + "\" probably does not support the \"Accept-Language\" parameter. Going to reconnect without it");
				domainsWithUnsupportedAcceptLanguageParameter.add(domainStr);	// Take note that this domain does not support it..

				conn = (HttpURLConnection) url.openConnection();
				conn.setRequestProperty("User-Agent", userAgent);
				conn.setInstanceFollowRedirects(false);

				if ( useHttpGetMethod ) {
					conn.setRequestMethod("GET");	// Go directly with "GET".
					conn.setConnectTimeout(maxConnGETWaitingTime);
					conn.setReadTimeout(maxConnGETWaitingTime);
				} else {
					conn.setRequestMethod("HEAD");	// Else, try "HEAD" (it may be either a domain that supports "HEAD", or a new domain, for which we have no info yet).
					conn.setConnectTimeout(maxConnHEADWaitingTime);
					conn.setReadTimeout(maxConnHEADWaitingTime);
				}

				ConnSupportUtils.applyPolitenessDelay(domainStr);

				conn.connect();	// Else, first connect and if there is no error, log this domain as the last one.

				if ( conn.getResponseCode() == -1 )
					throw new RuntimeException("Invalid HTTP response for \"" + resourceURL + "\"");
			}
			else if ( ((responseCode == 405) || (responseCode == 501)) && conn.getRequestMethod().equals("HEAD") )	// If this SERVER doesn't support "HEAD" method or doesn't allow us to use it..
			{
				//logger.debug("HTTP \"HEAD\" method is not supported for: \"" + resourceURL +"\". Server's responseCode was: " + responseCode);
				domainsWithUnsupportedHeadMethod.add(domainStr);	// This domain doesn't support "HEAD" method, log it and then check if we can retry with "GET" or not.

				if ( !calledForPageUrl && shouldNOTacceptGETmethodForUncategorizedInternalLinks && !calledForPossibleDocUrl )	// If we set not to retry with "GET" when we try uncategorizedInternalLinks, throw the related exception and stop the crawling of this page.
					throw new DomainWithUnsupportedHEADmethodException();

				// If we accept connection's retrying, using "GET", move on reconnecting.
				// No call of "conn.disconnect()" here, as we will connect to the same server.
				conn = (HttpURLConnection) url.openConnection();
				conn.setRequestMethod("GET");	// To reach here, it means that the HEAD method is unsupported.
				conn.setRequestProperty("User-Agent", userAgent);
				if ( !domainsWithUnsupportedAcceptLanguageParameter.contains(domainStr) )
					conn.setRequestProperty("Accept-Language", acceptLanguage);
				conn.setConnectTimeout(maxConnGETWaitingTime);
				conn.setReadTimeout(maxConnGETWaitingTime);
				conn.setInstanceFollowRedirects(false);

				ConnSupportUtils.applyPolitenessDelay(domainStr);

				conn.connect();
				//logger.debug("responseCode for \"" + resourceURL + "\", after setting conn-method to: \"" + conn.getRequestMethod() + "\" is: " + conn.getResponseCode());

				responseCode = conn.getResponseCode();
				if ( responseCode == -1 )	// Make sure we throw a RunEx on invalidHTTP.
					throw new RuntimeException("Invalid HTTP response for \"" + resourceURL + "\"");

				if ( responseCode == 406 )	// It's possible that the server does not support the "Accept-Language" parameter.
				{
					//logger.debug("The \"Accept-Language\" parameter is probably not supported for: \"" + resourceURL +"\". Server's responseCode was: " + responseCode);
					logger.warn("The server \"" + domainStr + "\" probably does not support the \"Accept-Language\" parameter. Going to reconnect without it");
					domainsWithUnsupportedAcceptLanguageParameter.add(domainStr);	// Take note that this domain does not support it..

					conn = (HttpURLConnection) url.openConnection();
					conn.setRequestMethod("GET");	// To reach here, it means that the HEAD method is unsupported.
					conn.setRequestProperty("User-Agent", userAgent);
					conn.setConnectTimeout(maxConnGETWaitingTime);
					conn.setReadTimeout(maxConnGETWaitingTime);
					conn.setInstanceFollowRedirects(false);

					ConnSupportUtils.applyPolitenessDelay(domainStr);

					conn.connect();	// Else, first connect and if there is no error, log this domain as the last one.

					if ( (conn.getResponseCode()) == -1 )
						throw new RuntimeException("Invalid HTTP response for \"" + resourceURL + "\"");
				}
			}
		} catch (RuntimeException | DomainWithUnsupportedHEADmethodException redwuhme) {
			if ( conn != null )
				conn.disconnect();
			throw redwuhme;	// We want to throw the same exception to keep the messages and the stacktrace in place.
		} catch (Exception e) {
			if ( conn != null )
				conn.disconnect();
			if ( e instanceof UnknownHostException ) {
				logger.warn("A new \"Unknown Network\" Host was found and blacklisted: \"" + domainStr + "\"");
				blacklistedDomains.add(domainStr);	//Log it to never try connecting with it again.
				throw new DomainBlockedException(domainStr);
			}
			else if ( e instanceof SocketTimeoutException ) {
				logger.debug("Url: \"" + resourceURL + "\" failed to respond on time!");
				ConnSupportUtils.onTimeoutException(domainStr);	// May throw a "DomainBlockedException", which will be thrown before the "ConnTimeoutException".
				throw new ConnTimeoutException();
			}
			else if ( e instanceof ConnectException ) {
				String eMsg = e.getMessage();
				if ( (eMsg != null) && eMsg.toLowerCase().contains("timeout") ) {	// If it's a "connection timeout" type of exception, treat it like it.
					ConnSupportUtils.onTimeoutException(domainStr);	// Can throw a "DomainBlockedException", which will be thrown before the "ConnTimeoutException".
					throw new ConnTimeoutException();
				}
				throw new RuntimeException(eMsg);
			}
			else if ( e instanceof SSLException ) {
				// TODO - For "SSLProtocolException", see more about it's possible handling here: https://stackoverflow.com/questions/7615645/ssl-handshake-alert-unrecognized-name-error-since-upgrade-to-java-1-7-0/14884941#14884941
				// TODO - Maybe we should make another list where only urls in https, from these domains, would be blocked.
				blacklistedDomains.add(domainStr);
				numOfDomainsBlockedDueToSSLException.incrementAndGet();
				logger.warn("No Secure connection was able to be negotiated with the domain: \"" + domainStr + "\", so it was blocked. Exception message: " + e.getMessage());
				throw new DomainBlockedException(domainStr);
			}
			else if ( e instanceof SocketException ) {
				String errorMsg = e.getMessage();
				if ( errorMsg != null )
					errorMsg = "\"" + errorMsg + "\". This SocketException was received after trying to connect with the domain: \"" + domainStr + "\"";

				// We don't block the domain, since this is temporary.
				throw new RuntimeException(errorMsg);
			}

			logger.error("", e);
			throw new RuntimeException(e.getMessage());
		}
		
		return conn;
    }
    
	
    /**
     * This method takes an open connection for which there is a need for redirections (this need is verified before this method is called).
     * It opens a new connection every time, up to the point we reach a certain number of redirections defined by "maxRedirects".
	 * @param urlId
	 * @param sourceUrl
	 * @param pageUrl
	 * @param internalLink
	 * @param conn
	 * @param responseCode
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
	public static HttpURLConnection handleRedirects(String urlId, String sourceUrl, String pageUrl, String internalLink, HttpURLConnection conn, int responseCode, String domainStr, boolean calledForPageUrl, boolean calledForPossibleDocUrl)
																			throws AlreadyFoundDocUrlException, RuntimeException, ConnTimeoutException, DomainBlockedException, DomainWithUnsupportedHEADmethodException
	{
		int curRedirectsNum = 0;
		int maxRedirects = 0;
		String initialUrl = null;
		String urlType = null;	// Used for logging.
		
		if ( calledForPageUrl ) {
			maxRedirects = maxRedirectsForPageUrls;
			initialUrl = sourceUrl;	// Keep initialUrl for logging and debugging.
			urlType = "pageUrl";
		} else {
			maxRedirects = maxRedirectsForInternalLinks;
			initialUrl = internalLink;
			urlType = "internalLink";
		}
		URL currentUrlObject;
		String currentUrl;

		try {
			do {	// We assume we already have an HTTP-3XX response code.
				curRedirectsNum ++;
				if ( curRedirectsNum > maxRedirects )
					throw new RuntimeException("Redirects exceeded their limit (" + maxRedirects + ") for " + urlType + ": \"" + initialUrl + "\"");

				currentUrlObject = conn.getURL();
				currentUrl = currentUrlObject.toString();

				String location = conn.getHeaderField("Location");
				if ( location == null )
				{
					if ( responseCode == 300 ) {	// The "Location"-data MAY be provided, inside the html-response, giving the proposed link by the server.
						// Go and parse the page and select one of the links to redirect to. Assign it to the "location".
						if ( (location = ConnSupportUtils.getInternalLinkFromHTTP300Page(conn)) == null )
							throw new RuntimeException("No \"link\" was retrieved from the HTTP-300-page: \"" + currentUrl + "\".");
					}
					else	// It's unacceptable for codes > 300 to not provide the "location" field.
						throw new RuntimeException("No \"Location\" field was found in the HTTP Header of \"" + currentUrl + "\", after receiving an \"HTTP " + responseCode + "\" Redirect Code.");
				}

				String targetUrl = ConnSupportUtils.getFullyFormedUrl(null, location, currentUrlObject);
				if ( targetUrl == null )
					throw new RuntimeException("Could not create target url for resourceUrl: " + currentUrl + " having location: " + location);

				String lowerCaseTargetUrl = targetUrl.toLowerCase();
				if ( (calledForPageUrl && UrlTypeChecker.shouldNotAcceptPageUrl(targetUrl, lowerCaseTargetUrl))	// Redirecting a pageUrl.
						|| (!calledForPageUrl && UrlTypeChecker.shouldNotAcceptInternalLink(targetUrl, lowerCaseTargetUrl)) )	// Redirecting an internalPageLink.
					throw new RuntimeException("Url: \"" + initialUrl + "\" was prevented to redirect to the unwanted location: \"" + targetUrl + "\", after receiving an \"HTTP " + responseCode + "\" Redirect Code, in redirection-number: " + curRedirectsNum);
				else if ( lowerCaseTargetUrl.contains("sharedsitesession") ) {	// either "getSharedSiteSession" or "consumeSharedSiteSession".
					logger.warn("Initial-url: \"" + initialUrl + "\" tried to cause a \"sharedSiteSession-redirectionPack\" by redirecting to \"" + targetUrl + "\"!");
					List<String> blockedDomains = ConnSupportUtils.blockSharedSiteSessionDomains(targetUrl, currentUrl);
					throw new DomainBlockedException(blockedDomains);
				}

				String tempTargetUrl = targetUrl;
				if ( !UrlUtils.URL_ACCEPTED_CHARS_TO_AVOID_CANONICALIZATION.matcher(targetUrl).matches() && ((targetUrl = URLCanonicalizer.getCanonicalURL(targetUrl, null, StandardCharsets.UTF_8)) == null) )
					throw new RuntimeException("Could not canonicalize target url: " + tempTargetUrl);	// Don't let it continue.

				//ConnSupportUtils.printRedirectDebugInfo(currentUrl, location, targetUrl, responseCode, curRedirectsNum);

				if ( UrlUtils.docOrDatasetUrlsWithIDs.containsKey(targetUrl) ) {	// If we got into an already-found docUrl, log it and return.
					ConnSupportUtils.handleReCrossedDocUrl(urlId, sourceUrl, pageUrl, targetUrl, calledForPageUrl);
					throw new AlreadyFoundDocUrlException();
				}

				if ( !targetUrl.contains(domainStr) ) {    // If the next page is not in the same domain as the current one, we have to find the domain again.
					conn.disconnect();	// Close the socket with that server.
					if ( (domainStr = UrlUtils.getDomainStr(targetUrl, null)) == null )
						throw new RuntimeException("Unable to obtain the domain!");	// The cause it's already logged inside "getDomainStr()".
				}

				// Check if this redirection is just http-to-https and store the domain to do offline-redirection in the future.
				// If the domain supports it, do an offline-redirect to "HTTPS", thus avoiding the expensive real-redirect.
				if ( ConnSupportUtils.isJustAnHTTPSredirect(currentUrl, targetUrl) ) {
					domainsSupportingHTTPS.add(domainStr);    // It does not store duplicates.
					//logger.debug("Found an HTTP-TO-HTTPS redirect: " + currentUrl + " --> " + targetUrl);	// DEBUG!
				}

				if ( ConnSupportUtils.isJustASlashRedirect(currentUrl, targetUrl) ) {	// If the "targetUrl" is the "currentUrl" but with just an added "/".
					domainsWithSlashRedirect.add(domainStr);
					//logger.debug("Found a non-slash-ended to slash-ended url redirection: " + currentUrl + " --> " + targetUrl);	// DEBUG!s
				}

				conn = HttpConnUtils.openHttpConnection(targetUrl, domainStr, calledForPageUrl, calledForPossibleDocUrl);

				responseCode = conn.getResponseCode();	// It's already checked for -1 case (Invalid HTTP), inside openHttpConnection().

				if ( ((responseCode >= 200) && (responseCode <= 299)) || (responseCode == 304) ) {
					//ConnSupportUtils.printFinalRedirectDataForWantedUrlType(initialUrl, currentUrl, null, curRedirectsNum);	// DEBUG!
					return conn;	// It's an "HTTP SUCCESS" or "NOT MODIFIED" (the content is cached), return immediately.
				}
			} while ( (responseCode >= 300) && (responseCode <= 399) );
			
			// It should have returned if there was an HTTP 2XX code. Now we have to handle the error-code.
			String errorMessage = ConnSupportUtils.onErrorStatusCode(currentUrl, domainStr, responseCode, calledForPageUrl);
			throw new RuntimeException(errorMessage);	// This is not thrown if a "DomainBlockedException" was thrown first.
			
		} catch (AlreadyFoundDocUrlException | RuntimeException | ConnTimeoutException | DomainBlockedException | DomainWithUnsupportedHEADmethodException e) {	// We already logged the right messages.
			conn.disconnect();
			throw e;
		} catch (Exception e) {
			logger.warn("", e);
			conn.disconnect();
			throw new RuntimeException(e.getMessage());
		}
	}

}
