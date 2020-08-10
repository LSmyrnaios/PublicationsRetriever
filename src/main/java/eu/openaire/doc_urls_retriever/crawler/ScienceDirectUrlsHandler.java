package eu.openaire.doc_urls_retriever.crawler;


import eu.openaire.doc_urls_retriever.exceptions.FailedToProcessScienceDirectException;
import eu.openaire.doc_urls_retriever.util.http.ConnSupportUtils;
import eu.openaire.doc_urls_retriever.util.http.HttpConnUtils;
import eu.openaire.doc_urls_retriever.util.url.UrlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * @author Lampros Smyrnaios
 */
public class ScienceDirectUrlsHandler
{
	private static final Logger logger = LoggerFactory.getLogger(ScienceDirectUrlsHandler.class);
	
	public static final Pattern SCIENCEDIRECT_FINAL_DOC_URL = Pattern.compile("(?:window.location[\\s]+\\=[\\s]+\\')(.*)(?:\\'\\;)");
	
	
	/**
	 * This method checks if the given url belongs to the "scienceDirect-family"-urls and if so it handles it.
	 * If the given url is not a kindOf-scienceDirect-url, then it
	 * @param urlId
	 * @param sourceUrl
	 * @param pageUrl
	 * @param pageDomain
	 * @param conn
	 * @return
	 */
	public static boolean checkIfAndHandleScienceDirect(String urlId, String sourceUrl, String pageUrl, String pageDomain, HttpURLConnection conn)
	{
		try {
			if ( pageDomain.equals("linkinghub.elsevier.com") ) {
				ScienceDirectUrlsHandler.handleScienceDirectFamilyUrls(urlId, sourceUrl, pageUrl, pageDomain, conn, true);
				return true;
			} else if ( pageDomain.equals("sciencedirect.com") ) {	// Be-careful if we move-on changing the retrieving of the domain of a url.
				ScienceDirectUrlsHandler.handleScienceDirectFamilyUrls(urlId, sourceUrl, pageUrl, pageDomain, conn, false);
				return true;
			}
		} catch (FailedToProcessScienceDirectException sdhe) {
			logger.warn("Problem when handling \"sciencedirect.com\" urls.");
			UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Discarded in 'PageCrawler.visit()' method, when a 'sciencedirect.com'-url was not able to be handled correctly.", null);
			return true;	// All that could be done for this ScienceDirectFamilyUrl, was done, signal that it was handled and proceed to the next pageUrl.
		}
		return false;
	}
	
	
	/**
	 * This method handles the JavaScriptSites of "sciencedirect.com"-family. It retrieves the docLinks hiding inside.
	 * It returns true if the docUrl was found, otherwise, it returns false.
	 * Note that these docUrl d not last long, since they are produced based on timestamp and jsessionid. After a while they just redirect to the pageUrl.
	 * ScienceDirect and linkinghub urls have no cannonicalisation problems.
	 * @param urlId
	 * @param sourceUrl
	 * @param pageUrl
	 * @param pageDomain
	 * @param conn
	 * @param isLinkinghubElsevier
	 * @throws FailedToProcessScienceDirectException
	 */
	public static void handleScienceDirectFamilyUrls(String urlId, String sourceUrl, String pageUrl, String pageDomain, HttpURLConnection conn, boolean isLinkinghubElsevier)
																						throws FailedToProcessScienceDirectException
	{
		String currentConnectedUrl = null;	// Used for error-logging-message on connection-error, as there might be multiple urls here to connect with.
		try {
			// Handle "linkinghub.elsevier.com" urls which contain javaScriptRedirect..
			if ( isLinkinghubElsevier ) {
				//UrlUtils.elsevierLinks ++;
				if ( (pageUrl = silentRedirectElsevierToScienceDirect(pageUrl)) != null ) {
					// The already open connection for "linkinghub.elsevier.com" get closed by "connectAndCheckMimeType()" when the "visit()" returns (after "handleScienceDirectFamilyUrls()" returns).
					//logger.debug("Produced ScienceDirect-url: " + pageUrl);	// DEBUG!
					currentConnectedUrl = pageUrl;
					conn = HttpConnUtils.handleConnection(urlId, sourceUrl, pageUrl, pageUrl, pageDomain, true, false);
				} else
					throw new FailedToProcessScienceDirectException();
			}
			// We now have the "sciencedirect.com" url (either from the beginning or after "silent-redirect").
			
			String html = ConnSupportUtils.getHtmlString(conn);
			Matcher metaDocUrlMatcher = MetaDocUrlsHandler.META_DOC_URL.matcher(html);
			if ( !metaDocUrlMatcher.find() )
			{
				logger.warn("The metaDocLink could not be found!");	// It's possible if the document only available after paying or if the crawler gets blocked for heavy traffic.
				//logger.debug("HTML-code:\n" + html);	// DEBUG!
				throw new FailedToProcessScienceDirectException();
			}

			String metaDocUrl = MetaDocUrlsHandler.getMetaDocUrlFromMatcher(metaDocUrlMatcher);
			if ( metaDocUrl == null ) {
				logger.error("Could not retrieve the metaDocUrl from a \"sciencedirect.com\" url!");
				throw new FailedToProcessScienceDirectException();
			}

			// Get the new html after connecting to the "metaDocUrl".
			// We don't disconnect the previous one, since they both are in the same domain (see JavaDocs).
			currentConnectedUrl = metaDocUrl;
			conn = HttpConnUtils.handleConnection(urlId, sourceUrl, pageUrl, metaDocUrl, pageDomain, true, false);

			//logger.debug("Url after connecting: " + conn.getURL().toString());
			//logger.debug("MimeType: " + conn.getContentType());

			html = ConnSupportUtils.getHtmlString(conn);	// Take the new html.
			Matcher finalDocUrlMatcher = SCIENCEDIRECT_FINAL_DOC_URL.matcher(html);
			if ( !finalDocUrlMatcher.find() )
			{
				logger.warn("The finalDocLink could not be found!");
				//logger.debug("HTML-code:\n" + html);	// DEBUG!
				throw new FailedToProcessScienceDirectException();
			}
			String finalDocUrl = null;
			try {
				finalDocUrl = finalDocUrlMatcher.group(1);
			} catch (Exception e) {
				logger.error("", e);
			}
			if ( (finalDocUrl == null) || finalDocUrl.isEmpty() ) {
				logger.error("Could not retrieve the finalDocUrl from a \"sciencedirect.com\" url!");
				throw new FailedToProcessScienceDirectException();
			}
			//logger.debug("FinalDocUrl: " + finalDocUrl);	// DEBUG!

			// Check and/or download the docUrl. These urls are one-time-links, meaning that after a while they will just redirect to their pageUrl.
			currentConnectedUrl = finalDocUrl;
			if ( !HttpConnUtils.connectAndCheckMimeType(urlId, sourceUrl, pageUrl, finalDocUrl, pageDomain, false, true) ) {	// We log the docUrl inside this method.
				logger.warn("LookedUp finalDocUrl: \"" + finalDocUrl + "\" was not an actual docUrl!");
				throw new FailedToProcessScienceDirectException();
			}
		} catch (FailedToProcessScienceDirectException fthsde) {
			logger.error("" + fthsde);
			throw fthsde;
		} catch (RuntimeException re) {
			ConnSupportUtils.printEmbeddedExceptionMessage(re, currentConnectedUrl);
			throw new FailedToProcessScienceDirectException();
		} catch (Exception e) {
			logger.error("" + e);
			throw new FailedToProcessScienceDirectException();
		}
		finally {
			// If the initial pageDomain was different from "sciencedirect.com", close the "sciencedirect.com"-connection here.
			// Otherwise, if it came as a "sciencedirect.com", it will be closed where it was first created, meaning in "HttpConnUtils.connectAndCheckMimeType()".
			if ( !pageDomain.equals("sciencedirect.com") )
				conn.disconnect();	// Disconnect from the final-"sciencedirect.com"-connection.
		}
	}
	
	
	/**
	 * This method receives a url from "linkinghub.elsevier.com" and returns it's matched url in "sciencedirect.com".
	 * We do this because the "linkinghub.elsevier.com" urls have a javaScript redirect inside which we are not able to handle without doing html scraping.
	 * If there is any error this method returns the URL it first received.
	 * @param linkinghubElsevierUrl
	 * @return
	 */
	public static String silentRedirectElsevierToScienceDirect(String linkinghubElsevierUrl)
	{
		String idStr = null;
		Matcher matcher = UrlUtils.URL_TRIPLE.matcher(linkinghubElsevierUrl);
		if ( matcher.matches() ) {
			try {
				idStr = matcher.group(3);
			} catch (Exception e) { logger.error("", e); }
			if ( (idStr == null) || idStr.isEmpty() ) {
				logger.warn("Unexpected id-missing case for: " + linkinghubElsevierUrl);
				return null;
			}
		}
		else {
			logger.warn("Unexpected \"URL_TRIPLE\" mismatch for: " + linkinghubElsevierUrl);
			return null;
		}
		
		return ("https://www.sciencedirect.com/science/article/pii/" + idStr);
	}
	
}
