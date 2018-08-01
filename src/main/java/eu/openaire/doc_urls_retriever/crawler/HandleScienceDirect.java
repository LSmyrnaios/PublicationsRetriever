package eu.openaire.doc_urls_retriever.crawler;


import eu.openaire.doc_urls_retriever.exceptions.FailedToHandleScienceDirectException;
import eu.openaire.doc_urls_retriever.util.http.ConnSupportUtils;
import eu.openaire.doc_urls_retriever.util.http.HttpConnUtils;
import eu.openaire.doc_urls_retriever.util.url.UrlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * @author Lampros A. Smyrnaios
 */
public class HandleScienceDirect
{
	private static final Logger logger = LoggerFactory.getLogger(HandleScienceDirect.class);
	
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
				HandleScienceDirect.handleScienceDirectFamilyUrls(urlId, sourceUrl, pageUrl, pageDomain, conn, true);
				return true;
			} else if ( pageDomain.equals("sciencedirect.com") ) {	// Be-careful if we move-on changing the retrieving of the domain of a url.
				HandleScienceDirect.handleScienceDirectFamilyUrls(urlId, sourceUrl, pageUrl, pageDomain, conn, false);
				return true;
			}
		} catch (FailedToHandleScienceDirectException sdhe) {
			logger.warn("Problem when handling \"sciencedirect.com\" urls.");
			UrlUtils.logQuadruple(urlId, sourceUrl, null, null, "Discarded in 'PageCrawler.visit()' method, when a 'sciencedirect.com'-url was not able to be handled correctly.", null);
		}
		return false;
	}
	
	
	/**
	 * This method handles the JavaScriptSites of "sciencedirect.com"-family. It retrieves the docLinks hiding inside.
	 * It returns true if the docUrl was found, otherwise, it returns false.
	 * Note that these docUrl d not last long, since they are produced based on timestamp and jsessionid. After a while they just redirect to the pageUrl.
	 * @param urlId
	 * @param sourceUrl
	 * @param pageUrl
	 * @param pageDomain
	 * @param conn
	 * @param isLinkinghubElsevier
	 * @throws FailedToHandleScienceDirectException
	 */
	public static void handleScienceDirectFamilyUrls(String urlId, String sourceUrl, String pageUrl, String pageDomain, HttpURLConnection conn, boolean isLinkinghubElsevier)
																						throws FailedToHandleScienceDirectException
	{
		try {
			// Handle "linkinghub.elsevier.com" urls which contain javaScriptRedirect..
			if ( isLinkinghubElsevier ) {
				//UrlUtils.elsevierLinks ++;
				if ( (pageUrl = silentRedirectElsevierToScienseRedirect(pageUrl)) != null )
					conn = HttpConnUtils.handleConnection(urlId, sourceUrl, pageUrl, pageUrl, pageDomain, true, false);
				else
					throw new FailedToHandleScienceDirectException();
			}
			
			// We now have the "sciencedirect.com" url (either from the beginning or after silentRedirect).
			
			if ( isLinkinghubElsevier )	// Otherwise it's the same as the "visited"-one.
				logger.debug("Produced ScienceDirect-url: " + pageUrl);
			
			String html = ConnSupportUtils.getHtmlString(conn);
			Matcher metaDocUrlMatcher = PageCrawler.META_DOC_URL.matcher(html);
			if ( metaDocUrlMatcher.find() )
			{
				String metaDocUrl = metaDocUrlMatcher.group(1);
				if ( metaDocUrl.isEmpty() ) {
					logger.error("Could not retrieve the metaDocUrl from a \"sciencedirect.com\" url!");
					throw new FailedToHandleScienceDirectException();
				}
				//logger.debug("MetaDocUrl: " + metaDocUrl);    // DEBUG!
				
				// Get the new html..
				// We don't disconnect the previous one, since they both are in the same domain (see JavaDocs).
				conn = HttpConnUtils.handleConnection(urlId, sourceUrl, pageUrl, metaDocUrl, pageDomain, true, false);
				
				//logger.debug("Url after connecting: " + conn.getURL().toString());
				//logger.debug("MimeType: " + conn.getContentType());
				
				html = ConnSupportUtils.getHtmlString(conn);	// Take the new html.
				Matcher finalDocUrlMatcher = SCIENCEDIRECT_FINAL_DOC_URL.matcher(html);
				if ( finalDocUrlMatcher.find() )
				{
					String finalDocUrl = finalDocUrlMatcher.group(1);
					if ( finalDocUrl.isEmpty() ) {
						logger.error("Could not retrieve the finalDocUrl from a \"sciencedirect.com\" url!");
						throw new FailedToHandleScienceDirectException();
					}
					//logger.debug("FinalDocUrl: " + finalDocUrl);    // DEBUG!
					
					// Check and/or download the docUrl. These urls are one-time-links, meaning that after a while they will just redirect to their pageUrl.
					if ( !HttpConnUtils.connectAndCheckMimeType(urlId, sourceUrl, pageUrl, finalDocUrl, pageDomain, false, true) ) {	// We log the docUrl inside this method.
						logger.warn("LookedUp finalDocUrl: \"" + finalDocUrl + "\" was not an actual docUrl!");
						throw new FailedToHandleScienceDirectException();
					}
				} else {
					logger.warn("The finalDocLink could not be found!");
					//logger.debug("HTML-code:\n" + html);	// DEBUG!
					throw new FailedToHandleScienceDirectException();
				}
			} else {
				logger.warn("The metaDocLink could not be found!");	// It's possible if the document only available after paying (https://www.sciencedirect.com/science/article/pii/S1094202598900527)
				//logger.debug("HTML-code:\n" + html);    // DEBUG!
				throw new FailedToHandleScienceDirectException();
			}
		} catch (Exception e) {
			logger.error("" + e);
			throw new FailedToHandleScienceDirectException();
		}
		finally {
			// If the initial pageDomain was different from "sciencedirect.com", close the "sciencedirect.com"-connection here.
			// Otherwise, if it came as a "sciencedirect.com", it will be closed where it was first created, meaning in "HttpConnUtils.connectAndCheckMimeType()".
			if ( !pageDomain.equals("sciencedirect.com") )
				conn.disconnect();	// Disconnect from the final-"sciencedirect.com"-connection.
		}
	}
	
	
	/**
	 * This method recieves a url from "linkinghub.elsevier.com" and returns it's matched url in "sciencedirect.com".
	 * We do this because the "linkinghub.elsevier.com" urls have a javaScript redirect inside which we are not able to handle without doing html scraping.
	 * If there is any error this method returns the URL it first recieved.
	 * @param linkinghubElsevierUrl
	 * @return
	 */
	public static String silentRedirectElsevierToScienseRedirect(String linkinghubElsevierUrl)
	{
		if ( !linkinghubElsevierUrl.contains("linkinghub.elsevier.com") ) // If this method was called for the wrong url, then just return it.
			return linkinghubElsevierUrl;
		
		String idStr = null;
		Matcher matcher = UrlUtils.URL_TRIPLE.matcher(linkinghubElsevierUrl);
		if ( matcher.matches() ) {
			idStr = matcher.group(3);
			if ( idStr == null || idStr.isEmpty() ) {
				logger.warn("Unexpected id-missing case for: " + linkinghubElsevierUrl);
				return linkinghubElsevierUrl;
			}
		}
		else {
			logger.warn("Unexpected \"URL_TRIPLE\" mismatch for: " + linkinghubElsevierUrl);
			return linkinghubElsevierUrl;
		}
		
		return ("https://www.sciencedirect.com/science/article/pii/" + idStr);
	}
	
}
