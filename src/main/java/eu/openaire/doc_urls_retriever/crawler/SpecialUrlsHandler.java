package eu.openaire.doc_urls_retriever.crawler;


import eu.openaire.doc_urls_retriever.exceptions.FailedToProcessScienceDirectException;
import eu.openaire.doc_urls_retriever.util.url.UrlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author Lampros Smyrnaios
 */
public class SpecialUrlsHandler
{
	private static final Logger logger = LoggerFactory.getLogger(SpecialUrlsHandler.class);
	
	// The following regex-pattern was used in ScienceDirect-handling in early days. Keep it here, as it may be of need in any domain in the future..
	// public static final Pattern JAVASCRIPT_REDIRECT_URL = Pattern.compile("(?:window.location[\\s]+\\=[\\s]+\\')(.*)(?:\\'\\;)");

	private static final String scienceDirectBasePath = "https://www.sciencedirect.com/science/article/pii/";


	/**
	 * This method checks if the given url belongs to the "scienceDirect-family"-urls and if so it handles it.
	 * If the given url is not a kindOf-scienceDirect-url, then it
	 * @param pageUrl
	 * @return
	 */
	public static String checkAndGetScienceDirectUrl(String pageUrl) throws FailedToProcessScienceDirectException
	{
		boolean wasLinkinghubElsevier = false;
		if ( pageUrl.contains("linkinghub.elsevier.com") )
		{
			if ( (pageUrl = offlineRedirectElsevierToScienceDirect(pageUrl)) == null ) {	// Logging is handled inside "offlineRedirectElsevierToScienceDirect()".
				throw new FailedToProcessScienceDirectException();	// Throw the exception to avoid the connection.
				//logger.debug("Produced ScienceDirect-url: " + pageUrl);	// DEBUG!
			}
			wasLinkinghubElsevier = true;
		}

		if ( wasLinkinghubElsevier || (pageUrl.contains("sciencedirect.com") && !pageUrl.endsWith("/pdf")) )
			return (pageUrl + (pageUrl.endsWith("/") ? "pdf" : "/pdf"));	// Add a "/pdf" in the end. That will indicate we are asking for the docUrl.
		else
			return null;	// This indicates that the calling method will not replace the url.
	}


	/**
	 * This method receives a url from "linkinghub.elsevier.com" and returns it's matched url in "sciencedirect.com".
	 * We do this because the "linkinghub.elsevier.com" urls have a javaScript redirect inside which we are not able to handle without doing html scraping.
	 * If there is any error this method returns the URL it first received.
	 * @param linkinghubElsevierUrl
	 * @return
	 */
	public static String offlineRedirectElsevierToScienceDirect(String linkinghubElsevierUrl)
	{
		String idStr = UrlUtils.getDocIdStr(linkinghubElsevierUrl, null);
		if ( idStr != null )
			return (scienceDirectBasePath + idStr);
		else
			return null;
	}
	
}
