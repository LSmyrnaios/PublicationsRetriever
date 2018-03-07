package eu.openaire.doc_urls_retriever.crawler;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.url.URLCanonicalizer;
import edu.uci.ics.crawler4j.url.WebURL;
import eu.openaire.doc_urls_retriever.util.http.HttpUtils;
import eu.openaire.doc_urls_retriever.util.url.UrlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author Lampros A. Smyrnaios
 */
public class PageCrawler extends WebCrawler
{
	private static final Logger logger = LoggerFactory.getLogger(PageCrawler.class);
	public static long totalPagesReachedCrawling = 0;	// This counts the pages which reached the crawlingStage, i.e: were not discarded in any case and waited to have their innerLinks checked.

	@Override
	public boolean shouldVisit(Page referringPage, WebURL url)
	{
		String urlStr = url.toString();
		String lowerCaseUrlStr = urlStr.toLowerCase();
		
		// Check  redirect-finished-urls for certain unwanted types.
		// Note that "elsevier.com" is reached after redirections only and that it's an intermediate site itself.. so it isn't found at loading time.
		if ( lowerCaseUrlStr.contains("elsevier.com") ) {   // Avoid this JavaScript site with non accesible dynamic links.
            UrlUtils.elsevierLinks ++;
			UrlUtils.logTriple(urlStr, "unreachable", "Discarded in PageCrawler.shouldVisit() method, after matching to the JavaScript site: \"elsevier.com\".");
            return false;
		}
		else if ( UrlUtils.SPECIFIC_DOMAIN_FILTER.matcher(lowerCaseUrlStr).matches()
					|| UrlUtils.PAGE_FILE_EXTENSION_FILTER.matcher(lowerCaseUrlStr).matches()
					||UrlUtils.URL_DIRECTORY_FILTER.matcher(lowerCaseUrlStr).matches() )
		{
			UrlUtils.logTriple(urlStr, "unreachable", "Discarded in PageCrawler.shouldVisit() method, after matching to unwantedType-rules.");
			return false;
		}
		else
			return true;
	}
	
	
	public boolean shouldNotCheckInnerLink(String referringPageDomain, String linkStr)
	{
		String lowerCaseLink = linkStr.toLowerCase();
		
		return	lowerCaseLink.contains("mailto:")
				|| UrlUtils.SPECIFIC_DOMAIN_FILTER.matcher(lowerCaseLink).matches()
				|| UrlUtils.PLAIN_DOMAIN_FILTER.matcher(lowerCaseLink).matches()
				|| UrlUtils.URL_DIRECTORY_FILTER.matcher(lowerCaseLink).matches()
				|| UrlUtils.INNER_LINKS_FILE_EXTENSION_FILTER.matcher(lowerCaseLink).matches()
				|| UrlUtils.INNER_LINKS_FILE_FORMAT_FILTER.matcher(lowerCaseLink).matches()
				|| UrlUtils.PLAIN_PAGE_EXTENSION_FILTER.matcher(lowerCaseLink).matches();
		
		// The following check is obsolete here, as we already use it inside "visit()" method. Still keep it here, as it makes our intentions clearer.
		// !lowerCaseLink.contains(referringPageDomain)	// Don't check this link if it belongs in a different domain than the referringPage's one.
	}
	
	
	@Override
	public boolean shouldFollowLinksIn(WebURL url)
	{
		return false;	// We don't want any inner links to be followed for crawling.
	}
	
	
	@Override
	public void visit(Page page)
	{
		String pageUrl = page.getWebURL().getURL();
		
		logger.debug("Checking pageUrl: \"" + pageUrl + "\".");
		
		String currentPageDomain = UrlUtils.getDomainStr(pageUrl);
		if ( currentPageDomain == null ) {    // If the domain is not found, it means that a serious problem exists with this docPage.
			logger.debug("Problematic URL in \"visit()\": \"" +  pageUrl + "\"");
			UrlUtils.logTriple(pageUrl, pageUrl, "Discarded in visit() method, after the occurrence of a domain-retrieval error.");
			return;
		}
		
		HttpUtils.lastConnectedHost = currentPageDomain;	// The crawler opened a connection to download this page.
		
		if ( UrlUtils.docUrls.contains(pageUrl) ) {	// If we got into an already-found docUrl, log it and return.
			logger.debug("Re-crossing the already found docUrl: \"" +  pageUrl + "\"");
			UrlUtils.logTriple(pageUrl, pageUrl, "");	// No error here.
			return;
		}

		String pageContentType = page.getContentType();

		if ( UrlUtils.hasDocMimeType(pageUrl, pageContentType) ) {
			UrlUtils.logTriple(pageUrl, pageUrl, "");
			return;
		}
		
		if ( HttpUtils.blacklistedDomains.contains(currentPageDomain) ) {	// Check if it has been blackListed after running inner links' checks.
			logger.debug("Avoid crawling blackListed domain: \"" + currentPageDomain + "\"");
			UrlUtils.logTriple(pageUrl, "unreachable", "Discarded in PageCrawler.visit() method, as its domain was found blackListed.");
			return;
		}
		
		PageCrawler.totalPagesReachedCrawling ++;	// Used for M.L.A.'s execution-manipulation.
		
	    // Check if we can use AND if we should run, the MLA.
		if ( MachineLearning.useMLA )
			if ( MachineLearning.shouldRunMLA(currentPageDomain) )
	    		if (MachineLearning.guessInnerDocUrlUsingML(pageUrl, currentPageDomain) )	// Check if we can find the docUrl based on previous runs. (Still in experimental stage)
    				return;	// If we were able to find the right path.. and hit a docUrl successfully.. return.
        
	    Set<WebURL> currentPageLinks = page.getParseData().getOutgoingUrls();

		//logger.debug("Num of links in: \"" + pageUrl + "\" is: " + currentPageLinks.size());

		if ( currentPageLinks.isEmpty() ) {	// If no links were retrieved (e.g. the pageUrl was some kind of non-page binary content)
			logger.warn("No links were able to be retrieved from pageUrl: \"" + pageUrl + "\". Its contentType is: " + pageContentType);
			UrlUtils.logTriple(pageUrl, "unreachable", "Discarded in PageCrawler.visit() method, as no links were able to be retrieved from it. Its contentType is: \"" + pageContentType + "\"");
			return;
		}

		HashSet<String> curLinksStr = new HashSet<String>();	// HashSet to store the String version of each link.

		String urlToCheck = null;

		// Check if urls inside this page, match to a docUrl regex, if they do, try connecting with them and see if they truly are docUrls. If they are, return.
		for ( WebURL link : currentPageLinks )
		{
            // Produce fully functional inner links, NOT inner paths or non-canonicalized.
            // (Crawler4j doesn't canonicalize the urls when it takes them, it does this only if it visit them, depending on "shouldFollowLinksIn()" method.)
            // See "Parser.java" and "Net.java", in Crawler4j files, for more info.
            String currentLink = link.toString();
            if ( (urlToCheck = URLCanonicalizer.getCanonicalURL(currentLink, pageUrl)) == null ) {	// Fix potential encoding problems.
                logger.debug("Could not cannonicalize inner url: " + currentLink);
                UrlUtils.duplicateUrls.add(currentLink);
                continue;
            }
			
			if ( !urlToCheck.contains(currentPageDomain) )	// Make sure we check only links from current page's domain.
				continue;
			
            if ( UrlUtils.duplicateUrls.contains(urlToCheck) )
                continue;
			
            if ( UrlUtils.docUrls.contains(urlToCheck) ) {	// If we got into an already-found docUrl, log it and return.
				logger.debug("Re-crossing the already found docUrl: \"" +  urlToCheck + "\"");
                UrlUtils.logTriple(pageUrl, urlToCheck, "");	// No error here.
                return;
            }
            
            Matcher docUrlMatcher = UrlUtils.DOC_URL_FILTER.matcher(urlToCheck.toLowerCase());
            if ( docUrlMatcher.matches() )
            {
                try {
                    if ( HttpUtils.connectAndCheckMimeType(pageUrl, urlToCheck, currentPageDomain) ) {		// We log the docUrl inside this method.
                        //logger.debug("\"DOC_URL_FILTER\" revealed a docUrl in pageUrl: \"" + pageUrl + "\", after matching to: \"" + docUrlMatcher.group(1) + "\"");
                        return;
                    }
                    else
                        continue;	// Don't add it in the new set.
                } catch (RuntimeException re) {
                    UrlUtils.duplicateUrls.add(urlToCheck);	// Don't check it ever again..
                    continue;
                }
            }
			
			curLinksStr.add(urlToCheck);	// Keep the string version of this link, in order not to make the transformation later..
			
		}// end for-loop

		// If we reached here, it means that we couldn't find a docUrl the quick way.. so we have to check some (we exclude lots of them) of the inner links one by one.

		currentPageLinks.clear();	// Free-up non-needed memory.

		for ( String currentLink : curLinksStr )
		{
			if ( shouldNotCheckInnerLink(currentPageDomain, currentLink) ) {	// If this link matches certain blackListed criteria, move on..
				//logger.debug("Avoided link: " + currentLink );
				UrlUtils.duplicateUrls.add(currentLink);
				continue;
			}

			try {
				if ( HttpUtils.connectAndCheckMimeType(pageUrl, currentLink, currentPageDomain) )	// We log the docUrl inside this method.
					return;
			} catch (RuntimeException e) {
				// No special handling here.. nor logging..
			}
		}	// end for-loop

		// If we get here it means that this pageUrl is not a docUrl itself, nor it contains a docUrl..
		logger.warn("Page: \"" + pageUrl + "\" does not contain a docUrl.");
		UrlUtils.logTriple(pageUrl, "unreachable", "Logged in PageCrawler.visit() method, as no docUrl was found inside.");
	}


	@Override
	public void onUnexpectedStatusCode(String urlStr, int statusCode, String contentType, String description)
	{
		// Call our general statusCode-handling method (it will also find the domainStr).
		HttpUtils.onErrorStatusCode(urlStr, null, statusCode);
		UrlUtils.logTriple(urlStr, "unreachable", "Logged in PageCrawler.onUnexpectedStatusCode() method, after returning: " + statusCode + " errorCode.");
	}


	@Override
	public void onContentFetchError(WebURL webUrl)
	{
		String urlStr = webUrl.toString();
		logger.warn("Can't fetch content of: \"" + urlStr + "\"");
		UrlUtils.logTriple(urlStr, "unreachable", "Logged in PageCrawler.onContentFetchError() method, as no content was able to be fetched for this page.");
	}


	@Override
	protected void onParseError(WebURL webUrl)
	{
		String urlStr = webUrl.toString();
		logger.warn("Parsing error of: \"" + urlStr + "\"" );
		UrlUtils.logTriple(urlStr, "unreachable", "Logged in PageCrawler.onParseError(() method, as there was a problem parsing this page.");
	}


	@Override
	public void onUnhandledException(WebURL webUrl, Throwable e)
	{
		if ( webUrl != null )
		{
			String urlStr = webUrl.toString();
			String exceptionMessage = e.getMessage();
			
			int curTreatableException = 0;
			
			if (exceptionMessage.contains("UnknownHostException"))
				curTreatableException = 1;
			else if (exceptionMessage.contains("SocketTimeoutException"))
				curTreatableException = 2;
			else if (exceptionMessage.contains("ConnectException"))    // This is a "Connection Timeout" type of Exception.
				curTreatableException = 3;
			
			if (curTreatableException > 0)	// If there is a treatable Exception.
			{
				String domainStr = UrlUtils.getDomainStr(urlStr);
				if (domainStr != null)
				{
					if (curTreatableException == 1)
						HttpUtils.blacklistedDomains.add(domainStr);
					else // TODO - More checks to be added if more exceptions are treated here in the future.
						HttpUtils.onTimeoutException(domainStr);
				}
				
				// Log the right messages for these exceptions.
				switch ( curTreatableException ) {
					case 1:
						logger.warn("UnknownHostException was thrown while trying to fetch url: \"" + urlStr + "\".");
						UrlUtils.logTriple(urlStr, "unreachable", "Logged in PageCrawler.onUnhandledException() method, as there was an \"UnknownHostException\" for this url.");
						break;
					case 2:
						logger.warn("SocketTimeoutException was thrown while trying to fetch url: \"" + urlStr + "\".");
						UrlUtils.logTriple(urlStr, "unreachable", "Logged in PageCrawler.onUnhandledException() method, as there was an \"SocketTimeoutException\" for this url.");
						break;
					case 3:
						logger.warn("ConnectException was thrown while trying to fetch url: \"" + urlStr + "\".");
						UrlUtils.logTriple(urlStr, "unreachable", "Logged in PageCrawler.onUnhandledException() method, as there was an \"ConnectException\" for this url.");
						break;
					default:
						logger.error("Undefined value for \"curTreatableException\"! Re-check which exception are treated!");
						break;
				}
			}
			else {	// If this Exception cannot be treated.
				logger.warn("Unhandled exception: \"" + e + "\" while fetching url: \"" + urlStr + "\"");
				UrlUtils.logTriple(urlStr, "unreachable", "Logged in PageCrawler.onUnhandledException() method, as there was an unhandled exception: " + e);
			}
		}
		else // If the url is null.
			logger.warn("", e);
	}


	@Override
	public void onPageBiggerThanMaxSize(String urlStr, long pageSize)
	{
		long generalPageSizeLimit = CrawlerController.controller.getConfig().getMaxDownloadSize();
		logger.warn("Skipping url: \"" + urlStr + "\" which was bigger (" + pageSize +") than max allowed size (" + generalPageSizeLimit + ")");
		UrlUtils.logTriple(urlStr, "unreachable", "Logged in PageCrawler.onPageBiggerThanMaxSize() method, as this page's size was over the limit (" + generalPageSizeLimit + ").");
	}

}
