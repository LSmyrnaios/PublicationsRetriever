package eu.openaire.doc_urls_retriever.crawler;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.url.URLCanonicalizer;
import edu.uci.ics.crawler4j.url.WebURL;
import eu.openaire.doc_urls_retriever.util.http.HttpUtils;
import eu.openaire.doc_urls_retriever.util.url.UrlUtils;



public class PageCrawler extends WebCrawler
{
	private static final Logger logger = LogManager.getLogger(PageCrawler.class);


	@Override
	public boolean shouldVisit(Page referringPage, WebURL url)
	{
		String urlStr = url.toString();
		String lowerCaseUrlStr = urlStr.toLowerCase();

		if ( lowerCaseUrlStr.contains("elsevier.com") ) {   // Avoid this JavaScript site with non accesible dynamic links.
            UrlUtils.elsevierLinks ++;
			UrlUtils.logUrl(urlStr, "unreachable", "Discarded in PageCrawler.shouldVisit() method, after matching to the JavaScript site: \"elsevier.com\".");
            return false;
		}
		else if ( lowerCaseUrlStr.contains("doaj.org/toc/") ) {	// Avoid resultPages.
			UrlUtils.doajResultPageLinks ++;
			UrlUtils.logUrl(urlStr, "unreachable", "Discarded in PageCrawler.shouldVisit() method, after matching to the Results-directory: \"doaj.org/toc/\".");
			return false;
		}
		else if ( lowerCaseUrlStr.contains("dlib.org") ) {    // Avoid HTML docUrls.
			UrlUtils.dlibHtmlDocUrls ++;
			UrlUtils.logUrl(urlStr, "unreachable", "Discarded in PageCrawler.shouldVisit() method, after matching to the HTML-docUrls site: \"dlib.org\".");
			return false;
		}
		else if ( UrlUtils.SPECIFIC_DOMAIN_FILTER.matcher(lowerCaseUrlStr).matches()
					|| UrlUtils.PAGE_FILE_EXTENSION_FILTER.matcher(lowerCaseUrlStr).matches()
					||UrlUtils.URL_DIRECTORY_FILTER.matcher(lowerCaseUrlStr).matches() )
		{
			UrlUtils.logUrl(urlStr, "unreachable", "Discarded in PageCrawler.shouldVisit() method, after matching to unwantedType-rules.");
			return false;
		}
		else
			return true;
	}
	
	
	public boolean shouldNotCheckInnerLink(String referringPageDomain, String linkStr)
	{
		String lowerCaseLink = linkStr.toLowerCase();

		return	!lowerCaseLink.contains(referringPageDomain)	// Don't check this link if it belongs in a different domain than the referringPage's one.
				|| lowerCaseLink.contains("mailto:")
				|| UrlUtils.SPECIFIC_DOMAIN_FILTER.matcher(lowerCaseLink).matches()
				|| UrlUtils.PLAIN_DOMAIN_FILTER.matcher(lowerCaseLink).matches()
				|| UrlUtils.URL_DIRECTORY_FILTER.matcher(lowerCaseLink).matches()
				|| UrlUtils.INNER_LINKS_FILE_EXTENSION_FILTER.matcher(lowerCaseLink).matches()
				|| UrlUtils.INNER_LINKS_FILE_FORMAT_FILTER.matcher(lowerCaseLink).matches()
				|| UrlUtils.PLAIN_PAGE_EXTENSION_FILTER.matcher(lowerCaseLink).matches();
	}
	
	
	@Override
	public boolean shouldFollowLinksIn(WebURL url)
	{
		return false;	// We don't want any inner lnks to be followed for crawling.
	}
	
	
	@Override
	public void visit(Page page)
	{
		String pageUrl = page.getWebURL().getURL();

		logger.debug("Checking pageUrl: \"" + pageUrl + "\".");

		if ( pageUrl.contains("doaj.org/toc/") ) {	// Re-check here for these resultPages, as it seems that Crawler4j has a bug in handling "shouldVisit()" method.
			logger.debug("Not visiting: " + pageUrl + " as per your \"shouldVisit\" policy (used a workaround for Crawler4j bug)");
			UrlUtils.doajResultPageLinks ++;
			UrlUtils.logUrl(pageUrl, "unreachable", "Discarded in PageCrawler.visit() method, after matching to the Results-directory: \"doaj.org/toc/\".");
			return;
		}

		String pageContentType = page.getContentType();

		if ( UrlUtils.hasDocMimeType(pageUrl, pageContentType) ) {
			UrlUtils.logUrl(pageUrl, pageUrl, "");
			return;
		}
		
		String currentPageDomain = UrlUtils.getDomainStr(pageUrl);
		
		HttpUtils.lastConnectedHost = currentPageDomain;	// The crawler opened a connection to download this page.

		if ( HttpUtils.blacklistedDomains.contains(currentPageDomain) ) {	// Check if it has been blackListed after running inner links' checks.
			logger.debug("Avoid crawling blackListed domain: \"" + currentPageDomain + "\"");
			UrlUtils.logUrl(pageUrl, "unreachable", "Discarded in PageCrawler.visit() method, as its domain was found blackListed.");
			return;
		}

	    // Check if we can find the docUrl based on previous runs. (Still in experimental stage)
		if ( UrlUtils.useMLA )
	    	if ( UrlUtils.guessInnerDocUrl(pageUrl) )	// If we were able to find the right path.. and hit a docUrl successfully.. return.
    			return;
        
	    Set<WebURL> currentPageLinks = page.getParseData().getOutgoingUrls();

		//logger.debug("Num of links in: \"" + pageUrl + "\" is: " + currentPageLinks.size());

		if ( currentPageLinks.isEmpty() ) {	// If no links were retrieved (e.g. the pageUrl was some kind of non-page binary content)
			logger.warn("No links were able to be retrieved from pageUrl: \"" + pageUrl + "\". Its contentType is: " + pageContentType);
			UrlUtils.logUrl(pageUrl, "unreachable", "Discarded in PageCrawler.visit() method, as no links were able to be retrieved from it. Its contentType is: \"" + pageContentType + "\"");
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

            if ( UrlUtils.duplicateUrls.contains(urlToCheck) )
                continue;

            if ( UrlUtils.docUrls.contains(urlToCheck) ) {	// If we got into an already-found docUrl, log it and return.
				logger.debug("Re-crossing the already found url: \"" +  urlToCheck + "\"");
                UrlUtils.logUrl(pageUrl, urlToCheck, "");	// No error here.
                return;
            }

            Matcher docUrlMatcher = UrlUtils.DOC_URL_FILTER.matcher(urlToCheck.toLowerCase());
            if ( docUrlMatcher.matches() )
            {
                try {
                    if ( HttpUtils.connectAndCheckMimeType(pageUrl, urlToCheck, null) ) {		// We log the docUrl inside this method.
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
				if ( HttpUtils.connectAndCheckMimeType(pageUrl, currentLink, null) )	// We log the docUrl inside this method.
					return;
			} catch (RuntimeException e) {
				// No special handling here.. nor logging..
			}
		}	// end for-loop

		// If we get here it means that this pageUrl is not a docUrl itself, nor it contains a docUrl..
		logger.warn("Page: \"" + pageUrl + "\" does not contain a docUrl.");
		UrlUtils.logUrl(pageUrl, "unreachable", "Logged in PageCrawler.visit() method, as no docUrl was found inside.");
	}


	@Override
	public void onUnexpectedStatusCode(String urlStr, int statusCode, String contentType, String description)
	{
		// Call our general statusCode-handling method (it will also find the domainStr).
		HttpUtils.onErrorStatusCode(urlStr, null, statusCode);
		UrlUtils.logUrl(urlStr, "unreachable", "Logged in PageCrawler.onUnexpectedStatusCode() method, after returning: " + statusCode + " errorCode.");
	}


	@Override
	public void onContentFetchError(WebURL webUrl)
	{
		String urlStr = webUrl.toString();
		logger.warn("Can't fetch content of: \"" + urlStr + "\"");
		UrlUtils.logUrl(urlStr, "unreachable", "Logged in PageCrawler.onContentFetchError() method, as no content was able to be fetched for this page.");
	}


	@Override
	protected void onParseError(WebURL webUrl)
	{
		String urlStr = webUrl.toString();
		logger.warn("Parsing error of: \"" + urlStr + "\"" );
		UrlUtils.logUrl(urlStr, "unreachable", "Logged in PageCrawler.onParseError(() method, as there was a problem parsing this page.");
	}


	@Override
	public void onUnhandledException(WebURL webUrl, Throwable e)
	{
		if ( webUrl != null )
		{
			String urlStr = webUrl.toString();
			logger.warn("Unhandled exception while fetching: \"" + urlStr + "\"" );
			UrlUtils.logUrl(urlStr, "unreachable", "Logged in PageCrawler.onUnhandledException() method, as there was an unhandled exception with this url: " + e);
		}
		logger.warn(e);
	}


	@Override
	public void onPageBiggerThanMaxSize(String urlStr, long pageSize)
	{
		long generalPageSizeLimit = CrawlerController.controller.getConfig().getMaxDownloadSize();
		logger.warn("Skipping url: \"" + urlStr + "\" which was bigger (" + pageSize +") than max allowed size (" + generalPageSizeLimit + ")");
		UrlUtils.logUrl(urlStr, "unreachable", "Logged in PageCrawler.onPageBiggerThanMaxSize() method, as this page's size was over the limit (" + generalPageSizeLimit + ").");
	}

}
