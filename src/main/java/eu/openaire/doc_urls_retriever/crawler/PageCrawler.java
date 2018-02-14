package eu.openaire.doc_urls_retriever.crawler;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.URLCanonicalizer;
import edu.uci.ics.crawler4j.url.WebURL;
import eu.openaire.doc_urls_retriever.util.http.HttpUtils;
import eu.openaire.doc_urls_retriever.util.url.UrlUtils;



@SuppressWarnings("ALL")
public class PageCrawler extends WebCrawler
{
	private static final Logger logger = LogManager.getLogger(PageCrawler.class);


	@Override
	protected WebURL handleUrlBeforeProcess(WebURL curURL)
	{
		String urlStr = curURL.toString();

		// Handle "elsevier.com" urls which contain javaScriptRedirect..
		// TODO - We also have to deal with docLinks been only showd with javaScript.
		if ( urlStr.contains("elsevier.com") ) {
			String retUrl = null;
			if ( (retUrl = silentRedirectElsevierToScienseRedirect(urlStr)) != null) {
				WebURL url = new WebURL();
				url.setURL(retUrl);
				curURL = url;
			}
		}
		return curURL;
	}
	
	@Override
	public boolean shouldVisit(Page referringPage, WebURL url)
	{
		String lowerCaseUrlStr = url.toString().toLowerCase();
		
		return !UrlUtils.SPECIFIC_DOMAIN_FILTER.matcher(lowerCaseUrlStr).matches()
				&& !UrlUtils.PAGE_FILE_EXTENSION_FILTER.matcher(lowerCaseUrlStr).matches() && !UrlUtils.URL_DIRECTORY_FILTER.matcher(lowerCaseUrlStr).matches();
	}
	
	
	public boolean shouldNotCheckInnerLink(String referringPageDomain, String linkStr)
	{
		String lowerCaseLink = linkStr.toLowerCase();
		
		return	!lowerCaseLink.contains(referringPageDomain)	// Don't check this link if it belongs in a different domain than the referringPage's one.
				|| lowerCaseLink.contains("citation") || lowerCaseLink.contains("mailto:") || lowerCaseLink.contains("instruction") || lowerCaseLink.contains("manual")
				|| lowerCaseLink.contains("://doi.org/")	// Plain doi.org links are not docUrl by themselves.. (we still accept them to be crawled (if given at loading time), but we don't need to check if they are docUrls themselves)
				|| UrlUtils.URL_DIRECTORY_FILTER.matcher(lowerCaseLink).matches() || UrlUtils.SPECIFIC_DOMAIN_FILTER.matcher(lowerCaseLink).matches()
				|| UrlUtils.PLAIN_DOMAIN_FILTER.matcher(lowerCaseLink).matches() || UrlUtils.PLAIN_FIRST_LEVEL_DIRECTORY_FILTER.matcher(lowerCaseLink).matches()
				|| UrlUtils.INNER_LINKS_FILE_EXTENSION_FILTER.matcher(lowerCaseLink).matches() || UrlUtils.INNER_LINKS_FILE_FORMAT_FILTER.matcher(lowerCaseLink).matches();
	}
	
	
	@Override
	public boolean shouldFollowLinksIn(WebURL url)
	{
		return false;	// We don't want any inner lnks to be followed for crawling.
	}
	
	
	@Override
	public void visit(Page page)
	{	
	    // No need to check if this page itself is a docUrl since we have already checked it when we were loading the urls from the inputFile.
	    
		String pageUrl = page.getWebURL().getURL();
		
		String currentPageDomain = UrlUtils.getDomainStr(pageUrl);
		
		HttpUtils.lastConnectedHost = currentPageDomain;	// The crawler opened a connection to download it's data.
		
	    // Check if we can find the docUrl based on previous runs.
    	if ( UrlUtils.guessInnerDocUrl(pageUrl) )	// If we were able to find the right path.. and hit a docUrl successfully.. return.
    		return;
        
	    if (page.getParseData() instanceof HtmlParseData)	// If the page is crawlable..
	    {
		    logger.debug("Crawl URL: " + pageUrl);
	        HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
	        Set<WebURL> currentPageLinks = htmlParseData.getOutgoingUrls();
		    
	        HashSet<String> curLinksStr = new HashSet<String>();	// HashSet to store the String version of each link.
	        
	        String lowerCaseLink = null;
	        String urlToCheck = null;
	        
	        
	        // Check if urls inside this page, match to a docUrl regex, if they do, try connecting with them and see if they truly are docUrls.
		    for ( WebURL link : currentPageLinks )
		    {
		    	// Produce fully functional inner links, NOT inner paths. (Make sure that Crawler4j doesn't handle that already..)
				try {
					URL base = new URL(pageUrl);
					URL targetUrl = new URL(base, link.toString());	// Combine base (domain) and resource (inner link), to produce the final link.
					String currentLink  = targetUrl.toString();
					
			    	if ( UrlUtils.duplicateUrls.contains(urlToCheck) )
			    		continue;
			    	
					if ( (urlToCheck = URLCanonicalizer.getCanonicalURL(currentLink) ) == null ) {	// Fix potential encoding problems.
						logger.debug("Could not cannonicalize inner url: " + currentLink);
						UrlUtils.duplicateUrls.add(currentLink);
						continue;
					}
					
			    	if ( UrlUtils.docUrls.contains(urlToCheck) ) {	// If we got into an already-found docUrl, log it and return.
			    		UrlUtils.logUrl(pageUrl, urlToCheck);
			    		logger.debug("Re-crossing the already found url: \"" +  urlToCheck + "\"");
			    		return;
			    	}
			    	
			    	curLinksStr.add(urlToCheck);	// Add keep the string version of this link, useful for later.
			    	
			    	lowerCaseLink = urlToCheck.toLowerCase();
			    	
			    	Matcher docUrlMatcher = UrlUtils.DOC_URL_FILTER.matcher(lowerCaseLink);
			    	if ( docUrlMatcher.matches() && !lowerCaseLink.contains("citation") )// If matches the "DOC_URL_FILTER" and it's not a citation url, go check if it's
			    	{
						if ( UrlUtils.checkIfDocMimeType(pageUrl, urlToCheck, null, false) ) {
							//logger.debug("\"DOC_URL_FILTER\" revealed a docUrl in pageUrl: \"" + pageUrl + "\", after matching to: \"" + docUrlMatcher.group(1) + "\"");
							return;
						}
			    	}// end if
				} catch (RuntimeException re) {
					UrlUtils.duplicateUrls.add(urlToCheck);	// Don't check it ever again..
				} catch (MalformedURLException me) {
					logger.warn(me);
				}
		    }// end for-loop
		    
	    	
		    // If we reached here, it means that we couldn't find a docUrl the quick way.. so we have to check some (we exclude lots of them) of the inner links one by one.
	        
	        
	        //logger.debug("Num of links in \"" + pageUrl + "\" is: " + currentPageLinks.size());
	        
			for ( String currentLink : curLinksStr )
			{
				if ( shouldNotCheckInnerLink(currentPageDomain, currentLink) )	// If this link matches certain blackListed criteria, move on..
					continue;
				
				if ( (urlToCheck = URLCanonicalizer.getCanonicalURL(currentLink) ) == null ) {	// Fix potential encoding problems.
					logger.debug("Could not cannonicalize inner url: " + currentLink);
					UrlUtils.duplicateUrls.add(currentLink);
					continue;	// Could not canonicalize this url! Move on..
				}
				
		    	if ( UrlUtils.docUrls.contains(urlToCheck) ) {	// If we got into an already-found docUrl, log it and return.
		    		UrlUtils.logUrl(pageUrl, currentLink);
		    		logger.debug("Re-crossing the already found url: \"" +  urlToCheck + "\"");
		    		return;
		    	}
				
				if ( UrlUtils.duplicateUrls.contains(urlToCheck) )
					continue;	// Don't check already seen links.
				
				try {
					if ( UrlUtils.checkIfDocMimeType(pageUrl, urlToCheck, null, false) )
						return;
				} catch (RuntimeException e) {
					// No special handling here.. or logging..
				}
			}	// end for-loop
			
			// If we get here it means that this pageLink is not a pdfLink.. So add blank next to it..
			UrlUtils.logUrl(pageUrl, "unreachable");
	    }
	    else {	// This page is not crawlable.. nor we were able to guess its docUrl log it.
	    	logger.warn("Pageurl: \"" + pageUrl + "\" is not crawlable! It's contentType is: \"" + page.getContentType() + "\".");
	    	UrlUtils.logUrl(pageUrl, "unreachable");
	    }
	}


	/**
	 * This method recieves a url from "elsevier.com" and returns it's matched url in "sciencedirect.com".
	 * We do this because the "elsevier.com" urls have a javaScript redirect inside which we are not able to handle without doing html scraping.
	 * If there is any error this method returns the string it first recieved.
	 * @param elsevierUrl
	 * @return
	 */
	public static String silentRedirectElsevierToScienseRedirect(String elsevierUrl)
	{
		if ( !elsevierUrl.contains("elsevier.com") ) // If this method was called for the wrong url, then just return it.
			return elsevierUrl;

		String idStr = null;
		Matcher matcher = UrlUtils.URL_TRIPLE.matcher(elsevierUrl);
		if ( matcher.matches() ) {
			idStr = matcher.group(3);
			if ( idStr == null || idStr.isEmpty() ) {
				logger.warn("Unexpected id-missing case for: " + elsevierUrl);
				return elsevierUrl;
			}
		}
		else {
			logger.warn("Unexpected \"URL_TRIPLE\" mismatch for: " + elsevierUrl);
			return elsevierUrl;
		}

		return ("https://www.sciencedirect.com/science/article/pii/" + idStr);
	}
    
}
