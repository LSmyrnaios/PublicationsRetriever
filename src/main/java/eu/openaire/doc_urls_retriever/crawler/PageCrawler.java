package eu.openaire.doc_urls_retriever.crawler;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.TextParseData;
import edu.uci.ics.crawler4j.url.URLCanonicalizer;
import edu.uci.ics.crawler4j.url.WebURL;
import edu.uci.ics.crawler4j.util.Net;
import eu.openaire.doc_urls_retriever.exceptions.ConnTimeoutException;
import eu.openaire.doc_urls_retriever.exceptions.DocFileNotRetrievedException;
import eu.openaire.doc_urls_retriever.exceptions.DomainBlockedException;
import eu.openaire.doc_urls_retriever.exceptions.DomainWithUnsupportedHEADmethodException;
import eu.openaire.doc_urls_retriever.util.file.FileUtils;
import eu.openaire.doc_urls_retriever.util.http.HttpUtils;
import eu.openaire.doc_urls_retriever.util.url.UrlUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author Lampros A. Smyrnaios
 */
public class PageCrawler extends WebCrawler
{
	private static final Logger logger = LoggerFactory.getLogger(PageCrawler.class);
	public static long totalPagesReachedCrawling = 0;	// This counts the pages which reached the crawlingStage, i.e: were not discarded in any case and waited to have their innerLinks checked.
	
	
	/**
	 * This method checks if the url, for which Crawler4j is going to open a connection, is of specific type in runtime.
	 * This method doesn't check for general-unwantedUrlTypes, since every url passes from here, has already been checked against those types, at loading.
	 * If it is, it returns null and Crawler4j goes to the next one. If this url is not matched against any specific case, it returns the url itself.
	 * @param curURL
	 * @return curURL / null
	 */
	@Override
	public WebURL handleUrlBeforeProcess(WebURL curURL)
	{
		String urlStr = curURL.toString();
		
		String currentUrlDomain = UrlUtils.getDomainStr(urlStr);
		if ( currentUrlDomain == null ) {    // If the domain is not found, it means that a serious problem exists with this docPage and we shouldn't crawl it.
			logger.warn("Problematic URL in \"PageCrawler.handleUrlBeforeProcess()\": \"" +  urlStr + "\"");
			UrlUtils.logTriple(urlStr, urlStr, "Discarded in PageCrawler.handleUrlBeforeProcess() method, after the occurrence of a domain-retrieval error.", null);
			return null;
		}
		
		if ( UrlUtils.docUrls.contains(urlStr) ) {	// If we got into an already-found docUrl, log it and return.
			logger.debug("Re-crossing (before connecting to it) the already found docUrl: \"" +  urlStr + "\"");
			if ( FileUtils.shouldDownloadDocFiles )
				UrlUtils.logTriple(urlStr, urlStr, "This file is probably already downloaded.", currentUrlDomain);
			else
				UrlUtils.logTriple(urlStr, urlStr, "", currentUrlDomain);
			return null;	//Skip this url from connecting and crawling.
		}
		
		if ( HttpUtils.blacklistedDomains.contains(currentUrlDomain) ) {	// Check if it has been blackListed after running inner links' checks.
			logger.debug("Crawler4j will avoid to connect to blackListed domain: \"" + currentUrlDomain + "\"");
			UrlUtils.logTriple(urlStr, "unreachable", "Discarded in PageCrawler.handleUrlBeforeProcess() method, as its domain was found blackListed.", null);
			return null;
		}
		
		if ( HttpUtils.checkIfPathIs403BlackListed(urlStr, currentUrlDomain) ) {
			logger.warn("Preventing reaching 403ErrorCode with url: \"" + urlStr + "\"!");
			return null;
		}
		
		// Handle the weird-case of: "ir.lib.u-ryukyu.ac.jp"
		// See: http://ir.lib.u-ryukyu.ac.jp/handle/123456789/8743
		// Note that this is NOT the case for all of the urls containing "/handle/123456789/".. but just for this domain.
		if ( urlStr.contains("ir.lib.u-ryukyu.ac.jp") && urlStr.contains("/handle/123456789/") )
			curURL.setURL(StringUtils.replace(urlStr, "/123456789/", "/20.500.12000/"));
		
		return curURL;
	}
	
	
	/**
	 * This method is called by Crawler4j after a "referringPage" is going to be redirected to another "url".
	 * It is also called in case we follow innerLinks of a page (which currently we are not).
	 * It is NOT called before visit() for urls which immediately return 2XX.. so, runtime checks should be positioned both in shouldVisit() and in visit(). It's a bit confusing.
	 * It returns true if this "url" should be scheduled to be connected and crawled by Crawler4j, otherwise, it returns false.
	 * @param referringPage
	 * @param url
	 * @return true / false
	 */
	@Override
	public boolean shouldVisit(Page referringPage, WebURL url)
	{
		String currentPageDomain = null;
		if ( HttpUtils.politenessDelay > 0 ) {
			String pageUrl = referringPage.getWebURL().toString();
			
			// Get this url's domain for checks.
			currentPageDomain = UrlUtils.getDomainStr(pageUrl);
			if ( currentPageDomain != null )
				HttpUtils.lastConnectedHost = currentPageDomain;    // The crawler opened a connection which resulted in 3XX responceCode.
		}
		
		String urlStr = url.toString();
		
		if ( UrlUtils.docUrls.contains(urlStr) ) {	// If we got into an already-found docUrl, log it and return.
			logger.debug("Re-crossing the already found docUrl: \"" + urlStr + "\"");
			if ( FileUtils.shouldDownloadDocFiles )
				UrlUtils.logTriple(urlStr, urlStr, "This file is probably already downloaded.", currentPageDomain);	// Inner methods are responsible to domain-retrieval if "null" is sent instead.
			else
				UrlUtils.logTriple(urlStr, urlStr, "", currentPageDomain);
			return false;
		}
		
		String lowerCaseUrlStr = urlStr.toLowerCase();
		
		return	!UrlUtils.matchesUnwantedUrlType(urlStr, lowerCaseUrlStr);	// The output errorCause is already logged.
	}
	
	
	public static boolean shouldNotAcceptInnerLink(String linkStr)
	{
		String lowerCaseLink = linkStr.toLowerCase();
		
		return	UrlUtils.URL_DIRECTORY_FILTER.matcher(lowerCaseLink).matches() || UrlUtils.INNER_LINKS_KEYWORDS_FILTER.matcher(lowerCaseLink).matches()
				|| UrlUtils.SPECIFIC_DOMAIN_FILTER.matcher(lowerCaseLink).matches() || UrlUtils.PLAIN_DOMAIN_FILTER.matcher(lowerCaseLink).matches()
				|| UrlUtils.INNER_LINKS_FILE_EXTENSION_FILTER.matcher(lowerCaseLink).matches() || UrlUtils.INNER_LINKS_FILE_FORMAT_FILTER.matcher(lowerCaseLink).matches()
				|| UrlUtils.PLAIN_PAGE_EXTENSION_FILTER.matcher(lowerCaseLink).matches()
				|| UrlUtils.CURRENTLY_UNSUPPORTED_DOC_EXTENSION_FILTER.matcher(lowerCaseLink).matches();	// TODO - To be removed when these docExtensions get supported.
		
		// The following checks are obsolete here, as we already use it inside "visit()" method. Still keep it here, as it makes our intentions clearer.
		// !lowerCaseLink.contains(referringPageDomain)	// Don't check this link if it belongs in a different domain than the referringPage's one.
	}
	
	
	@Override
	public boolean shouldFollowLinksIn(WebURL url) {
		return false;	// We don't want any inner links to be followed for crawling.
	}
	
	
	public static String getPageContentDisposition(Page page)
	{
		for ( Header header : page.getFetchResponseHeaders() ) {
			if ( header.getName().equals("Content-Disposition") )
				return header.getValue();
		}
		return null;
	}
	
	
	/**
	 * This method retrieves the needed data to check if this page is a docUrl itself.
	 * @param page
	 * @param pageContentType
	 * @param pageUrl
	 * @return true/false
	 */
	private boolean isPageDocUrlItself(Page page, String pageContentType, String pageUrl)
	{
		String contentDisposition = null;
		if ( pageContentType == null )	// If we can't retrieve the contentType, try the "Content-Disposition".
			contentDisposition = getPageContentDisposition(page);
		
		return	UrlUtils.hasDocMimeType(pageUrl, pageContentType, contentDisposition);
	}
	
	
	/**
	 * This method collects Crawler's connData for this page and it sends it to the "FileUtils.storeDocFile()" for the docFile to be stored.
	 * Note that no reconnection is performed here, as the Crawler is only making "GET" requests.
	 * @param page
	 * @param pageUrl
	 * @throws DocFileNotRetrievedException
	 */
	public static String storeDocFileInsideCrawler(Page page, String pageUrl) throws DocFileNotRetrievedException
	{
		try { return FileUtils.storeDocFile(page.getContentData(), pageUrl, PageCrawler.getPageContentDisposition(page)); }
		catch (Exception e) { throw new DocFileNotRetrievedException(); }
	}
	
	
	@Override
	public void visit(Page page)
	{
		String pageUrl = page.getWebURL().toString();
		
		logger.debug("Visiting pageUrl: \"" + pageUrl + "\".");
		
		String currentPageDomain = UrlUtils.getDomainStr(pageUrl);
		if ( currentPageDomain == null ) {    // If the domain is not found, it means that a serious problem exists with this docPage and we shouldn't crawl it.
			logger.warn("Problematic URL in \"PageCrawler.visit()\": \"" + pageUrl + "\"");
			UrlUtils.logTriple(pageUrl, pageUrl, "Discarded in PageCrawler.visit() method, after the occurrence of a domain-retrieval error.", null);
			return;
		}
		else
			HttpUtils.lastConnectedHost = currentPageDomain;	// The crawler opened a connection to download this page. It's both here and in shouldVisit(), as the visit() method can be called without the shouldVisit to be previously called.
		
		if ( UrlUtils.docUrls.contains(pageUrl) ) {	// If we got into an already-found docUrl, log it and return.
			logger.debug("Re-crossing the already found docUrl: \"" + pageUrl + "\"");
			if ( FileUtils.shouldDownloadDocFiles )
				UrlUtils.logTriple(pageUrl, pageUrl, "This file is probably already downloaded.", currentPageDomain);
			else
				UrlUtils.logTriple(pageUrl, pageUrl, "", currentPageDomain);
			return;
		}
		
		// Check if it's a docUrlItself, maybe we don't need to crawl it.
		String pageContentType = page.getContentType();
		if ( isPageDocUrlItself(page, pageContentType, pageUrl) ) {
			String fullPathFileName = "";
			if ( FileUtils.shouldDownloadDocFiles ) {
				try { fullPathFileName = storeDocFileInsideCrawler(page, pageUrl); }
				catch (DocFileNotRetrievedException dfnde) {
					fullPathFileName = "DocFileNotRetrievedException was thrown before the docFile could be stored."; }
			}
			UrlUtils.logTriple(pageUrl, pageUrl, fullPathFileName, currentPageDomain);
			return;
		}
		
		if ( HttpUtils.blacklistedDomains.contains(currentPageDomain) ) {	// Check if it has been blackListed.
			logger.debug("Avoid crawling blackListed domain: \"" + currentPageDomain + "\"");
			UrlUtils.logTriple(pageUrl, "unreachable", "Discarded in PageCrawler.visit() method, as its domain was found blackListed.", null);
			return;
		}
		
	    // Check if we can use AND if we should run, the MLA.
		if ( MachineLearning.useMLA ) {
			PageCrawler.totalPagesReachedCrawling ++;	// Used for M.L.A.'s execution-manipulation.
			if ( MachineLearning.shouldRunMLA(currentPageDomain) )
				if ( MachineLearning.guessInnerDocUrlUsingML(pageUrl, currentPageDomain) )	// Check if we can find the docUrl based on previous runs. (Still in experimental stage)
					return;	// If we were able to find the right path.. and hit a docUrl successfully.. return.
		}
		
		if ( pageContentType.equals("application/xhtml+xml") ) {	// Unless we set Crawler4j to parse binaryContent, this type is not parsed. TODO - More tests need to be done.
			try {
				parsePageDataWorkAroundForCrawler4j(page, pageUrl, pageContentType);
			} catch (Exception e) {	// Already logged.
				return;
			}
		}
		
	    Set<WebURL> currentPageLinks = page.getParseData().getOutgoingUrls();

		//logger.debug("Num of links in: \"" + pageUrl + "\" is: " + currentPageLinks.size());

		if ( currentPageLinks.isEmpty() ) {	// If no links were retrieved (e.g. the pageUrl was some kind of non-page binary content)
			logger.warn("No links were able to be retrieved from pageUrl: \"" + pageUrl + "\". Its contentType is: " + pageContentType);
			UrlUtils.logTriple(pageUrl, "unreachable", "Discarded in PageCrawler.visit() method, as no links were able to be retrieved from it. Its contentType is: \"" + pageContentType + "\"", null);
			return;
		}
		
		//Check innerLinks for debugging:
		 /*
		 if ( pageUrl.contains("<url>") )
			for ( WebURL url : currentPageLinks )
				logger.debug(url.toString());
		 */
		
		HashSet<String> curLinksStr = new HashSet<String>();	// HashSet to store the String version of each link.
		String urlToCheck = null;
		String lowerCaseUrl = null;
		
		// Do a fast-loop, try connecting only to a handful of promising links first.
		// Check if urls inside this page, match to a docUrl regex, if they do, try connecting with them and see if they truly are docUrls. If they are, return.
		for ( WebURL link : currentPageLinks )
		{
            // Produce fully functional inner links, NOT inner paths or non-canonicalized.
            // (Crawler4j doesn't canonicalize the urls when it takes them, it does this only if it visit them, depending on "shouldFollowLinksIn()" method.)
            // See "Parser.java" and "Net.java", in Crawler4j files, for more info.
            String currentLink = link.toString();
            if ( (urlToCheck = URLCanonicalizer.getCanonicalURL(currentLink, pageUrl, StandardCharsets.UTF_8)) == null ) {	// Fix potential encoding problems.
                logger.warn("Could not cannonicalize inner url: " + currentLink);
                UrlUtils.duplicateUrls.add(currentLink);
                continue;
            }
            
            if ( !urlToCheck.contains(currentPageDomain)	// Make sure we avoid connecting to different domains, loginPages, or tracking links.
				|| urlToCheck.contains("site=") || urlToCheck.contains("linkout") || urlToCheck.contains("login") || urlToCheck.contains("LinkListener") )
            	continue;
			
            if ( UrlUtils.duplicateUrls.contains(urlToCheck) )
                continue;
			
            if ( UrlUtils.docUrls.contains(urlToCheck) ) {	// If we got into an already-found docUrl, log it and return.
				logger.debug("Re-crossing the already found docUrl: \"" +  urlToCheck + "\"");
				if ( FileUtils.shouldDownloadDocFiles )
					UrlUtils.logTriple(pageUrl, urlToCheck, "This file is probably already downloaded.", currentPageDomain);
				else
					UrlUtils.logTriple(pageUrl, urlToCheck, "", currentPageDomain);
                return;
            }
            
            lowerCaseUrl = urlToCheck.toLowerCase();
            if ( UrlUtils.DOC_URL_FILTER.matcher(lowerCaseUrl).matches() )
			{
				if ( shouldNotAcceptInnerLink(urlToCheck) )	// Avoid false-positives, such as images (a common one: ".../pdf.png").
					continue;
				
				//logger.debug("InnerPossibleDocLink: " + urlToCheck);	// DEBUG!
				try {
					if ( HttpUtils.connectAndCheckMimeType(pageUrl, urlToCheck, currentPageDomain, false, true) )	// We log the docUrl inside this method.
						return;
					else
						continue;    // Don't add it in the new set.
				} catch (RuntimeException re) {
					UrlUtils.duplicateUrls.add(urlToCheck);    // Don't check it ever again..
					continue;
				} catch (DomainBlockedException dbe) {
					logger.warn("Page: \"" + pageUrl + "\" left \"PageCrawler.visit()\" after it's domain was blocked.");
					UrlUtils.logTriple(pageUrl, "unreachable", "Logged in PageCrawler.visit() method, as its domain was blocked during crawling.", null);
					return;
				} catch (ConnTimeoutException cte) {	// In this case, it's unworthy to stay and check other innerLinks here.
					logger.warn("Page: \"" + pageUrl + "\" left \"PageCrawler.visit()\" after a potentialDocUrl caused a ConnTimeoutException.");
					UrlUtils.logTriple(pageUrl, "unreachable", "Logged in \"PageCrawler.visit()\" method, as an innerLink of this page caused \"ConnTimeoutException\".", null);
					return;
				} catch (Exception e) {	// The exception: "DomainWithUnsupportedHEADmethodException" should never be caught here, as we use "GET" for possibleDocUrls.
					logger.error("" + e);
				}
            }
            
			curLinksStr.add(urlToCheck);	// Keep the string version of this link, in order not to make the transformation later..
			
		}// end for-loop
		
		// If we reached here, it means that we couldn't find a docUrl the quick way.. so we have to check some (we exclude lots of them) of the inner links one by one.
		
		for ( String currentLink : curLinksStr )
		{
			// We re-check here, as, in the fast-loop not all of the links are checked against this.
			if ( shouldNotAcceptInnerLink(currentLink) ) {	// If this link matches certain blackListed criteria, move on..
				//logger.debug("Avoided link: " + currentLink );
				UrlUtils.duplicateUrls.add(currentLink);
				continue;
			}
			
			//logger.debug("InnerLink: " + currentLink);	// DEBUG!
			try {
				if ( HttpUtils.connectAndCheckMimeType(pageUrl, currentLink, currentPageDomain, false, false) )	// We log the docUrl inside this method.
					return;
			} catch (DomainBlockedException dbe) {
				logger.warn("Page: \"" + pageUrl + "\" left \"PageCrawler.visit()\" after it's domain was blocked.");
				UrlUtils.logTriple(pageUrl, "unreachable", "Logged in PageCrawler.visit() method, as its domain was blocked during crawling.", null);
				return;
			} catch (DomainWithUnsupportedHEADmethodException dwuhe) {
				logger.warn("Page: \"" + pageUrl + "\" left \"PageCrawler.visit()\" after it's domain was caught to not support the HTTP HEAD method.");
				UrlUtils.logTriple(pageUrl, "unreachable", "Logged in PageCrawler.visit() method, as its domain was caught to not support the HTTP HEAD method.", null);
				return;
			} catch (ConnTimeoutException cte) {	// In this case, it's unworthy to stay and check other innerLinks here.
				logger.warn("Page: \"" + pageUrl + "\" left \"PageCrawler.visit()\" after an innerLink caused a ConnTimeoutException.");
				UrlUtils.logTriple(pageUrl, "unreachable", "Logged in PageCrawler.visit() method, as an innerLink of this page caused \"ConnTimeoutException\".", null);
				return;
			} catch (RuntimeException e) {
				// No special handling here.. nor logging..
			}
		}	// end for-loop
		
		// If we get here it means that this pageUrl is not a docUrl itself, nor it contains a docUrl..
		logger.warn("Page: \"" + pageUrl + "\" does not contain a docUrl.");
		UrlUtils.logTriple(pageUrl, "unreachable", "Logged in PageCrawler.visit() method, as no docUrl was found inside.", null);
	}
	
	
	private static void parsePageDataWorkAroundForCrawler4j(Page page, String pageUrl, String pageContentType)
	{
		try {	// Follow the issue I opened on GitHub: https://github.com/yasserg/crawler4j/issues/306
			logger.debug("Page with contentType = " + pageContentType + " was found! Trying to parse it..");
			TextParseData parseData = new TextParseData();
			if (page.getContentCharset() == null)
				parseData.setTextContent(new String(page.getContentData()));
			else
				parseData.setTextContent(new String(page.getContentData(), page.getContentCharset()));
			
			parseData.setOutgoingUrls(Net.extractUrls(parseData.getTextContent()));
			page.setParseData(parseData);
		} catch (Exception e) {
			logger.error(e.getMessage() + ", while parsing: " + pageUrl);
			UrlUtils.logTriple(pageUrl, "unreachable", "Discarded in PageCrawler.visit() method, as it could not be parsed manually, having ContentType: " + pageContentType, null);
		}
	}


	@Override
	public void onUnexpectedStatusCode(String urlStr, int statusCode, String contentType, String description)
	{
		UrlUtils.logTriple(urlStr, "unreachable", "Logged in PageCrawler.onUnexpectedStatusCode() method, after returning: " + statusCode + " errorCode.", null);
		
		String currentPageDomain = UrlUtils.getDomainStr(urlStr);
		if ( currentPageDomain == null ) {    // If the domain is not found, it means that a serious problem exists with this docPageString and we shouldn't crawl it.
			logger.warn("Url: \"" + urlStr + "\" seems to be unreachable and its domain is unretrievable. Recieved unexpected responceCode: " + statusCode);
			UrlUtils.connProblematicUrls ++;
			return;
		}
		else
			HttpUtils.lastConnectedHost = currentPageDomain;	// The crawler opened a connection to download this page. It's both here and in shouldVisit(), as the visit() method can be called without the shouldVisit to be previously called.
		
		// Call our general statusCode-handling method (it will also find the domainStr).
		try { HttpUtils.onErrorStatusCode(urlStr, currentPageDomain, statusCode); }
		catch (DomainBlockedException dbe) { }
		UrlUtils.connProblematicUrls ++;
	}


	@Override
	public void onContentFetchError(Page page)
	{
		String pageUrl = page.getWebURL().toString();
		//logger.warn("Can't fetch content of: \"" + pageUrl + "\"");	// The same log-message is already displayed by Crawler4j as it currently calls the old-deprecated method as well.
		UrlUtils.logTriple(pageUrl, "unreachable", "Logged in PageCrawler.onContentFetchError() method, as no content was able to be fetched for this page.", null);
		UrlUtils.connProblematicUrls ++;
	}


	@Override
	protected void onParseError(WebURL webUrl)
	{
		String urlStr = webUrl.toString();
		logger.warn("Parsing error of: \"" + urlStr + "\"" );
		
		if ( HttpUtils.politenessDelay > 0 ) {
			String domain = UrlUtils.getDomainStr(urlStr);
			if ( domain != null )
				HttpUtils.lastConnectedHost = domain;
			else {
				UrlUtils.logTriple(urlStr, "unreachable", "Logged in PageCrawler.onParseError() method, as there was a problem parsing this page.", null);
				return;
			}
		}
		
		// Try rescuing the possible-docUrl.
		try {
			if ( HttpUtils.connectAndCheckMimeType(urlStr, urlStr, null, false, true) )	// Sometimes "TIKA" (Crawler4j uses it for parsing webPages) falls into a parsing error, when parsing PDFs.
				return;
			else
				UrlUtils.logTriple(urlStr, "unreachable", "Logged in PageCrawler.onParseError() method, as there was a problem parsing this page.", null);
		} catch (Exception e) {
			UrlUtils.logTriple(urlStr, "unreachable", "Logged in PageCrawler.onParseError() method, as there was a problem parsing this page.", null);
		}
	}


	@Override
	public void onUnhandledException(WebURL webUrl, Throwable e)
	{
		if ( webUrl != null )
		{
			String urlStr = webUrl.toString();
			String exceptionReason = e.toString();
			if ( exceptionReason == null ) {    // Avoid causing an "NPE".
				UrlUtils.logTriple(urlStr, "unreachable", "Logged in PageCrawler.onUnhandledException() method, as there was an unhandled exception: " + e, null);
				return;
			}
			
			int curTreatableException = 0;
			
			if ( exceptionReason.contains("UnknownHostException") )
				curTreatableException = 1;
			else if ( exceptionReason.contains("SocketTimeoutException") )
				curTreatableException = 2;
			else if ( exceptionReason.contains("ConnectTimeoutException") )
				curTreatableException = 3;
			
			if ( curTreatableException > 0 )	// If there is a treatable Exception.
			{
				String domainStr = UrlUtils.getDomainStr(urlStr);
				if ( domainStr != null )
				{
					if ( curTreatableException == 1 )
						HttpUtils.blacklistedDomains.add(domainStr);
					else { // TODO - More checks to be added if more exceptions are treated here in the future.
						try { HttpUtils.onTimeoutException(domainStr); }
						catch (DomainBlockedException dbe) { }	// Do nothing here, as Crawler4j will already go to the next url, by itself.
					}
				}
				
				// Log the right messages for these exceptions.
				switch ( curTreatableException ) {
					case 1:
						logger.warn("UnknownHostException was thrown while trying to fetch url: \"" + urlStr + "\".");
						UrlUtils.logTriple(urlStr, "unreachable", "Logged in PageCrawler.onUnhandledException() method, as there was an \"UnknownHostException\" for this url.", null);
						break;
					case 2:
						logger.warn("SocketTimeoutException was thrown while trying to fetch url: \"" + urlStr + "\".");
						HttpUtils.lastConnectedHost = domainStr;
						UrlUtils.logTriple(urlStr, "unreachable", "Logged in PageCrawler.onUnhandledException() method, as there was an \"SocketTimeoutException\" for this url.", null);
						break;
					case 3:
						logger.warn("ConnectTimeoutException was thrown while trying to fetch url: \"" + urlStr + "\".");
						HttpUtils.lastConnectedHost = domainStr;
						UrlUtils.logTriple(urlStr, "unreachable", "Logged in PageCrawler.onUnhandledException() method, as there was an \"ConnectTimeoutException\" for this url.", null);
						break;
					default:
						logger.error("Undefined value for \"curTreatableException\"! Re-check which exceptions are treated!");
						break;
				}
			}
			else {	// If this Exception cannot be treated.
				logger.warn("Unhandled exception: \"" + e + "\" while fetching url: \"" + urlStr + "\"");
				UrlUtils.logTriple(urlStr, "unreachable", "Logged in PageCrawler.onUnhandledException() method, as there was an unhandled exception: " + e, null);
			}
			
			UrlUtils.connProblematicUrls ++;
		}
		else // If the url is null.
			logger.warn("", e);
			// It cannot be logged in the output..
	}


	@Override
	public void onPageBiggerThanMaxSize(String urlStr, long pageSize)
	{
		logger.warn("Skipping url: \"" + urlStr + "\" which was bigger (" + pageSize +") than the max allowed size (" + HttpUtils.maxAllowedContentSize + ")");
		UrlUtils.logTriple(urlStr, "unreachable", "Logged in PageCrawler.onPageBiggerThanMaxSize() method, as this page's size was over the limit (" + HttpUtils.maxAllowedContentSize + ").", null);	// No domain needs to be passed along..
		
		if ( HttpUtils.politenessDelay > 0 ) {
			String domain = null;
			if ( (domain = UrlUtils.getDomainStr(urlStr)) != null )
				HttpUtils.lastConnectedHost = domain;
		}
	}

}
