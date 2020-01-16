package eu.openaire.doc_urls_retriever.crawler;

import edu.uci.ics.crawler4j.url.URLCanonicalizer;
import eu.openaire.doc_urls_retriever.exceptions.*;
import eu.openaire.doc_urls_retriever.util.file.FileUtils;
import eu.openaire.doc_urls_retriever.util.http.ConnSupportUtils;
import eu.openaire.doc_urls_retriever.util.http.HttpConnUtils;
import eu.openaire.doc_urls_retriever.util.url.LoaderAndChecker;
import eu.openaire.doc_urls_retriever.util.url.UrlTypeChecker;
import eu.openaire.doc_urls_retriever.util.url.UrlUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * @author Lampros Smyrnaios
 */
public class PageCrawler
{
	private static final Logger logger = LoggerFactory.getLogger(PageCrawler.class);

	// Order-independent META_DOC_URL-regex.
	// (?:<meta(?:[\\s]*name=\"(?:citation_pdf_url|eprints.document_url)\"[\\s]*content=\"([http][\\w/.,\\-_%&;:~()\\[\\]?=]+)(?:\"))|(?:[\\s]*content=\"([http][\\w/.,\\-_%&;:~()\\[\\]?=]+)(?:\")[\\s]*name=\"(?:citation_pdf_url|eprints.document_url)\"))(?:[\\s]*(?:/)?>)
	private static String metaName = "name=\"(?:citation_pdf_url|eprints.document_url)\"";
	private static String metaContent = "content=\"([http][\\w/.,\\-_%&;:~()\\[\\]?=]+)\"";
	public static final Pattern META_DOC_URL = Pattern.compile("(?:<meta(?:[\\s]*" + metaName + "[\\s]*" + metaContent + ")|(?:[\\s]*" + metaContent + "[\\s]*" + metaName + ")(?:[\\s]*(?:/)?>))");

	public static final Pattern JAVASCRIPT_DOC_LINK = Pattern.compile("(?:javascript:pdflink.*')(http.+)(?:',.*)", Pattern.CASE_INSENSITIVE);
	
	public static int totalPagesReachedCrawling = 0;	// This counts the pages which reached the crawlingStage, i.e: were not discarded in any case and waited to have their internalLinks checked.
	
	public static final HashMap<String, Integer> timesDomainNotGivingInternalLinks = new HashMap<String, Integer>();
	public static final HashMap<String, Integer> timesDomainNotGivingDocUrls = new HashMap<String, Integer>();
	
	public static final int timesToGiveNoInternalLinksBeforeBlocked = 5;
	public static final int timesToGiveNoDocUrlsBeforeBlocked = 10;
	
	
	public static void visit(String urlId, String sourceUrl, String pageUrl, String pageContentType, HttpURLConnection conn)
	{
		logger.debug("Visiting pageUrl: \"" + pageUrl + "\".");
		
		String currentPageDomain = UrlUtils.getDomainStr(pageUrl);
		if ( currentPageDomain == null ) {    // If the domain is not found, it means that a serious problem exists with this docPage and we shouldn't crawl it.
			logger.warn("Problematic URL in \"PageCrawler.visit()\": \"" + pageUrl + "\"");
			UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Discarded in PageCrawler.visit() method, after the occurrence of a domain-retrieval error.", null);
			return;
		}
		
		if ( HandleScienceDirect.checkIfAndHandleScienceDirect(urlId, sourceUrl, pageUrl, currentPageDomain, conn) )
			return;	// We always return, if we have a kindOf-scienceDirect-url. The sourceUrl is already logged inside the called method.
		
		String pageHtml = null;
		try {	// Get the pageHtml to parse the page.
			pageHtml = ConnSupportUtils.getHtmlString(conn);
			//logger.debug(pageHtml);	// DEBUG!
		} catch (Exception e) {
			logger.debug("Could not retrieve the internalLinks for pageUrl: " + pageUrl);
			UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Discarded in 'PageCrawler.visit()' method, as there was a problem retrieving its internalLinks. Its contentType is: '" + pageContentType + "'", null);
			return;
		}
		
		// Check if the docLink is provided in a metaTag and connect to it directly.
		if ( checkIfAndHandleMetaDocUrl(urlId, sourceUrl, pageUrl, currentPageDomain, pageHtml) )
			return;	// The sourceUrl is already logged inside the called method.
		
	    // Check if we want to use AND if so, if we should run, the MLA.
		if ( MachineLearning.useMLA ) {
			PageCrawler.totalPagesReachedCrawling ++;	// Used for M.L.A.'s execution-manipulation.
			if ( MachineLearning.shouldRunPrediction() )
				if ( MachineLearning.predictInternalDocUrl(urlId, sourceUrl, pageUrl, currentPageDomain) )	// Check if we can find the docUrl based on previous runs. (Still in experimental stage)
					return;	// If we were able to find the right path.. and hit a docUrl successfully.. return. The Quadruple is already logged.
		}
		
		HashSet<String> currentPageLinks = null;
		if ( (currentPageLinks = retrieveInternalLinks(urlId, sourceUrl, pageUrl, currentPageDomain, pageHtml, pageContentType)) == null )
			return;	// The necessary logging is handled inside.
		
		HashSet<String> remainingLinks = new HashSet<>(currentPageLinks.size());	// Initialize with the total num of links (less will actually get stored there, but their num is unknown).
		String urlToCheck = null;
		String lowerCaseLink = null;
		
		// Do a fast-loop, try connecting only to a handful of promising links first.
		// Check if urls inside this page, match to a docUrl regex, if they do, try connecting with them and see if they truly are docUrls. If they are, return.
		for ( String currentLink : currentPageLinks )
		{
			// Produce fully functional internal links, NOT internal paths or non-canonicalized.
			if ( (urlToCheck = URLCanonicalizer.getCanonicalURL(currentLink, pageUrl, StandardCharsets.UTF_8)) == null ) {
				logger.warn("Could not cannonicalize internal url: " + currentLink);
				continue;
			}
			
            if ( UrlUtils.docUrlsWithKeys.containsKey(urlToCheck) ) {	// If we got into an already-found docUrl, log it and return.
				logger.info("re-crossed docUrl found: <" + urlToCheck + ">");
				if ( FileUtils.shouldDownloadDocFiles )
					UrlUtils.logQuadruple(urlId, sourceUrl, pageUrl, urlToCheck, UrlUtils.alreadyDownloadedByIDMessage + UrlUtils.docUrlsWithKeys.get(urlToCheck), currentPageDomain);
				else
					UrlUtils.logQuadruple(urlId, sourceUrl, pageUrl, urlToCheck, "", currentPageDomain);
                return;
            }
            
            lowerCaseLink = urlToCheck.toLowerCase();
            if ( LoaderAndChecker.DOC_URL_FILTER.matcher(lowerCaseLink).matches() )
			{
				// Some docUrls may be in different domain, so after filtering the urls based on the possible type.. then we can allow to check for links in different domains.
				
				if ( UrlUtils.duplicateUrls.contains(urlToCheck) )
					continue;
				
				if ( UrlTypeChecker.shouldNotAcceptInternalLink(urlToCheck, lowerCaseLink) ) {    // Avoid false-positives, such as images (a common one: ".../pdf.png").
					UrlUtils.duplicateUrls.add(urlToCheck);
					continue;	// Disclaimer: This way we might lose some docUrls like this: "http://repositorio.ipen.br:8080/xmlui/themes/Mirage/images/Portaria-387.pdf".
				}
				
				//logger.debug("InternalPossibleDocLink to connect with: " + urlToCheck);	// DEBUG!
				try {
					if ( HttpConnUtils.connectAndCheckMimeType(urlId, sourceUrl, pageUrl, urlToCheck, currentPageDomain, false, true) )	// We log the docUrl inside this method.
						return;
					else {	// It's not a DocUrl.
						UrlUtils.duplicateUrls.add(urlToCheck);
						continue;
					}
				} catch (RuntimeException re) {
					UrlUtils.duplicateUrls.add(urlToCheck);    // Don't check it ever again..
					continue;
				} catch (DomainBlockedException dbe) {
					logger.warn("Page: \"" + pageUrl + "\" left \"PageCrawler.visit()\" after it's domain was blocked.");
					UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Logged in 'PageCrawler.visit()' method, as its domain was blocked during crawling.", null);
					return;
				} catch (ConnTimeoutException cte) {	// In this case, it's unworthy to stay and check other internalLinks here.
					logger.warn("Page: \"" + pageUrl + "\" left \"PageCrawler.visit()\" after a potentialDocUrl caused a ConnTimeoutException.");
					UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Logged in 'PageCrawler.visit()' method, as an internalLink of this page caused 'ConnTimeoutException'.", null);
					return;
				} catch (Exception e) {	// The exception: "DomainWithUnsupportedHEADmethodException" should never be caught here, as we use "GET" for possibleDocUrls.
					logger.error("" + e);
					continue;
				}
            }
            
            remainingLinks.add(urlToCheck);	// Add the fully-formed & accepted remaining links into a new hashSet to be iterated.
		}// end for-loop
		
		// If we reached here, it means that we couldn't find a docUrl the quick way.. so we have to check some (we exclude lots of them) of the internal links one by one.
		
		for ( String currentLink : remainingLinks )	// Here we don't re-check already-checked links, as this is a new list.
		{
			// Make sure we avoid connecting to different domains to save time. We allow to check different domains only after matching to possible-urls in the previous fast-loop.
			if ( !currentLink.contains(currentPageDomain) )
				continue;
			
			if ( UrlUtils.duplicateUrls.contains(currentLink) )
				continue;
			
			// We re-check here, as, in the fast-loop not all of the links are checked against this.
			if ( UrlTypeChecker.shouldNotAcceptInternalLink(currentLink, null) ) {	// If this link matches certain blackListed criteria, move on..
				//logger.debug("Avoided link: " + currentLink );
				UrlUtils.duplicateUrls.add(currentLink);
				continue;
			}
			
			//logger.debug("InternalLink to connect with: " + currentLink);	// DEBUG!
			try {
				if ( HttpConnUtils.connectAndCheckMimeType(urlId, sourceUrl, pageUrl, currentLink, currentPageDomain, false, false) )	// We log the docUrl inside this method.
					return;
				else
					UrlUtils.duplicateUrls.add(currentLink);
			} catch (DomainBlockedException dbe) {
				logger.warn("Page: \"" + pageUrl + "\" left \"PageCrawler.visit()\" after it's domain was blocked.");
				UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Logged in 'PageCrawler.visit()' method, as its domain was blocked during crawling.", null);
				return;
			} catch (DomainWithUnsupportedHEADmethodException dwuhe) {
				logger.warn("Page: \"" + pageUrl + "\" left \"PageCrawler.visit()\" after it's domain was caught to not support the HTTP HEAD method.");
				UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Logged in 'PageCrawler.visit()' method, as its domain was caught to not support the HTTP HEAD method.", null);
				return;
			} catch (ConnTimeoutException cte) {	// In this case, it's unworthy to stay and check other internalLinks here.
				logger.warn("Page: \"" + pageUrl + "\" left \"PageCrawler.visit()\" after an internalLink caused a ConnTimeoutException.");
				UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Logged in 'PageCrawler.visit()' method, as an internalLink of this page caused 'ConnTimeoutException'.", null);
				return;
			} catch (RuntimeException e) {
				// No special handling here.. nor logging..
			}
		}	// end for-loop
		
		// If we get here it means that this pageUrl is not a docUrl itself, nor it contains a docUrl..
		logger.warn("Page: \"" + pageUrl + "\" does not contain a docUrl.");
		UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Logged in 'PageCrawler.visit()' method, as no docUrl was found inside.", null);
		if ( ConnSupportUtils.countAndBlockDomainAfterTimes(HttpConnUtils.blacklistedDomains, PageCrawler.timesDomainNotGivingDocUrls, currentPageDomain, PageCrawler.timesToGiveNoDocUrlsBeforeBlocked) )
			logger.debug("Domain: " + currentPageDomain + " was blocked after giving no docUrls more than " + PageCrawler.timesToGiveNoDocUrlsBeforeBlocked + " times.");
	}
	
	
	/**
	 * This method takes in the "pageHtml" of an already-connected url and checks if there is a metaDocUrl inside.
	 * If such url exist, then it connects to it and checks if it's really a docUrl and it may also download the full-text-document, if wanted.
	 * It returns "true" when the metaDocUrl was found and handled (independently of how it was handled),
	 * otherwise, if the metaDocUrl was not-found, it returns "false".
	 * @param urlId
	 * @param sourceUrl
	 * @param pageUrl
	 * @param currentPageDomain
	 * @param pageHtml
	 * @return
	 */
	public static boolean checkIfAndHandleMetaDocUrl(String urlId, String sourceUrl, String pageUrl, String currentPageDomain, String pageHtml)
	{
		// Check if the docLink is provided in a metaTag and connect to it directly.
		try {
			Matcher metaDocUrlMatcher = META_DOC_URL.matcher(pageHtml);
			if ( !metaDocUrlMatcher.find() )
				return false;    // It was not found and so it was not handled.

			//logger.debug("Matched meta-doc-url-line: " + metaDocUrlMatcher.group(0));	// DEBUG!!

			String metaDocUrl = null;
			try {
				metaDocUrl = metaDocUrlMatcher.group(1);
			} catch (Exception e) { logger.error("", e); }
			if ( metaDocUrl == null ) {
				metaDocUrl = metaDocUrlMatcher.group(2);	// Try the other group.
				if ( metaDocUrl == null ) {
					logger.error("Could not retrieve the metaDocUrl, continue by crawling the pageUrl.");
					return false;	// It was not handled.
				}
			}

			if ( metaDocUrl.contains("{{") )	// Dynamic link! The only way to handle it is by blocking the "currentPageUrlDomain".
			{
				logger.debug("The metaDocUrl is a dynamic-link. Abort the process nd block the domain of the pageUrl.");
				// Block the domain and return "true" to indicate handled-state.
				HttpConnUtils.blacklistedDomains.add(currentPageDomain);
				return true;
			}

			if ( (metaDocUrl = URLCanonicalizer.getCanonicalURL(metaDocUrl, null, StandardCharsets.UTF_8)) == null ) {
				logger.warn("Could not cannonicalize metaDocUrl: " + metaDocUrl);
				UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Discarded in \"checkIfAndHandleMetaDocUrl()\", due to cannibalization's problems.", null);
				return true;
			}

			// Connect to it directly.
			//logger.debug("MetaDocUrl: " + metaDocUrl);	// DEBUG!
			if ( !HttpConnUtils.connectAndCheckMimeType(urlId, sourceUrl, pageUrl, metaDocUrl, currentPageDomain, false, true) ) {    // On success, we log the docUrl inside this method.
				logger.warn("The retrieved metaDocUrl was not a docUrl (unexpected): " + metaDocUrl);
				UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Discarded in 'PageCrawler.visit()' method, as the retrieved metaDocUrl was not a docUrl.", null);
			}
			return true; 	// It should be the docUrl and it was handled.. so we don't continue checking the internalLink even if this wasn't a docUrl.

		} catch (Exception e) {	// After connecting to the metaDocUrl.
			UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Discarded in 'PageCrawler.visit()' method, as there was a problem with the metaTag-url.", null);
			return true;	// It was found and handled. Even if an exception was thrown, we don't want to check any other internalLinks in that page.
		}
	}
	
	
	public static HashSet<String> retrieveInternalLinks(String urlId, String sourceUrl, String pageUrl, String currentPageDomain, String pageHtml, String pageContentType)
	{
		HashSet<String> currentPageLinks = null;
		try {
			currentPageLinks = extractInternalLinksFromHtml(pageHtml);
		} catch ( DynamicInternalLinksFoundException dilfe) {
				logger.debug("Domain \"" + currentPageDomain + "\" was found to have dynamic links, so it will be blocked.");
				HttpConnUtils.blacklistedDomains.add(currentPageDomain);
				logger.warn("Page: \"" + pageUrl + "\" left \"PageCrawler.visit()\" after it's domain was blocked.");	// Refer "PageCrawler.visit()" here for consistency with other similar messages.
				UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Logged in 'PageCrawler.retrieveInternalLinks()', as it belongs to a domain with dynamic-links.", null);
			return null;
		} catch (JavaScriptDocLinkFoundException jsdlfe) {
			handleJavaScriptDocLink(urlId, sourceUrl, pageUrl, currentPageDomain, pageContentType, jsdlfe);	// url-logging is handled inside.
			return null;	// This JavaScriptDocLink is the only docLink we will ever gonna get from this page. The sourceUrl is logged inside the called method.
		} catch (Exception e) {
			logger.debug("Could not retrieve the internalLinks for pageUrl: " + pageUrl);
			UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Discarded in 'PageCrawler.visit()' method, as there was a problem retrieving its internalLinks. Its contentType is: '" + pageContentType + "'", null);
			return null;
		}
		
		//logger.debug("Num of links in: \"" + pageUrl + "\" is: " + currentPageLinks.size());
		
		if ( currentPageLinks.isEmpty() ) {	// If no links were retrieved (e.g. the pageUrl was some kind of non-page binary content)
			logger.warn("No links were able to be retrieved from pageUrl: \"" + pageUrl + "\". Its contentType is: " + pageContentType);
			UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Discarded in PageCrawler.visit() method, as no links were able to be retrieved from it. Its contentType is: '" + pageContentType + "'", null);
			if ( ConnSupportUtils.countAndBlockDomainAfterTimes(HttpConnUtils.blacklistedDomains, PageCrawler.timesDomainNotGivingInternalLinks, currentPageDomain, PageCrawler.timesToGiveNoInternalLinksBeforeBlocked) )
				logger.debug("Domain: " + currentPageDomain + " was blocked after giving no internalLinks more than " + PageCrawler.timesToGiveNoInternalLinksBeforeBlocked + " times.");
			return null;
		}
		
		//if ( pageUrl.contains(<keyWord> | <url>) )	// In case we want to print internal-links only for specific-pageTypes.
			//printInternalLinksForDebugging(currentPageLinks);
		
		return currentPageLinks;
	}


	public static HashSet<String> extractInternalLinksFromHtml(String pageHtml) throws JavaScriptDocLinkFoundException, DynamicInternalLinksFoundException
	{
		HashSet<String> urls = new HashSet<>();	// It will surely not be null.
		
		// Get the internalLinks using "Jsoup".
		Document document = Jsoup.parse(pageHtml);
		Elements linksOnPage = document.select("a[href]");
		
		for ( Element el : linksOnPage )
		{
			String internalLink = el.attr("href");

			if ( internalLink.isEmpty() )
				continue;

			if ( internalLink.contains("{{") )	// If "{{" is found inside any link, then all the links of this domain are dynamic, so throw an exception for the calling method to catch and log the pageUrl and return immediately.
				throw new DynamicInternalLinksFoundException();

			if ( internalLink.equals("/")
					|| internalLink.startsWith("mailto:") || internalLink.startsWith("tel:") || internalLink.startsWith("fax:")
					|| internalLink.startsWith("file:") || internalLink.startsWith("{openurl}") )
				continue;

			// Remove anchors from possible docUrls and add the remaining part to the list. Non-possibleDocUrls having anchors are rejected.
			if ( internalLink.contains("#") )
			{
				if ( LoaderAndChecker.DOC_URL_FILTER.matcher(internalLink.toLowerCase()).matches() ) {
					// There are some docURLs with anchors!! Like this: https://www.redalyc.org/pdf/104/10401515.pdf#page=1&zoom=auto,-13,792
					internalLink = UrlUtils.removeAnchor(internalLink);
					//logger.debug("Filtered InternalLink: " + internalLink);	// DEBUG!
					urls.add(internalLink);
				}
				// Else we reject it (don't add it in the hashSet)..
				continue;
			}

			//logger.debug("Filtered InternalLink: " + internalLink);	// DEBUG!

			if ( internalLink.toLowerCase().startsWith("javascript:") ) {
				String pdfLink = null;
				Matcher pdfLinkMatcher = JAVASCRIPT_DOC_LINK.matcher(internalLink);	// Send the non-lower-case version as we need the inside url untouched, in order to open a valid connection.
				if ( pdfLinkMatcher.matches() ) {
					try {
						pdfLink = pdfLinkMatcher.group(1);
					} catch (Exception e) { logger.error("", e); }
					throw new JavaScriptDocLinkFoundException(pdfLink);    // If it's 'null', we treat it when handling this exception.
				} else {    // It's a javaScript link or element which we don't treat.
					//logger.debug("This javaScript element was not handled: " + internalLink);	// Enable only if needed for specific debugging.
					continue;
				}
			}
			urls.add(internalLink);
		}
		return urls;
	}
	
	
	public static void handleJavaScriptDocLink(String urlId, String sourceUrl, String pageUrl, String currentPageDomain, String pageContentType, JavaScriptDocLinkFoundException jsdlfe)
	{
		String javaScriptDocLink = jsdlfe.getMessage();
		if ( javaScriptDocLink == null ) {
			logger.debug("JavaScriptLink was not retrieved!");
			UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Discarded in 'PageCrawler.visit()' method, as there was a problem retrieving its internalLinks. Its contentType is: '" + pageContentType + "'", null);
		}
		else {
			//logger.debug("Going to check JavaScriptDocLink: " + javaScriptDocLink);	// DEBUG!
			try {
				if ( !HttpConnUtils.connectAndCheckMimeType(urlId, sourceUrl, pageUrl, javaScriptDocLink, currentPageDomain, false, true) )	// We log the docUrl inside this method.
					UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Discarded in 'PageCrawler.visit()' method, as the retrieved JavaScriptDocLink: <" + javaScriptDocLink + "> was not a docUrl.", null);
			} catch (Exception e) {
				UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Discarded in 'PageCrawler.visit()' method, as the retrieved JavaScriptDocLink: <" + javaScriptDocLink + "> had connectivity problems.", null);
			}
		}
	}
	
	
	public static void printInternalLinksForDebugging(HashSet<String> currentPageLinks)
	{
		for ( String url : currentPageLinks ) {
			//if ( url.contains(<keyWord> | <url>) )	// In case we want to print only specific-linkTypes.
				logger.debug(url);
		}
	}
	
}
