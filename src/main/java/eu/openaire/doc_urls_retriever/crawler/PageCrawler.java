package eu.openaire.doc_urls_retriever.crawler;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;

import edu.uci.ics.crawler4j.url.URLCanonicalizer;
import eu.openaire.doc_urls_retriever.exceptions.ConnTimeoutException;
import eu.openaire.doc_urls_retriever.exceptions.DomainBlockedException;
import eu.openaire.doc_urls_retriever.exceptions.DomainWithUnsupportedHEADmethodException;
import eu.openaire.doc_urls_retriever.util.file.FileUtils;
import eu.openaire.doc_urls_retriever.util.http.HttpUtils;
import eu.openaire.doc_urls_retriever.util.url.UrlUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author Lampros A. Smyrnaios
 */
public class PageCrawler
{
	private static final Logger logger = LoggerFactory.getLogger(PageCrawler.class);
	public static int totalPagesReachedCrawling = 0;	// This counts the pages which reached the crawlingStage, i.e: were not discarded in any case and waited to have their innerLinks checked.
	
	public static final HashMap<String, Integer> timesDomainNotGivingInnerLinks = new HashMap<String, Integer>();
	public static final HashMap<String, Integer> timesDomainNotGivingDocUrls = new HashMap<String, Integer>();
	
	public static final int timesToGiveNoInnerLinksBegoreBlocked = 5;
	public static final int timesToGiveNoDocUrlsBeforeBlocked = 10;
	
	
	public static HashSet<String> getOutgoingUrls(HttpURLConnection conn) throws Exception
	{
		HashSet<String> urls = new HashSet<>();
		
		String html = getHtmlString(conn);	// It may throw an exception, which will be passed-on.
		
		// Get the innerLinks using "Jsoup".
		Document document = Jsoup.parse(html);
		Elements linksOnPage = document.select("a[href]");
		
		for (Element el : linksOnPage ) {
			String innerLink = el.attr("href");
			if ( !innerLink.isEmpty()
					&& !innerLink.equals("\\/") && !innerLink.equals("#")
					&& !innerLink.startsWith("mailto:") && !innerLink.startsWith("tel:") && !innerLink.toLowerCase().startsWith("javascript:") ) {
				//logger.debug("InnerLink: " + innerLink);
				urls.add(innerLink);
			}
		}
		
		return urls;
	}
	
	
	public static String getHtmlString(HttpURLConnection conn) throws Exception
	{
		try {
			StringBuilder strB = new StringBuilder(30000);	// We give an initial size to optimize performance.
			BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			
			String inputLine;
			while ( (inputLine = br.readLine()) != null ) {
				strB.append(inputLine);
			}
			br.close();
			
			return strB.toString();
			
		} catch (Exception e) {
			logger.error("", e);
			throw e;
		}
	}
	
	
	public static boolean shouldNotAcceptInnerLink(String linkStr, String lowerCaseLink)
	{
		String lowerCaseUrl = null;
		
		if ( lowerCaseLink == null )
			lowerCaseUrl = linkStr.toLowerCase();
		else
			lowerCaseUrl = lowerCaseLink;
		
		return	UrlUtils.URL_DIRECTORY_FILTER.matcher(lowerCaseUrl).matches() || UrlUtils.INNER_LINKS_KEYWORDS_FILTER.matcher(lowerCaseUrl).matches()
				|| UrlUtils.SPECIFIC_DOMAIN_FILTER.matcher(lowerCaseUrl).matches() || UrlUtils.PLAIN_DOMAIN_FILTER.matcher(lowerCaseUrl).matches()
				|| UrlUtils.INNER_LINKS_FILE_EXTENSION_FILTER.matcher(lowerCaseUrl).matches() || UrlUtils.INNER_LINKS_FILE_FORMAT_FILTER.matcher(lowerCaseUrl).matches()
				|| UrlUtils.PLAIN_PAGE_EXTENSION_FILTER.matcher(lowerCaseUrl).matches()
				|| UrlUtils.CURRENTLY_UNSUPPORTED_DOC_EXTENSION_FILTER.matcher(lowerCaseUrl).matches();	// TODO - To be removed when these docExtensions get supported.
		
		// The following checks are obsolete here, as we already use it inside "visit()" method. Still keep it here, as it makes our intentions clearer.
		// !lowerCaseUrl.contains(referringPageDomain)	// Don't check this link if it belongs in a different domain than the referringPage's one.
	}
	
	
	public static void visit(String urlId, String sourceUrl, String pageUrl, HttpURLConnection conn)
	{
		logger.debug("Visiting pageUrl: \"" + pageUrl + "\".");
		
		String currentPageDomain = UrlUtils.getDomainStr(pageUrl);
		if ( currentPageDomain == null ) {    // If the domain is not found, it means that a serious problem exists with this docPage and we shouldn't crawl it.
			logger.warn("Problematic URL in \"PageCrawler.visit()\": \"" + pageUrl + "\"");
			UrlUtils.logQuadruple(urlId, sourceUrl, null, pageUrl, "Discarded in PageCrawler.visit() method, after the occurrence of a domain-retrieval error.", null);
			return;
		}
		
	    // Check if we want to use AND if so, if we should run, the MLA.
		if ( MachineLearning.useMLA ) {
			PageCrawler.totalPagesReachedCrawling ++;	// Used for M.L.A.'s execution-manipulation.
			if ( MachineLearning.shouldRunMLA(currentPageDomain) )
				if ( MachineLearning.guessInnerDocUrlUsingML(urlId, sourceUrl, pageUrl, currentPageDomain) )	// Check if we can find the docUrl based on previous runs. (Still in experimental stage)
					return;	// If we were able to find the right path.. and hit a docUrl successfully.. return.
		}
		
		String pageContentType = conn.getContentType();
	    HashSet<String> currentPageLinks = null;
		
		try {
			currentPageLinks = getOutgoingUrls(conn);
		} catch (Exception e) {
			logger.debug("Could not retrieve the innerLinks for pgeUrl: " + pageUrl);
			UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Discarded in PageCrawler.visit() method, as there was a problem retrieving its innerLinks. Its contentType is: \"" + pageContentType + "\"", null);
			return;
		}

		//logger.debug("Num of links in: \"" + pageUrl + "\" is: " + currentPageLinks.size());

		if ( currentPageLinks.isEmpty() ) {	// If no links were retrieved (e.g. the pageUrl was some kind of non-page binary content)
			logger.warn("No links were able to be retrieved from pageUrl: \"" + pageUrl + "\". Its contentType is: " + pageContentType);
			UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Discarded in PageCrawler.visit() method, as no links were able to be retrieved from it. Its contentType is: \"" + pageContentType + "\"", null);
			if ( HttpUtils.countAndBlockDomainAfterTimes(HttpUtils.blacklistedDomains, PageCrawler.timesDomainNotGivingInnerLinks, currentPageDomain, PageCrawler.timesToGiveNoInnerLinksBegoreBlocked) )
				logger.debug("Domain: " + currentPageDomain + " was blocked after giving no innerLinks more than " + PageCrawler.timesToGiveNoInnerLinksBegoreBlocked + " times.");
			return;
		}
		
		//Check innerLinks for debugging:
		//if ( pageUrl.contains("<url>") )
		/*for ( String url : currentPageLinks )
			logger.debug(url);*/
		
		
		HashSet<String> remainingLinks = new HashSet<>();
		String urlToCheck = null;
		String lowerCaseLink = null;
		
		// Do a fast-loop, try connecting only to a handful of promising links first.
		// Check if urls inside this page, match to a docUrl regex, if they do, try connecting with them and see if they truly are docUrls. If they are, return.
		for ( String currentLink : currentPageLinks )
		{
			// Produce fully functional inner links, NOT inner paths or non-canonicalized.
			if ( (urlToCheck = URLCanonicalizer.getCanonicalURL(currentLink, pageUrl, StandardCharsets.UTF_8)) == null ) {
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
				logger.debug("Re-crossing the already found docUrl: \"" + urlToCheck + "\"");
				if ( FileUtils.shouldDownloadDocFiles )
					UrlUtils.logQuadruple(urlId, sourceUrl, pageUrl, urlToCheck, "This file is probably already downloaded.", currentPageDomain);
				else
					UrlUtils.logQuadruple(urlId, sourceUrl, pageUrl, urlToCheck, "", currentPageDomain);
                return;
            }
            
            lowerCaseLink = urlToCheck.toLowerCase();
            if ( UrlUtils.DOC_URL_FILTER.matcher(lowerCaseLink).matches() )
			{
				if ( shouldNotAcceptInnerLink(urlToCheck, lowerCaseLink) ) {    // Avoid false-positives, such as images (a common one: ".../pdf.png").
					UrlUtils.duplicateUrls.add(urlToCheck);
					continue;	// Disclaimer: This way we might lose some docUrls like this: "http://repositorio.ipen.br:8080/xmlui/themes/Mirage/images/Portaria-387.pdf".
				}
				
				//logger.debug("InnerPossibleDocLink to connect with: " + urlToCheck);	// DEBUG!
				try {
					if ( HttpUtils.connectAndCheckMimeType(urlId, sourceUrl, pageUrl, urlToCheck, currentPageDomain, false, true) )	// We log the docUrl inside this method.
						return;
					else {
						UrlUtils.duplicateUrls.add(urlToCheck);
						continue;
					}
				} catch (RuntimeException re) {
					UrlUtils.duplicateUrls.add(urlToCheck);    // Don't check it ever again..
					continue;
				} catch (DomainBlockedException dbe) {
					logger.warn("Page: \"" + pageUrl + "\" left \"PageCrawler.visit()\" after it's domain was blocked.");
					UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Logged in PageCrawler.visit() method, as its domain was blocked during crawling.", null);
					return;
				} catch (ConnTimeoutException cte) {	// In this case, it's unworthy to stay and check other innerLinks here.
					logger.warn("Page: \"" + pageUrl + "\" left \"PageCrawler.visit()\" after a potentialDocUrl caused a ConnTimeoutException.");
					UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Logged in \"PageCrawler.visit()\" method, as an innerLink of this page caused \"ConnTimeoutException\".", null);
					return;
				} catch (Exception e) {	// The exception: "DomainWithUnsupportedHEADmethodException" should never be caught here, as we use "GET" for possibleDocUrls.
					logger.error("" + e);
					continue;
				}
            }
            
            remainingLinks.add(urlToCheck);	// Add the fully-formed & accepted remaining links into a new hashSet to be iterated.
		}// end for-loop
		
		// If we reached here, it means that we couldn't find a docUrl the quick way.. so we have to check some (we exclude lots of them) of the inner links one by one.
		
		for ( String currentLink : remainingLinks )	// Here we don't re-check already-checked links, as they were removed.
		{
			// We re-check here, as, in the fast-loop not all of the links are checked against this.
			if ( shouldNotAcceptInnerLink(currentLink, null) ) {	// If this link matches certain blackListed criteria, move on..
				//logger.debug("Avoided link: " + currentLink );
				UrlUtils.duplicateUrls.add(currentLink);
				continue;
			}
			
			//logger.debug("InnerLink to connect with: " + currentLink);	// DEBUG!
			try {
				if ( HttpUtils.connectAndCheckMimeType(urlId, sourceUrl, pageUrl, currentLink, currentPageDomain, false, false) )	// We log the docUrl inside this method.
					return;
				else
					UrlUtils.duplicateUrls.add(currentLink);
			} catch (DomainBlockedException dbe) {
				logger.warn("Page: \"" + pageUrl + "\" left \"PageCrawler.visit()\" after it's domain was blocked.");
				UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Logged in PageCrawler.visit() method, as its domain was blocked during crawling.", null);
				return;
			} catch (DomainWithUnsupportedHEADmethodException dwuhe) {
				logger.warn("Page: \"" + pageUrl + "\" left \"PageCrawler.visit()\" after it's domain was caught to not support the HTTP HEAD method.");
				UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Logged in PageCrawler.visit() method, as its domain was caught to not support the HTTP HEAD method.", null);
				return;
			} catch (ConnTimeoutException cte) {	// In this case, it's unworthy to stay and check other innerLinks here.
				logger.warn("Page: \"" + pageUrl + "\" left \"PageCrawler.visit()\" after an innerLink caused a ConnTimeoutException.");
				UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Logged in PageCrawler.visit() method, as an innerLink of this page caused \"ConnTimeoutException\".", null);
				return;
			} catch (RuntimeException e) {
				// No special handling here.. nor logging..
			}
		}	// end for-loop
		
		// If we get here it means that this pageUrl is not a docUrl itself, nor it contains a docUrl..
		logger.warn("Page: \"" + pageUrl + "\" does not contain a docUrl.");
		UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Logged in PageCrawler.visit() method, as no docUrl was found inside.", null);
		if ( HttpUtils.countAndBlockDomainAfterTimes(HttpUtils.blacklistedDomains, PageCrawler.timesDomainNotGivingDocUrls, currentPageDomain, PageCrawler.timesToGiveNoDocUrlsBeforeBlocked) )
			logger.debug("Domain: " + currentPageDomain + " was blocked after giving no docUrls more than " + PageCrawler.timesToGiveNoDocUrlsBeforeBlocked + " times.");
	}
	
}
