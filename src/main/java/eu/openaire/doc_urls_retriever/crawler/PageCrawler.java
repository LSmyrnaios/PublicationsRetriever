package eu.openaire.doc_urls_retriever.crawler;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
	
	// Sciencedirect regexes. Use "find()" with those (they work best).
	public static final Pattern SCIENCEDIRECT_META_DOC_URL = Pattern.compile("(?:<meta name=\"citation_pdf_url\"[\\s]*content=\")((?:http)(?:.*)(?:\\.pdf))(?:\"[\\s]*/>)");
	public static final Pattern SCIENCEDIRECT_FINAL_DOC_URL = Pattern.compile("(?:window.location[\\s]+\\=[\\s]+\\')(.*)(?:\\'\\;)");
	
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
					&& !innerLink.startsWith("mailto:") && !innerLink.startsWith("tel:") && !innerLink.toLowerCase().startsWith("javascript:")
					&& !innerLink.startsWith("{openurl}") ) {
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
				//logger.debug("InputHTMLline: " + inputLine);	// DEBUG!
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
		
		if ( currentPageDomain.equals("linkinghub.elsevier.com") || currentPageDomain.equals("sciencedirect.com") ) {	// Be-careful if we move-on changing the retrieving of the domain of a url.
			if ( !handleScienceDirectFamilyUrls(urlId, sourceUrl, pageUrl, currentPageDomain, conn) ) {
				logger.warn("Problem when handling \"sciencedirect.com\" urls.");
				UrlUtils.logQuadruple(urlId, sourceUrl, null, null, "Discarded in 'PageCrawler.visit()' method, when a 'sciencedirect.com'-url was not able to be handled correctly.", null);
			}
			return;	// We always return in ths case.
		}
		
	    // Check if we want to use AND if so, if we should run, the MLA.
		if ( MachineLearning.useMLA ) {
			PageCrawler.totalPagesReachedCrawling ++;	// Used for M.L.A.'s execution-manipulation.
			if ( MachineLearning.shouldRunMLA(currentPageDomain) )
				if ( MachineLearning.guessInnerDocUrlUsingML(urlId, sourceUrl, pageUrl, currentPageDomain) )	// Check if we can find the docUrl based on previous runs. (Still in experimental stage)
					return;	// If we were able to find the right path.. and hit a docUrl successfully.. return. The Quadruple is already logged.
		}
		
		String pageContentType = conn.getContentType();
	    HashSet<String> currentPageLinks = null;
		
		try {
			currentPageLinks = getOutgoingUrls(conn);
		} catch (Exception e) {
			logger.debug("Could not retrieve the innerLinks for pgeUrl: " + pageUrl);
			UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Discarded in PageCrawler.visit() method, as there was a problem retrieving its innerLinks. Its contentType is: '" + pageContentType + "'", null);
			return;
		}

		//logger.debug("Num of links in: \"" + pageUrl + "\" is: " + currentPageLinks.size());

		if ( currentPageLinks.isEmpty() ) {	// If no links were retrieved (e.g. the pageUrl was some kind of non-page binary content)
			logger.warn("No links were able to be retrieved from pageUrl: \"" + pageUrl + "\". Its contentType is: " + pageContentType);
			UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Discarded in PageCrawler.visit() method, as no links were able to be retrieved from it. Its contentType is: '" + pageContentType + "'", null);
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
				logger.info("re-crossed docUrl found: <" + urlToCheck + ">");
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
					UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Logged in 'PageCrawler.visit()' method, as its domain was blocked during crawling.", null);
					return;
				} catch (ConnTimeoutException cte) {	// In this case, it's unworthy to stay and check other innerLinks here.
					logger.warn("Page: \"" + pageUrl + "\" left \"PageCrawler.visit()\" after a potentialDocUrl caused a ConnTimeoutException.");
					UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Logged in 'PageCrawler.visit()' method, as an innerLink of this page caused 'ConnTimeoutException'.", null);
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
				UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Logged in 'PageCrawler.visit()' method, as its domain was blocked during crawling.", null);
				return;
			} catch (DomainWithUnsupportedHEADmethodException dwuhe) {
				logger.warn("Page: \"" + pageUrl + "\" left \"PageCrawler.visit()\" after it's domain was caught to not support the HTTP HEAD method.");
				UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Logged in 'PageCrawler.visit()' method, as its domain was caught to not support the HTTP HEAD method.", null);
				return;
			} catch (ConnTimeoutException cte) {	// In this case, it's unworthy to stay and check other innerLinks here.
				logger.warn("Page: \"" + pageUrl + "\" left \"PageCrawler.visit()\" after an innerLink caused a ConnTimeoutException.");
				UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Logged in 'PageCrawler.visit()' method, as an innerLink of this page caused 'ConnTimeoutException'.", null);
				return;
			} catch (RuntimeException e) {
				// No special handling here.. nor logging..
			}
		}	// end for-loop
		
		// If we get here it means that this pageUrl is not a docUrl itself, nor it contains a docUrl..
		logger.warn("Page: \"" + pageUrl + "\" does not contain a docUrl.");
		UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Logged in 'PageCrawler.visit()' method, as no docUrl was found inside.", null);
		if ( HttpUtils.countAndBlockDomainAfterTimes(HttpUtils.blacklistedDomains, PageCrawler.timesDomainNotGivingDocUrls, currentPageDomain, PageCrawler.timesToGiveNoDocUrlsBeforeBlocked) )
			logger.debug("Domain: " + currentPageDomain + " was blocked after giving no docUrls more than " + PageCrawler.timesToGiveNoDocUrlsBeforeBlocked + " times.");
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
	 * @return true/false
	 */
	public static boolean handleScienceDirectFamilyUrls(String urlId, String sourceUrl, String pageUrl, String pageDomain, HttpURLConnection conn)
	{
		try {
			// Handle "linkinghub.elsevier.com" urls which contain javaScriptRedirect..
			if ( pageDomain.equals("linkinghub.elsevier.com") ) {
				//UrlUtils.elsevierLinks ++;
				if ( (pageUrl = silentRedirectElsevierToScienseRedirect(pageUrl)) != null )
					conn = HttpUtils.handleConnection(urlId, sourceUrl, pageUrl, pageUrl, pageDomain, true, false);
				else
					return false;
			}
			
			// We now have the "sciencedirect.com" url (either from the beginning or after silentRedirect).
			
			logger.debug("ScienceDirect-url: " + pageUrl);
			String html = getHtmlString(conn);
			Matcher metaDocUrlMatcher = SCIENCEDIRECT_META_DOC_URL.matcher(html);
			if ( metaDocUrlMatcher.find() )
			{
				String metaDocUrl = metaDocUrlMatcher.group(1);
				if ( metaDocUrl.isEmpty() ) {
					logger.error("Could not retrieve the finalDocUrl from a \"sciencedirect.com\" url!");
					return false;
				}
				//logger.debug("MetaDocUrl: " + metaDocUrl);	// DEBUG!
				
				// Get the new html..
				// We don't disconnect the previous one, since they both are in the same domain (see JavaDocs).
				conn = HttpUtils.handleConnection(urlId, sourceUrl, pageUrl, metaDocUrl, pageDomain, true, false);
				
				//logger.debug("Url after connecting: " + conn.getURL().toString());
				//logger.debug("MimeType: " + conn.getContentType());
				
				html = getHtmlString(conn);    // Take the new html.
				Matcher finalDocUrlMatcher = SCIENCEDIRECT_FINAL_DOC_URL.matcher(html);
				if ( finalDocUrlMatcher.find() )
				{
					String finalDocUrl = finalDocUrlMatcher.group(1);
					if ( finalDocUrl.isEmpty() ) {
						logger.error("Could not retrieve the finalDocUrl from a \"sciencedirect.com\" url!");
						return false;
					}
					//logger.debug("FinalDocUrl: " + finalDocUrl);	// DEBUG!
					
					// Check and/or download the docUrl. These urls are one-time-links, meaning that after a while they will just redirect to their pageUrl.
					if ( HttpUtils.connectAndCheckMimeType(urlId, sourceUrl, pageUrl, finalDocUrl, pageDomain, false, true) )    // We log the docUrl inside this method.
						return true;
					else {
						logger.warn("LookedUp finalDocUrl: \"" + finalDocUrl + "\" was not an actual docUrl!");
						return false;
					}
				} else {
					logger.warn("The finalDocLink could not be matched!");
					logger.debug("HTML-code:\n" + html);
					return false;
				}
			} else {
				logger.warn("The metaDocLink could not be matched!");
				logger.debug("HTML-code:\n" + html);
				return false;
			}
		} catch (Exception e) {
			logger.error("" + e);
			return false;
		}
		finally {
			// If the initial pageDomain was different from "sciencedirect.com", close the "sciencedirect.com"-connection here.
			// Otherwise, if it came as a "sciencedirect.com", it will be closed where it was first created, meaning in "HttpUtils.connectAndCheckMimeType()".
			if ( !pageDomain.equals("sciencedirect.com") )
				conn.disconnect();	// Disconnect from the final-"sciencedirect.com"-connection.
		}
	}
	
	
	/**
	 * This method recieves a url from "linkinghub.elsevier.com" and returns it's matched url in "sciencedirect.com".
	 * We do this because the "linkinghub.elsevier.com" urls have a javaScript redirect inside which we are not able to handle without doing html scraping.
	 * If there is any error this method returns the URL it first recieved.
	 * @param linkingElsevierUrl
	 * @return
	 */
	public static String silentRedirectElsevierToScienseRedirect(String linkingElsevierUrl)
	{
		if ( !linkingElsevierUrl.contains("linkinghub.elsevier.com") ) // If this method was called for the wrong url, then just return it.
			return linkingElsevierUrl;
		
		String idStr = null;
		Matcher matcher = UrlUtils.URL_TRIPLE.matcher(linkingElsevierUrl);
		if ( matcher.matches() ) {
			idStr = matcher.group(3);
			if ( idStr == null || idStr.isEmpty() ) {
				logger.warn("Unexpected id-missing case for: " + linkingElsevierUrl);
				return linkingElsevierUrl;
			}
		}
		else {
			logger.warn("Unexpected \"URL_TRIPLE\" mismatch for: " + linkingElsevierUrl);
			return linkingElsevierUrl;
		}
		
		return ("https://www.sciencedirect.com/science/article/pii/" + idStr);
	}
	
}
