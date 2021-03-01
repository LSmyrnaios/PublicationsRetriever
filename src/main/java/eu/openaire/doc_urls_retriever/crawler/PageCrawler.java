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

import java.io.BufferedReader;
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

	private static final Pattern INTERNAL_LINKS_STARTING_FROM_FILTER = Pattern.compile("^(?:(?:mailto|tel|fax|file|data):|\\{openurl}|[/]*\\?(?:locale(?:-attribute)?|ln)=).*");

	public static final Pattern JAVASCRIPT_DOC_LINK = Pattern.compile("(?:javascript:pdflink.*')(http.+)(?:',.*)", Pattern.CASE_INSENSITIVE);

	public static final HashMap<String, Integer> timesDomainNotGivingInternalLinks = new HashMap<String, Integer>();
	public static final HashMap<String, Integer> timesDomainNotGivingDocUrls = new HashMap<String, Integer>();

	public static final int timesToGiveNoInternalLinksBeforeBlocked = 20;
	public static final int timesToGiveNoDocUrlsBeforeBlocked = 10;

	public static int contentProblematicUrls = 0;

	private static final int MAX_REMAINING_INTERNAL_LINKS_TO_CHECK = 10;	// The < 10 > is the optimal value, figured out after tests.


	public static void visit(String urlId, String sourceUrl, String pageUrl, String pageContentType, HttpURLConnection conn, String firstHTMLlineFromDetectedContentType, BufferedReader bufferedReader)
	{
		logger.debug("Visiting pageUrl: \"" + pageUrl + "\".");

		String pageDomain = UrlUtils.getDomainStr(pageUrl, null);
		if ( pageDomain == null ) {    // If the domain is not found, it means that a serious problem exists with this docPage and we shouldn't crawl it.
			logger.warn("Problematic URL in \"PageCrawler.visit()\": \"" + pageUrl + "\"");
			UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Discarded in PageCrawler.visit() method, after the occurrence of a domain-retrieval error.", null, true);
			LoaderAndChecker.connProblematicUrls ++;
			ConnSupportUtils.closeBufferedReader(bufferedReader);	// This page's content-type was auto-detected, and the process fails before re-requesting the conn-inputStream, then make sure we close the last one.
			return;
		}

		String pageHtml = null;	// Get the pageHtml to parse the page.
		if ( (pageHtml = ConnSupportUtils.getHtmlString(conn, bufferedReader)) == null ) {
			logger.warn("Could not retrieve the HTML-code for pageUrl: " + pageUrl);
			UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Discarded in 'PageCrawler.visit()' method, as there was a problem retrieving its HTML-code. Its contentType is: '" + pageContentType + "'.", null, true);
			LoaderAndChecker.connProblematicUrls ++;
			return;
		}
		else if ( firstHTMLlineFromDetectedContentType != null ) {
			pageHtml = firstHTMLlineFromDetectedContentType + pageHtml;
		}

		//logger.debug(pageHtml);	// DEBUG!

		if ( !pageUrl.contains("sciencedirect.com") ) {	// The scienceDirect-pageUrl do not benefit from the method below, so we skip them..
			// Check if the docLink is provided in a metaTag and connect to it directly.
			if ( MetaDocUrlsHandler.checkIfAndHandleMetaDocUrl(urlId, sourceUrl, pageUrl, pageDomain, pageHtml) )
				return;	// The sourceUrl is already logged inside the called method.

			// Check if we want to use AND if so, if we should run, the MLA.
			if ( MachineLearning.useMLA ) {
				MachineLearning.totalPagesReachedMLAStage ++;	// Used for M.L.A.'s execution-manipulation.
				if ( MachineLearning.shouldRunPrediction() )
					if ( MachineLearning.predictInternalDocUrl(urlId, sourceUrl, pageUrl, pageDomain) )	// Check if we can find the docUrl based on previous runs. (Still in experimental stage)
						return;	// If we were able to find the right path.. and hit a docUrl successfully.. return. The Quadruple is already logged.
			}
		}

		HashSet<String> currentPageLinks = null;	// We use "HashSet" to avoid duplicates.
		if ( (currentPageLinks = retrieveInternalLinks(urlId, sourceUrl, pageUrl, pageDomain, pageHtml, pageContentType)) == null )
			return;	// The necessary logging is handled inside.

		HashSet<String> remainingLinks = new HashSet<>(currentPageLinks.size());	// Used later. Initialize with the total num of links (less will actually get stored there, but their num is unknown).
		String urlToCheck = null;
		String lowerCaseLink = null;

		// Do a fast-loop, try connecting only to a handful of promising links first.
		// Check if urls inside this page, match to a docUrl regex, if they do, try connecting with them and see if they truly are docUrls. If they are, return.
		for ( String currentLink : currentPageLinks )
		{
			// Produce fully functional internal links, NOT internal paths or non-canonicalized.
			urlToCheck = currentLink;
			if ( !urlToCheck.contains("[") && (urlToCheck = URLCanonicalizer.getCanonicalURL(currentLink, pageUrl, StandardCharsets.UTF_8)) == null ) {
				logger.warn("Could not canonicalize internal url: " + currentLink);
				continue;
			}

            if ( UrlUtils.docUrlsWithIDs.containsKey(urlToCheck) ) {	// If we got into an already-found docUrl, log it and return.
				logger.info("re-crossed docUrl found: < " + urlToCheck + " >");
				LoaderAndChecker.reCrossedDocUrls ++;
				if ( FileUtils.shouldDownloadDocFiles )
					UrlUtils.logQuadruple(urlId, sourceUrl, pageUrl, urlToCheck, UrlUtils.alreadyDownloadedByIDMessage + UrlUtils.docUrlsWithIDs.get(urlToCheck), pageDomain, false);
				else
					UrlUtils.logQuadruple(urlId, sourceUrl, pageUrl, urlToCheck, "", pageDomain, false);
                return;
            }

            lowerCaseLink = urlToCheck.toLowerCase();
            if ( LoaderAndChecker.DOC_URL_FILTER.matcher(lowerCaseLink).matches()
				|| LoaderAndChecker.DATASET_URL_FILTER.matcher(lowerCaseLink).matches() )
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
					if ( HttpConnUtils.connectAndCheckMimeType(urlId, sourceUrl, pageUrl, urlToCheck, null, false, true) )	// We log the docUrl inside this method.
						return;
					else {	// It's not a DocUrl.
						UrlUtils.duplicateUrls.add(urlToCheck);
						continue;
					}
				} catch (RuntimeException re) {
					UrlUtils.duplicateUrls.add(urlToCheck);    // Don't check it ever again..
					continue;
				} catch (DomainBlockedException dbe) {
					if ( urlToCheck.contains(pageDomain) ) {
						logger.warn("Page: \"" + pageUrl + "\" left \"PageCrawler.visit()\" after it's domain was blocked.");
						UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Logged in 'PageCrawler.visit()' method, as its domain was blocked during crawling.", null, true);
						return;
					}
					continue;
				} catch (ConnTimeoutException cte) {	// In this case, it's unworthy to stay and check other internalLinks here.
					if ( urlToCheck.contains(pageDomain) ) {
						logger.warn("Page: \"" + pageUrl + "\" left \"PageCrawler.visit()\" after a potentialDocUrl caused a ConnTimeoutException.");
						UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Logged in 'PageCrawler.visit()' method, as an internalLink of this page caused 'ConnTimeoutException'.", null, true);
						return;
					}
					continue;
				} catch (Exception e) {	// The exception: "DomainWithUnsupportedHEADmethodException" should never be caught here, as we use "GET" for possibleDocUrls.
					logger.error("" + e);
					continue;
				}
            }

            remainingLinks.add(urlToCheck);	// Add the fully-formed & accepted remaining links into a new hashSet to be iterated.
		}// end for-loop

		// If we reached here, it means that we couldn't find a docUrl the quick way.. so we have to check some (we exclude lots of them) of the internal links one by one.

		int remainingUrlsCounter = 0;

		for ( String currentLink : remainingLinks )	// Here we don't re-check already-checked links, as this is a new list. All the links here are full-canonicalized-urls.
		{
			// Make sure we avoid connecting to different domains to save time. We allow to check different domains only after matching to possible-urls in the previous fast-loop.
			if ( !currentLink.contains(pageDomain) )
				continue;

			if ( UrlUtils.duplicateUrls.contains(currentLink) )
				continue;

			// We re-check here, as, in the fast-loop not all of the links are checked against this.
			if ( UrlTypeChecker.shouldNotAcceptInternalLink(currentLink, null) ) {	// If this link matches certain blackListed criteria, move on..
				//logger.debug("Avoided link: " + currentLink );
				UrlUtils.duplicateUrls.add(currentLink);
				continue;
			}

			if ( (++remainingUrlsCounter) > MAX_REMAINING_INTERNAL_LINKS_TO_CHECK ) {	// The counter is incremented only on "wanted" links, so no need to pre-clean the "remainingLinks"-set.
				logger.warn("The maximum limit (" + MAX_REMAINING_INTERNAL_LINKS_TO_CHECK + ") of remaining links to be checked was reached for pageUrl: \"" + pageUrl + "\"");
				break;	// It will reach the end of this function, will call "handlePageWithNoDocUrls()" and then return.
			}

			//logger.debug("InternalLink to connect with: " + currentLink);	// DEBUG!
			try {
				if ( HttpConnUtils.connectAndCheckMimeType(urlId, sourceUrl, pageUrl, currentLink, null, false, false) )	// We log the docUrl inside this method.
					return;
				else
					UrlUtils.duplicateUrls.add(currentLink);
			} catch (DomainBlockedException dbe) {
				if ( currentLink.contains(pageDomain) ) {
					logger.warn("Page: \"" + pageUrl + "\" left \"PageCrawler.visit()\" after it's domain was blocked.");
					UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Logged in 'PageCrawler.visit()' method, as its domain was blocked during crawling.", null, true);
					return;
				}
			} catch (DomainWithUnsupportedHEADmethodException dwuhe) {
				if ( currentLink.contains(pageDomain) ) {
					logger.warn("Page: \"" + pageUrl + "\" left \"PageCrawler.visit()\" after it's domain was caught to not support the HTTP HEAD method, as a result, the internal-links will stop being checked.");
					UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Logged in 'PageCrawler.visit()' method, as its domain was caught to not support the HTTP HEAD method.", null, true);
					return;
				}
			} catch (ConnTimeoutException cte) {	// In this case, it's unworthy to stay and check other internalLinks here.
				if ( currentLink.contains(pageDomain) ) {
					logger.warn("Page: \"" + pageUrl + "\" left \"PageCrawler.visit()\" after an internalLink caused a ConnTimeoutException.");
					UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Logged in 'PageCrawler.visit()' method, as an internalLink of this page caused 'ConnTimeoutException'.", null, true);
					return;
				}
			} catch (RuntimeException e) {
				// No special handling here.. nor logging..
			}
		}	// end for-loop

		handlePageWithNoDocUrls(urlId, sourceUrl, pageUrl, pageDomain, false);
	}


	/**
	 * This method handles
	 * @param urlId
	 * @param sourceUrl
	 * @param pageUrl
	 * @param pageDomain
	 * @param isAlreadyLoggedToOutput
	 */
	private static void handlePageWithNoDocUrls(String urlId, String sourceUrl, String pageUrl, String pageDomain, boolean isAlreadyLoggedToOutput)
	{
		// If we get here it means that this pageUrl is not a docUrl itself, nor it contains a docUrl..
		logger.warn("Page: \"" + pageUrl + "\" does not contain a docUrl.");
		UrlTypeChecker.pagesNotProvidingDocUrls ++;
		if ( !isAlreadyLoggedToOutput )	// This check is used in error-cases, where we have already logged the Quadruple.
			UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Logged in 'PageCrawler.visit()' method, as no docUrl was found inside.", null, true);
		if ( ConnSupportUtils.countAndBlockDomainAfterTimes(HttpConnUtils.blacklistedDomains, PageCrawler.timesDomainNotGivingDocUrls, pageDomain, PageCrawler.timesToGiveNoDocUrlsBeforeBlocked, true) )
			logger.warn("Domain: \"" + pageDomain + "\" was blocked after giving no docUrls more than " + PageCrawler.timesToGiveNoDocUrlsBeforeBlocked + " times.");
	}


	public static HashSet<String> retrieveInternalLinks(String urlId, String sourceUrl, String pageUrl, String pageDomain, String pageHtml, String pageContentType)
	{
		HashSet<String> currentPageLinks = null;
		try {
			currentPageLinks = extractInternalLinksFromHtml(pageHtml, pageUrl);
		} catch (DynamicInternalLinksFoundException dilfe) {
			HttpConnUtils.blacklistedDomains.add(pageDomain);
			logger.warn("Page: \"" + pageUrl + "\" left \"PageCrawler.visit()\" after found to have dynamic links. Its domain \"" + pageDomain + "\"  was blocked.");	// Refer "PageCrawler.visit()" here for consistency with other similar messages.
			UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Logged in 'PageCrawler.retrieveInternalLinks()', as it belongs to a domain with dynamic-links.", null, true);
			PageCrawler.contentProblematicUrls ++;
			return null;
		} catch ( DocLinkFoundException dlfe) {
			if ( !verifyDocLink(urlId, sourceUrl, pageUrl, pageDomain, pageContentType, dlfe) )	// url-logging is handled inside.
				handlePageWithNoDocUrls(urlId, sourceUrl, pageUrl, pageDomain, true);
			return null;	// This DocLink is the only docLink we will ever gonna get from this page. The sourceUrl is logged inside the called method.
			// If this "DocLink" is a DocUrl, then returning "null" here, will trigger the 'PageCrawler.visit()' method to exit immediately (and normally).
		} catch ( DocLinkInvalidException dlie ) {
			//logger.warn("An invalid docLink < " + dlie.getMessage() + " > was found for pageUrl: \"" + pageUrl + "\". Search was stopped.");	// DEBUG!
			UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Discarded in 'PageCrawler.visit()' method, as there was an invalid docLink. Its contentType is: '" + pageContentType + "'", null, true);
			handlePageWithNoDocUrls(urlId, sourceUrl, pageUrl, pageDomain, true);
			return null;
		} catch (Exception e) {
			logger.warn("Could not retrieve the internalLinks for pageUrl: " + pageUrl);
			UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Discarded in 'PageCrawler.visit()' method, as there was a problem retrieving its internalLinks. Its contentType is: '" + pageContentType + "'", null, true);
			PageCrawler.contentProblematicUrls ++;
			return null;
		}

		boolean isNull = (currentPageLinks == null);
		boolean isEmpty = false;

		if ( !isNull )
			isEmpty = (currentPageLinks.size() == 0);

		if ( isNull || isEmpty ) {	// If no links were retrieved (e.g. the pageUrl was some kind of non-page binary content)
			logger.warn("No " + (isEmpty ? "valid " : "") + "links were able to be retrieved from pageUrl: \"" + pageUrl + "\". Its contentType is: " + pageContentType);
			PageCrawler.contentProblematicUrls ++;
			UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Discarded in PageCrawler.visit() method, as no " + (isEmpty ? "valid " : "") + "links were able to be retrieved from it. Its contentType is: '" + pageContentType + "'", null, true);
			if ( ConnSupportUtils.countAndBlockDomainAfterTimes(HttpConnUtils.blacklistedDomains, PageCrawler.timesDomainNotGivingInternalLinks, pageDomain, PageCrawler.timesToGiveNoInternalLinksBeforeBlocked, true) )
				logger.warn("Domain: \"" + pageDomain + "\" was blocked after not providing internalLinks more than " + PageCrawler.timesToGiveNoInternalLinksBeforeBlocked + " times.");
			return null;
		}

		//logger.debug("Num of links in: \"" + pageUrl + "\" is: " + currentPageLinks.size());

		//if ( pageUrl.contains(<keyWord> || <url>) )	// In case we want to print internal-links only for specific-pageTypes.
			//printInternalLinksForDebugging(currentPageLinks);

		return currentPageLinks;
	}


	/**
	 * Get the internalLinks using "Jsoup".
	 * @param pageHtml
	 * @param pageUrl
	 * @return The internalLinks
	 * @throws DocLinkFoundException
	 * @throws DynamicInternalLinksFoundException
	 */
	public static HashSet<String> extractInternalLinksFromHtml(String pageHtml, String pageUrl) throws DocLinkFoundException, DynamicInternalLinksFoundException, DocLinkInvalidException
	{
		Document document = Jsoup.parse(pageHtml);
		Elements elementLinksOnPage = document.select("a, link[href][type*=pdf]");
		if ( elementLinksOnPage.isEmpty() ) {	// It will surely not be null, by Jsoup-documentation.
			logger.warn("Jsoup did not extract any links from pageUrl: \"" + pageUrl + "\"");
			return null;
		}

		HashSet<String> urls = new HashSet<>(elementLinksOnPage.size()/2);	// Only some of the links will be added in the final set.
		String linkAttr, internalLink;

		for ( Element el : elementLinksOnPage )
		{
			linkAttr = el.text();
			if ( !linkAttr.isEmpty() && linkAttr.toLowerCase().contains("pdf") ) {
				internalLink = el.attr("href");
				if ( !internalLink.isEmpty() && !internalLink.startsWith("#", 0) ) {
					logger.debug("Found the docLink < " + internalLink + " > from link-text: \"" + linkAttr + "\"");
					throw new DocLinkFoundException(internalLink);
				}
				throw new DocLinkInvalidException(internalLink);
			}

			linkAttr = el.attr("title");
			if ( !linkAttr.isEmpty() && linkAttr.toLowerCase().contains("pdf") ) {
				internalLink = el.attr("href");
				if ( !internalLink.isEmpty() && !internalLink.startsWith("#", 0) ) {
					logger.debug("Found the docLink < " + internalLink + " > from link-title: \"" + linkAttr + "\"");
					throw new DocLinkFoundException(internalLink);
				}
				throw new DocLinkInvalidException(internalLink);
			}

			// Check if we have a "link[href][type*=pdf]" get the docUrl. This also check all the "types" even from the HTML-"a" elements.
			linkAttr = el.attr("type");
			if ( !linkAttr.isEmpty() && ConnSupportUtils.knownDocMimeTypes.contains(linkAttr) ) {
				internalLink = el.attr("href");
				if ( !internalLink.isEmpty() && !internalLink.startsWith("#", 0) ) {
					logger.debug("Found the docLink < " + internalLink + " > from link-type: \"" + linkAttr + "\"");
					throw new DocLinkFoundException(internalLink);
				}
				throw new DocLinkInvalidException(internalLink);
			}

			internalLink = el.attr("href");
			if ( internalLink.isEmpty() || internalLink.equals("#") ) {
				internalLink = el.attr("data-popup");	// Ex: https://www.ingentaconnect.com/content/cscript/cvia/2017/00000002/00000003/art00008
				if ( internalLink.isEmpty() )
					continue;
			}
			if ( (internalLink = gatherInternalLink(internalLink)) != null )	// Throws exceptions which go to the caller method.
				urls.add(internalLink);
		}
		return urls;
	}


	public static String gatherInternalLink(String internalLink) throws DynamicInternalLinksFoundException, DocLinkFoundException
	{
		if ( internalLink.equals("/") )
			return null;

		if ( internalLink.contains("{{") || internalLink.contains("<?") )	// If "{{" or "<?" is found inside any link, then all the links of this domain are dynamic, so throw an exception for the calling method to catch and log the pageUrl and return immediately.
			throw new DynamicInternalLinksFoundException();

		String lowerCaseInternalLink = internalLink.toLowerCase();

		if ( INTERNAL_LINKS_STARTING_FROM_FILTER.matcher(lowerCaseInternalLink).matches() )
			return null;

		// Remove anchors from possible docUrls and add the remaining part to the list. Non-possibleDocUrls having anchors are rejected (except for hashtag-directories).
		if ( lowerCaseInternalLink.contains("#") )
		{
			if ( LoaderAndChecker.DOC_URL_FILTER.matcher(lowerCaseInternalLink).matches() ) {
				// There are some docURLs with anchors. We should get the docUrl but remove the anchors to keep them clean and connectable.
				// Like this: https://www.redalyc.org/pdf/104/10401515.pdf#page=1&zoom=auto,-13,792
				internalLink = UrlUtils.removeAnchor(internalLink);
				//logger.debug("Filtered InternalLink: " + internalLink);	// DEBUG!
				return internalLink;
			}
			else if ( !lowerCaseInternalLink.contains("/#/") )
				return null;	// Else if it has not a hashtag-directory we reject it (don't add it in the hashSet)..
		}
		else if ( lowerCaseInternalLink.contains("\"") || lowerCaseInternalLink.contains("[error") )	// They cannot be canonicalized and especially the second one is not wanted.
			return null;

		//logger.debug("Filtered InternalLink: " + internalLink);	// DEBUG!

		if ( lowerCaseInternalLink.startsWith("javascript:", 0) ) {
			String pdfLink = null;
			Matcher pdfLinkMatcher = JAVASCRIPT_DOC_LINK.matcher(internalLink);	// Send the non-lower-case version as we need the inside url untouched, in order to open a valid connection.
			if ( !pdfLinkMatcher.matches() ) {  // It's a javaScript link or element which we don't treat.
				//logger.warn("This javaScript element was not handled: " + internalLink);	// Enable only if needed for specific debugging.
				return null;
			}
			try {
				pdfLink = pdfLinkMatcher.group(1);
			} catch (Exception e) { logger.error("", e); }	// Do not "return null;" here, as we want the page-search to stop, not just for this link to not be connected..
			throw new DocLinkFoundException(pdfLink);    // If it's 'null' or 'empty', we treat it when handling this exception.
		}

		return internalLink;
	}


	public static boolean verifyDocLink(String urlId, String sourceUrl, String pageUrl, String pageDomain, String pageContentType, DocLinkFoundException dlfe)
	{
		String docLink = dlfe.getMessage();
		if ( (docLink == null) || docLink.isEmpty() ) {
			logger.warn("DocLink was not retrieved!");
			UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Discarded in 'PageCrawler.visit()' method, as there was a problem retrieving its internalLinks. Its contentType is: '" + pageContentType + "'", null, true);
			return false;
		}

		// Produce fully functional internal links, NOT internal paths or non-canonicalized.
		String tempLink = docLink;
		if ( (docLink = URLCanonicalizer.getCanonicalURL(docLink, pageUrl, StandardCharsets.UTF_8)) == null ) {
			logger.warn("Could not canonicalize internal url: " + tempLink);
			UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Discarded in 'PageCrawler.visit()' method, as there were canonicalization problems with the 'possibleDocUrl' found inside: " + tempLink, null, true);
			return false;
		}

		if ( UrlUtils.docUrlsWithIDs.containsKey(docLink) ) {    // If we got into an already-found docUrl, log it and return.
			logger.info("re-crossed docUrl found: < " + docLink + " >");
			LoaderAndChecker.reCrossedDocUrls ++;
			if ( FileUtils.shouldDownloadDocFiles )
				UrlUtils.logQuadruple(urlId, sourceUrl, pageUrl, docLink, UrlUtils.alreadyDownloadedByIDMessage + UrlUtils.docUrlsWithIDs.get(docLink), pageDomain, false);
			else
				UrlUtils.logQuadruple(urlId, sourceUrl, pageUrl, docLink, "", pageDomain, false);
			return true;
		}

		//logger.debug("Going to check DocLink: " + docLink);	// DEBUG!
		try {
			if ( !HttpConnUtils.connectAndCheckMimeType(urlId, sourceUrl, pageUrl, docLink, pageDomain, false, true) ) {    // We log the docUrl inside this method.
				logger.warn("The DocLink < " + docLink + " > was not a docUrl (unexpected)!");
				UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Discarded in 'PageCrawler.visit()' method, as the retrieved DocLink: < " + docLink + " > was not a docUrl.", null, true);
				return false;
			}
			return true;
		} catch (Exception e) {	// After connecting to the metaDocUrl.
			logger.warn("The DocLink < " + docLink + " > was not reached!");
			if (e instanceof RuntimeException)
				ConnSupportUtils.printEmbeddedExceptionMessage(e, docLink);
			UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Discarded in 'PageCrawler.visit()' method, as the retrieved DocLink: < " + docLink + " > had connectivity problems.", null, true);
			return false;
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
