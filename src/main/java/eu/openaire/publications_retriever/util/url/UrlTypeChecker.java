package eu.openaire.publications_retriever.util.url;

import eu.openaire.publications_retriever.crawler.PageCrawler;
import eu.openaire.publications_retriever.util.http.ConnSupportUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;


/**
 * This class contains methods which inspect the urls -based on REGEXES and specific strings- and decide whether they are wanted or not.
 * @author Lampros Smyrnaios
 */
public class UrlTypeChecker
{
	private static final Logger logger = LoggerFactory.getLogger(UrlTypeChecker.class);

	private static final String htExtensionsPattern = "[\\w]?ht(?:[\\w]{1,2})?";
	private static final String phpExtensionsPattern = "php[\\d]?";
	private static final String mediaExtensionsPattern = "ico|gif|jpg|jpeg|png|wav|mp3|mp4|webm|mkv|mov";

	public static final Pattern URL_DIRECTORY_FILTER =
			Pattern.compile("[^/]+://.*/(?:(discover|profile|user|survey)(?!.+(?:file|pdf|document|dataset))|(?:(?:ldap|password)-)?login|auth(?:entication)?\\.|ac[c]?ess(?!\\.)|sign[-]?(?:in|out|up)|session|join|subscr|register|submi(?:t|ssion)|(?:post|send|shop|view|export|(?:wp-)?admin|home|form)/|watch|browse|import|bookmark|announcement|rss|feedback|share|about|faq|wiki|news|events|cart|support|(?:site|html)map|documentation|default/|help|license|disclaimer|copyright|(?:site-)?polic(?:y|ies)|privacy|terms|law|principles"
					+ "|(?:my|your|create)?[-]?account|(?:service|help)[-]?desk|settings|fund|aut[h]?or|(?:journal-)?editor|author:|(?<!ntrs.nasa.gov/(?:api/)?)citation|review|external|facets|statistics|application|selfarchive|permission|ethic(s)?/.*/view/|conta[c]?t|wallet|contribute|deposit|donate|our[_-][\\w]+|template|logo|image|photo|video|media|theme|advertiser|product|people|(?:the)?press|forum|blog|column|row|for-authors|css|js|captcha|clipboard"
					+ "|(?:(?:advanced[-]?)?search|search-results|(?:[e]?books|journals)(?:-catalog)?|issue|docs|index|oai|(?:abstracting-)?indexing|online[-]?early|honors|awards|careers|meetings|calendar|diversity|scholarships|invo(?:ice|lved)|errata|classroom)[/]?$"	// Url ends with these.
					// TODO - In case we have just DocUrls (not datasetUrls), exclude the following as well: "/(?:bibtext|dc(?:terms)?|tei|endnote)$", it could be added in another regex.. or do an initialization check and construct this regex based on the url-option provided.
					+ "|rights[-]?permissions|publication[-]?ethics|advertising|reset[-]?password|\\*/|communit(?:y|ies)"
					+ "|restricted|noaccess|crawlprevention|error|(?:mis|ab)use|\\?denied|gateway|defaultwebpage|sorryserver|cookie|notfound|(?:404|accessibility|invalid|catalog(?:ue|ar|o)?)\\." + htExtensionsPattern + ").*");
	// We check them as a directory to avoid discarding publications' urls about these subjects. There's "acesso" (single "c") in Portuguese.. Also there's "autore" & "contatto" in Italian.

	public static final Pattern CURRENTLY_UNSUPPORTED_DOC_EXTENSION_FILTER = Pattern.compile(".+\\.(?:(?:doc|ppt)[x]?|ps|epub|od[tp]|djvu|rtf)(?:\\?.+)?$");	// Doc-extensions which are currently unsupported. Some pageUrls give also .zip files, but that's another story.

	public static final Pattern URL_FILE_EXTENSION_FILTER = Pattern.compile(".+\\.(?:css|js(?:\\?y)?|" + mediaExtensionsPattern + "|pt|bib|nt|refer|enw|ris|mso|dtl|do|asc|c|cc|cxx|cpp|java|py)(?:\\?.+)?$");
	// In the above, don't include .php and relative extensions, since even this can be a docUrl. For example: https://www.dovepress.com/getfile.php?fileID=5337

	public static final Pattern INTERNAL_LINKS_KEYWORDS_FILTER = Pattern.compile(".*(?:doi.org|\\?l[a]?n[g]?=|isallowed=n|site=|linkout|login|linklistener).*");	// Plain key-words inside internalLinks-String. We avoid "doi.org" in internal links, as, after many redirects, they will reach the same pageUrl.
	// The diff with the "login" being here, in compare with being in "URL_DIRECTORY_FILTER"-regex, is that it can be found in a random place inside a url.. not just as a directory..

	// So, we make a new REGEX for these extensions, this time, without a potential argument in the end (e.g. ?id=XXX..), except for the potential "lang".
	public static final Pattern PLAIN_PAGE_EXTENSION_FILTER = Pattern.compile(".+\\.(?:" + phpExtensionsPattern + "|" + htExtensionsPattern + "|[aj]sp[x]*|jsf|do|asc|cgi)(?:\\?.+)?$");
	
	public static final Pattern INTERNAL_LINKS_FILE_FORMAT_FILTER = Pattern.compile(".+format=(?:xml|" + htExtensionsPattern + "|rss|ris|bib).*");	// This exists as a url-parameter.

	public static final Pattern SPECIFIC_DOMAIN_FILTER = Pattern.compile("[^/]+://.*(?:(?<!drive.)google\\.|goo.gl|gstatic|facebook|twitter|insta(?:gram|paper)|youtube|vimeo|linkedin|wordpress|ebay|bing|(?:amazon|analytics)\\.|s.w.org|wikipedia|myspace|yahoo|mail|pinterest|reddit|tumblr"
			+ "|www.ccdc.cam.ac.uk|figshare.com/collections/|datadryad.org/stash/dataset/"
			+ "|evernote|skype|(?<!academic.)microsoft|adobe|buffer|digg|stumbleupon|addthis|delicious|dailymotion|gostats|blog(?:ger)?|copyright|friendfeed|newsvine|telegram|getpocket"
			+ "|flipboard|line.me|vk|ok.rudouban|baidu|qzone|xing|renren|weibo|doubleclick|bit.ly|github|reviewofbooks"
			+ "|(?<!linkinghub.)elsevier.com"	// Avoid pageUrls redirecting to plain "elsevier.com"  ("(www|journals).elsevier.com", coming mostly from "doi.org"-urls).
			+ "|arvojournals.org"	// Avoid this problematic domain, which redirects to another domain, but also adds a special token, which cannot be replicated. Also, it has cookies issues.
			+ "|perfdrive."	// Avoid "robot-check domain". It blocks quickly and redirect us to "validate.perfdrive.com".
			+ "|services.bepress.com"	// Avoid potential malicious domain (Avast had some urls of this domain in the Blacklist).
			+ "|careers."
			+ ").*/.*");

	public static final Pattern PLAIN_DOMAIN_FILTER = Pattern.compile("[^/]+://[\\w.:-]+(?:/[\\w]{2})?(?:/index.(?:" + htExtensionsPattern + "|" + phpExtensionsPattern + "))?[/]?(?:\\?(?:locale(?:-attribute)?|ln)=[\\w_-]+)?$");	// Exclude plain domains' urls. Use "ISO 639-1" for language-codes (2 letters directory).

	// Counters for certain unwanted domains. We show statistics in the end.
	public static AtomicInteger javascriptPageUrls = new AtomicInteger(0);
	public static AtomicInteger crawlerSensitiveDomains = new AtomicInteger(0);
	public static AtomicInteger doajResultPageUrls = new AtomicInteger(0);
	public static AtomicInteger pagesWithHtmlDocUrls = new AtomicInteger(0);
	public static AtomicInteger pagesRequireLoginToAccessDocFiles = new AtomicInteger(0);
	public static AtomicInteger pagesWithLargerCrawlingDepth = new AtomicInteger(0);	// Pages with their docUrl behind an internal "view" page.
	public static AtomicInteger longToRespondUrls = new AtomicInteger(0);	// Urls belonging to domains which take too long to respond.
	public static AtomicInteger urlsWithUnwantedForm = new AtomicInteger(0);	// (plain domains, unwanted page-extensions ect.)
	public static AtomicInteger pangaeaUrls = new AtomicInteger(0);	// These urls are in false form by default, but even if they weren't, or we transform them, PANGAEA. only gives datasets, not fulltext.
	public static AtomicInteger pagesNotProvidingDocUrls = new AtomicInteger(0);
	
	
	/**
	 * This method takes the "retrievedUrl" from the inputFile and the "lowerCaseUrl" that comes out the retrieved one.
	 * It then checks if the "lowerCaseUrl" matched certain criteria representing the unwanted urls' types. It uses the "retrievedUrl" for proper logging.
	 * If these criteria match, then it logs the url and returns "true", otherwise, it returns "false".
	 * @param urlId
	 * @param lowerCaseUrl
	 * @return true/false
	 */
	public static boolean matchesUnwantedUrlType(String urlId, String retrievedUrl, String lowerCaseUrl)
	{
		String loggingMessage = null;
		
		// Avoid JavaScript-powered domains, other than the "sciencedirect.com", which is handled separately.
		// We could "guess" the pdf-link for some of them, but for "persee.fr" for ex. there's also a captcha requirement for the connection, also the "tandfonline.com" wants its cookies, otherwise it redirects to "cookieAbsent"..
		if ( lowerCaseUrl.contains("tandfonline.com") || lowerCaseUrl.contains("persee.fr")
			|| lowerCaseUrl.contains("documentation.ird.fr") )	// The "documentation.ird.fr" works in "UrlCheck-test" but not when running multiple urls from the inputFile.
		{
			loggingMessage = "Discarded after matching to a JavaScript-using domain, other than the 'sciencedirect.com'.";
			logger.debug("Url-\"" + retrievedUrl + "\": " + loggingMessage);
			UrlUtils.logOutputData(urlId, retrievedUrl, null, "unreachable", loggingMessage, null, true, "true", "N/A", "false", "false", "false", null, "null");
			if ( !LoaderAndChecker.useIdUrlPairs )
				javascriptPageUrls.incrementAndGet();
			return true;
		}
		// Avoid resultPages (containing multiple publication-results).
		else if ( lowerCaseUrl.contains("doaj.org/toc/") ) {
			loggingMessage = "Discarded after matching to the Results-directory: 'doaj.org/toc/'.";
			logger.debug("Url-\"" + retrievedUrl + "\": " + loggingMessage);
			UrlUtils.logOutputData(urlId, retrievedUrl, null, "unreachable", loggingMessage, null, true, "true", "N/A", "false", "false", "false", null, "null");
			if ( !LoaderAndChecker.useIdUrlPairs )
				doajResultPageUrls.incrementAndGet();
			return true;
		}
		// Avoid HTML docUrls. These are shown simply inside the html-text of the page. No binary to download.
		else if ( lowerCaseUrl.contains("dlib.org") || lowerCaseUrl.contains("saberes.fcecon.unr.edu.ar") || lowerCaseUrl.contains("eumed.net") ) {
			loggingMessage = "Discarded after matching to a site containing the full-text as plain-text inside its HTML.";
			logger.debug("Url-\"" + retrievedUrl + "\": " + loggingMessage);
			UrlUtils.logOutputData(urlId, retrievedUrl, null, "unreachable", loggingMessage, null, true, "true", "N/A", "false", "false", "false", null, "null");
			if ( !LoaderAndChecker.useIdUrlPairs )
				pagesWithHtmlDocUrls.incrementAndGet();
			return true;
		}
		// Avoid pages known to not provide docUrls (just metadata).
		else if ( lowerCaseUrl.contains("rivisteweb.it") || lowerCaseUrl.contains("wur.nl") || lowerCaseUrl.contains("remeri.org.mx")	// Keep only "remeri" subDomain of "org.mx", as the TLD is having a lot of different sites.
				|| lowerCaseUrl.contains("cam.ac.uk") || lowerCaseUrl.contains("scindeks.ceon.rs") || lowerCaseUrl.contains("egms.de") ) {
			loggingMessage = "Discarded after matching to a domain which doesn't provide docUrls.";
			logger.debug("Url-\"" + retrievedUrl + "\": " + loggingMessage);
			UrlUtils.logOutputData(urlId, retrievedUrl, null, "unreachable", loggingMessage, null, true, "true", "N/A", "false", "false", "false", null, "null");
			if ( !LoaderAndChecker.useIdUrlPairs )
				pagesNotProvidingDocUrls.incrementAndGet();
			return true;
		}
		// Avoid domains requiring login to access docUrls.
		else if ( lowerCaseUrl.contains("bibliotecadigital.uel.br") || lowerCaseUrl.contains("cepr.org") ) {
			loggingMessage = "Discarded after matching to a domain which needs login to access docFiles.";
			logger.debug("Url-\"" + retrievedUrl + "\": " + loggingMessage);
			UrlUtils.logOutputData(urlId, retrievedUrl, null, "unreachable", loggingMessage, null, true, "true", "N/A", "false", "false", "false", null, "null");
			if ( !LoaderAndChecker.useIdUrlPairs )
				pagesRequireLoginToAccessDocFiles.incrementAndGet();
			return true;
		}
		// Avoid crawling pages having their DocUrls in larger depth (internalPagesToDocUrls or PreviousOfDocUrls).
		else if ( lowerCaseUrl.contains("/view/") || lowerCaseUrl.contains("scielosp.org") || lowerCaseUrl.contains("dk.um.si") || lowerCaseUrl.contains("apospublications.com")
				|| lowerCaseUrl.contains("jorr.org") || lowerCaseUrl.contains("rwth-aachen.de") || lowerCaseUrl.contains("pubmed.ncbi.nlm.nih.gov") ) {
			loggingMessage = "Discarded after matching to a site having its DocUrls in larger depth.";
			logger.debug("Url-\"" + retrievedUrl + "\": " + loggingMessage);
			UrlUtils.logOutputData(urlId, retrievedUrl, null, "unreachable", loggingMessage, null, true, "true", "N/A", "false", "false", "false", null, "null");
			if ( !LoaderAndChecker.useIdUrlPairs )
				pagesWithLargerCrawlingDepth.incrementAndGet();
			return true;
		}
		// Avoid "PANGAEA."-urls with problematic form and non docUrl internal links (yes WITH the "DOT").
		else if ( lowerCaseUrl.contains("doi.org/https://doi.org/") && lowerCaseUrl.contains("pangaea.") ) {
			loggingMessage = "Discarded after matching to 'PANGAEA.' urls with invalid form and non-docUrls in their internal links.";
			logger.debug("Url-\"" + retrievedUrl + "\": " + loggingMessage);
			UrlUtils.logOutputData(urlId, retrievedUrl, null, "unreachable", loggingMessage, null, true, "true", "N/A", "false", "false", "false", null, "null");
			if ( !LoaderAndChecker.useIdUrlPairs )
				pangaeaUrls.incrementAndGet();
			return true;
		}
		// Avoid known domains with connectivity problems.
		else if ( lowerCaseUrl.contains("200.17.137.108") ) {
			loggingMessage = "Discarded after matching to known urls with connectivity problems.";
			logger.debug("Url-\"" + retrievedUrl + "\": " + loggingMessage);
			UrlUtils.logOutputData(urlId, retrievedUrl, null, "unreachable", loggingMessage, null, true, "true", "N/A", "false", "false", "false", null, "null");
			if ( !LoaderAndChecker.useIdUrlPairs )
				LoaderAndChecker.connProblematicUrls.incrementAndGet();
			return true;
		}
		/*
		// Avoid slow urls (taking more than 3secs to connect). This is currently disabled since it was decided to let more pageUrl unblocked.
		else if ( lowerCaseUrl.contains("handle.net") || lowerCaseUrl.contains("doors.doshisha.ac.jp") || lowerCaseUrl.contains("opac-ir.lib.osaka-kyoiku.ac.jp") ) {
			loggingMessage = "Discarded after matching to domain, known to take long to respond.";
			logger.debug("Url-\"" + retrievedUrl + "\": " + loggingMessage);
			UrlUtils.logOutputData(urlId, retrievedUrl, null, "unreachable", loggingMessage, null, true, "true", "N/A", "false", "false", "false", null, null);
			if ( !LoaderAndChecker.useIdUrlPairs )
				longToRespondUrls ++;
			return true;
		}*/
		// Avoid urls which contain either "getSharedSiteSession" or "consumeSharedSiteSession" as these cause an infinite loop.
		else if ( lowerCaseUrl.contains("sharedsitesession") ) {
			ConnSupportUtils.blockSharedSiteSessionDomains(retrievedUrl, null);
			loggingMessage = "It was discarded after participating in a 'sharedSiteSession-endlessRedirectionPack'.";
			logger.debug("Url-\"" + retrievedUrl + "\": " + loggingMessage);
			UrlUtils.logOutputData(urlId, retrievedUrl, null, "unreachable", loggingMessage, null, true, "true", "N/A", "false", "false", "false", null, "null");
			if ( !LoaderAndChecker.useIdUrlPairs )
				LoaderAndChecker.connProblematicUrls.incrementAndGet();
			return true;
		}
		// Avoid pages based on unwanted url-string-content.
		else if ( shouldNotAcceptPageUrl(retrievedUrl, lowerCaseUrl) ) {
			loggingMessage = "Discarded after matching to unwantedType-regex-rules.";
			logger.debug("Url-\"" + retrievedUrl + "\": " + loggingMessage);
			UrlUtils.logOutputData(urlId, retrievedUrl, null, "unreachable", loggingMessage, null, true, "true", "N/A", "false", "false", "false", null, "null");
			if ( !LoaderAndChecker.useIdUrlPairs )
				urlsWithUnwantedForm.incrementAndGet();
			return true;
		}
		else
			return false;	// No logging needed here.
	}
	
	
	/**
	 * This method matches the given pageUrl against general regex-es.
	 * It returns "true" if the givenUrl should not be accepted, otherwise, it returns "false".
	 * @param pageUrl
	 * @param lowerCaseUrl
	 * @return true / false
	 */
	public static boolean shouldNotAcceptPageUrl(String pageUrl, String lowerCaseUrl)
	{
		if ( lowerCaseUrl == null )
			lowerCaseUrl = pageUrl.toLowerCase();
		// If it's not "null", it means we have already done the transformation in the calling method.
		
		return	PLAIN_DOMAIN_FILTER.matcher(lowerCaseUrl).matches() || SPECIFIC_DOMAIN_FILTER.matcher(lowerCaseUrl).matches()
				|| URL_DIRECTORY_FILTER.matcher(lowerCaseUrl).matches() || URL_FILE_EXTENSION_FILTER.matcher(lowerCaseUrl).matches()
				|| PageCrawler.NON_VALID_DOCUMENT.matcher(lowerCaseUrl).matches()
				|| CURRENTLY_UNSUPPORTED_DOC_EXTENSION_FILTER.matcher(lowerCaseUrl).matches();	// TODO - To be removed when these docExtensions get supported.
	}
	
	
	public static boolean shouldNotAcceptInternalLink(String linkStr, String lowerCaseLink)
	{
		if ( lowerCaseLink == null )
			lowerCaseLink = linkStr.toLowerCase();
		// If it's not "null", it means we have already done the transformation in the calling method.
		
		return	URL_DIRECTORY_FILTER.matcher(lowerCaseLink).matches() || INTERNAL_LINKS_KEYWORDS_FILTER.matcher(lowerCaseLink).matches()
				|| SPECIFIC_DOMAIN_FILTER.matcher(lowerCaseLink).matches() || PLAIN_DOMAIN_FILTER.matcher(lowerCaseLink).matches()
				|| URL_FILE_EXTENSION_FILTER.matcher(lowerCaseLink).matches() || INTERNAL_LINKS_FILE_FORMAT_FILTER.matcher(lowerCaseLink).matches()
				|| PLAIN_PAGE_EXTENSION_FILTER.matcher(lowerCaseLink).matches()
				|| PageCrawler.NON_VALID_DOCUMENT.matcher(lowerCaseLink).matches()
				|| CURRENTLY_UNSUPPORTED_DOC_EXTENSION_FILTER.matcher(lowerCaseLink).matches();	// TODO - To be removed when these docExtensions get supported.
	}
	
}
