package eu.openaire.publications_retriever.util.url;

import eu.openaire.publications_retriever.crawler.PageCrawler;
import eu.openaire.publications_retriever.util.args.ArgsUtils;
import eu.openaire.publications_retriever.util.http.ConnSupportUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * This class contains methods which inspect the urls -based on REGEXES and specific strings- and decide whether they are wanted or not.
 * @author Lampros Smyrnaios
 */
public class UrlTypeChecker
{
	private static final Logger logger = LoggerFactory.getLogger(UrlTypeChecker.class);

	private static final String htOrPhpExtensionsPattern = "(?:[\\w]?ht(?:[\\w]{1,2})?|php[\\d]{0,2})";
	private static final String mediaExtensionsPattern = "ico|gif|jpg|jpeg|png|wav|mp3|mp4|webm|mkv|mov";


	private static final String docOrDatasetKeywords = "(?:file|pdf|document|dataset|article|fulltext)";	// TODO - When adding more file-types, add the related exclusion here, as well.
	private static final String wordsPattern = "[\\w/_.,-]{0,100}";
	private static final String docOrDatasetNegativeLookAroundPattern = "(?<!" + wordsPattern + docOrDatasetKeywords + wordsPattern + ")(?!.*" + docOrDatasetKeywords + ".*)";
	// Note: Up to Java 8, we cannot use the "*" or "+" inside the lookbehind, so we use character-class with limits.

	public static Pattern URL_DIRECTORY_FILTER = null;	// Set this regex during runtime to account for the user's preference in selecting to retrieve documents only (not datasets).

	public static final String unsupportedDocFileTypes = "(?:(?:doc|ppt)[x]?|ps|epub|od[tp]|djvu|rtf)";
	public static final Pattern CURRENTLY_UNSUPPORTED_DOC_EXTENSION_FILTER = Pattern.compile(".+\\." + unsupportedDocFileTypes + "(?:\\?.+)?$");	// Doc-extensions which are currently unsupported. Some pageUrls give also .zip files, but that's another story.

	public static final Pattern URL_FILE_EXTENSION_FILTER = Pattern.compile(".+\\.(?:css|js(?:\\?y)?|" + mediaExtensionsPattern + "|pt|bib|nt|refer|enw|ris|mso|dtl|do|asc|c|cc" + docOrDatasetNegativeLookAroundPattern + "|cxx|cpp|java|py)(?:\\?.+)?$");
	// In the above, don't include .php and relative extensions, since even this can be a docUrl. For example: https://www.dovepress.com/getfile.php?fileID=5337
	// Some docUrls might end with capital "CC", so we apply a "lookaround" which ,makes sure the url is not blocked if it's possible to lead to a publication-file.

	public static final Pattern INTERNAL_LINKS_KEYWORDS_FILTER = Pattern.compile(".*(?:doi.org|\\?l[a]?n[g]?=|isallowed=n|site=|link(?:out|listener)|login).*");	// Plain key-words inside internalLinks-String. We avoid "doi.org" in internal links, as, after many redirects, they will reach the same pageUrl.
	// The diff with the "login" being here, in compare with being in "URL_DIRECTORY_FILTER"-regex, is that it can be found in a random place inside a url.. not just as a directory..

	// So, we make a new REGEX for these extensions, this time, without a potential argument in the end (e.g. ?id=XXX..), except for the potential "lang".
	public static final Pattern PLAIN_PAGE_EXTENSION_FILTER = Pattern.compile(".+(?<!" + docOrDatasetKeywords + ")\\.(?:" + htOrPhpExtensionsPattern+ "|[aj]sp[x]?|jsf|do|asc|cgi|cfm)(?:\\?(?!.*" + docOrDatasetKeywords + ").*)?$");	// We may have this page, which runs a script to return the pdf: "https://www.ijcseonline.org/pdf_paper_view.php?paper_id=4547&48-IJCSE-07375.pdf" or this pdf-internal-link: "https://meetingorganizer.copernicus.org/EGU2020/EGU2020-6296.html?pdf"
	
	public static Pattern INTERNAL_LINKS_FILE_FORMAT_FILTER = null;	// This includes filter for url-parameters.

	public static final Pattern SPECIFIC_DOMAIN_FILTER = Pattern.compile("[^/]+://[^/]*(?<=[/.])(?:(?<!drive.)google\\.|goo.gl|gstatic|facebook|fb.me|twitter|(?:meta|xing|baidu|t|x|vk).co|insta(?:gram|paper)|tiktok|youtube|vimeo|linkedin|ebay|bing|(?:amazon|[./]analytics)\\.|s.w.org|wikipedia|myspace|yahoo|mail|pinterest|reddit|tumblr"
			+ "|www.ccdc.cam.ac.uk|figshare.com/collections/|datadryad.org/stash/dataset/"
			+ "|evernote|skype|microsoft|adobe|buffer|digg|stumbleupon|addthis|delicious|dailymotion|gostats|blog(?:ger)?|copyright|friendfeed|newsvine|telegram|getpocket"
			+ "|flipboard|line.me|ok.rudouban|qzone|renren|weibo|doubleclick|bit.ly|github|reviewofbooks|plu.mx"
			+ "|(?<!files.)wordpress|orcid.org"
			+ "|auth(?:oriz(?:e|ation)|entication)?\\."

			// Block nearly all the "elsevier.com" urls, as well as the "sciencedirect.com" urls.
			// The "(linkinghub|api).elsevier.com" urls redirect -automatically or can be redirected manually- to the "sciencedirect.com", where the pdf is provided, BUT they cannot be retrieved.
			// The "sciencedirect.com" urls provide the pdf BUT! since some time now they require auth-tokens and decoding by javascript methods.
			// The ideal approach is to acquire an official api-key in order to retrieve the full-text in xml-format. Check: https://dev.elsevier.com/documentation/FullTextRetrievalAPI.wadl

			// The "<...>.pure.elsevier.com" urls give a page which does not contain the docUrl, but a doi-link instead, which leads to another page which contains the docUrl.
			// The "manuscript.elsevier.com" gives pdfs right away, so it should be allowed.
			// The "(www|journals).elsevier.com", come mostly from "doi.org"-urls.
			+ "|(?<!manuscript.)elsevier.com|sciencedirect.com"
			+ "|(?:static|multimedia|tienda).elsevier."	// Holds generic pdfs with various info about journals etc. or images or unrelated info.

			+ "|arvojournals.org"	// Avoid this problematic domain, which redirects to another domain, but also adds a special token, which cannot be replicated. Also, it has cookies issues.
			+ "|books.openedition.org"	// Avoid this closed-access sub-domain. (other subdomains, like "journals.openedition.org" are fine).
			+ "|perfdrive."	// Avoid "robot-check domain". It blocks quickly and redirect us to "validate.perfdrive.com".
			+ "|services.bepress.com"	// Avoid potential malicious domain (Avast had some urls of this domain in the Blacklist).
			+ "|(?:careers|shop).|myworkdayjobs.com"
			+ "|editorialmanager.com"

			// Add domains with a specific blocking-reason, in "capturing-groups", in order to be able to get the matched-group-number and know the exact reason the block occurred.

			+ "|(tandfonline.com|persee.fr|papers.ssrn.com|documentation.ird.fr|library.unisa.edu.au|publications.cnr.it)"	// 1. JavaScript-powered domains.
			// We could "guess" the pdf-link for some of them, but for "persee.fr" for example, there's also a captcha requirement.
			// The "tandfonline.com" cannot give even direct "/pdf/" urls, as it gives "HTTP 503 Server Error" or "HTTP 403 Forbidden", which urls appear to work when opened in a Browser.

			+ "|(doaj.org/toc/)"	// 2. Avoid resultPages (containing multiple publication-results).
			+ "|(dlib.org|saberes.fcecon.unr.edu.ar|eumed.net)"	// 3. Avoid HTML docUrls. These are shown simply inside the html-text of the page. No binary to download.
			+ "|(rivisteweb.it|wur.nl|remeri.org.mx|cam.ac.uk|scindeks.ceon.rs|egms.de)"	// 4. Avoid pages known to not provide docUrls (just metadata).
			+ "|(bibliotecadigital.uel.br|cepr.org)"	// 5. Avoid domains requiring login to access docUrls.
			+ "|(scielosp.org" + docOrDatasetNegativeLookAroundPattern + "|cepr.org|dk.um.si|apospublications.com|jorr.org|rwth-aachen.de|pubmed.ncbi.nlm.nih.gov)"	// 6. Avoid domains which have their DocUrls in larger depth (internalPagesToDocUrls or PreviousOfDocUrls).
			+ "|(200.17.137.108)"	// 7. Avoid known domains with connectivity problems.

			// Avoid slow urls (taking more than 3secs to connect). This is currently disabled since it was decided to let more pageUrl unblocked.
			//"handle.net", "doors.doshisha.ac.jp", "opac-ir.lib.osaka-kyoiku.ac.jp"
			/*
			// In case these "slow" domain are blocked later, use something like the following when handling them..
			loggingMessage = "Discarded after matching to domain, known to take long to respond.";
			logger.debug("Url-\"" + retrievedUrl + "\": " + loggingMessage);
			UrlUtils.logOutputData(urlId, retrievedUrl, null, UrlUtils.unreachableDocOrDatasetUrlIndicator, loggingMessage, "N/A", null, true, "true", wasUrlValid, "false", "false", "false", null, null);
			longToRespondUrls.incrementAndGet();
			*/

			+ ")[^/]*/.*");

	public static final Pattern PLAIN_DOMAIN_FILTER = Pattern.compile("[^/]+://[\\w.:-]+(?:/[\\w]{2})?(?:/index." + htOrPhpExtensionsPattern + ")?[/]?(?:\\?(?:locale(?:-attribute)?|ln)=[\\w_-]+)?$");	// Exclude plain domains' urls. Use "ISO 639-1" for language-codes (2 letters directory).

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
	 * This method depends on the initialization of the "ArgsUtils.retrieveDatasets" variable, given by the user as cmd-arg, or defined by a service which wraps this software.
	 * */
	public static void setRuntimeInitializedRegexes() {
		URL_DIRECTORY_FILTER =
			Pattern.compile("[^/]+://.*/(?:(?:(?:(?:discover|profile|user|survey|index|media|theme|product|deposit|default|shop|view)/" + docOrDatasetNegativeLookAroundPattern	// Avoid blocking these if the url is likely to give a file.
				+ "|(?:(?:ldap|password)-)?login|ac[c]?ess(?![./]+)|sign[-]?(?:in|out|up)|session|(?:how-to-)?(?:join[^t]|subscr)|authwall|regist(?:er|ration)|submi(?:t|ssion)|(?:post|send|export|(?:wp-)?admin|home|form|career[s]?|company)/|watch|browse|import|bookmark|announcement|feedback|share[^d]|about|(?:[^/]+-)?faq|wiki|news|events|cart|support|(?:site|html)map|documentation|help|license|disclaimer|copyright|(?:site-)?polic(?:y|ies)(?!.*paper)|privacy|terms|law|principles"
				+ "|(?:my|your|create)?[-]?account|my(?:dspace|selection|cart)|(?:service|help)[-]?desk|settings|fund|aut[h]?or" + docOrDatasetNegativeLookAroundPattern + "|journal/key|(?:journal-)?editor|author:|(?<!ntrs.nasa.gov/(?:api/)?)citation|review|external|facets|statistics|application|selfarchive|permission|ethic(s)?/.*/view/|/view/" + docOrDatasetNegativeLookAroundPattern + "|conta[c]?t|wallet|contribute|donate|our[_-][\\w]+|template|logo|image|photo/|video|advertiser|most-popular|people|(?:the)?press|for-authors|customer-service[s]?|captcha|clipboard|dropdown|widget"
				+ "|(?:forum|blog|column|row|js|[cr]ss|legal)/"	// These are absolute directory names.	TODO - Should I add the "|citation[s]?" rule ? BUT, The NASA-docUrls include it normally..
				+ "|(?:(?:advanced[-]?)?search|search/advanced|search-results|(?:[e]?books|journals)(?:-catalog)?|issue|docs|oai|(?:abstracting-)?indexing|online[-]?early|honors|awards|meetings|calendar|diversity|scholarships|invo(?:ice|lved)|errata|classroom|publish(?:-with-us)?|upload|products|forgot|home|ethics|comics|podcast|trends|bestof|booksellers|recommendations|bibliographic|volume[s]?)[/]?$"	// Url ends with these. Note that some of them are likely to be part of a docUrl, for ex. the "/trends/"-dir.
				+ "|rights[-]?permissions|publication[-]?ethics|advertising|reset[-]?password|\\*/|communit(?:y|ies)"
				+ "|restricted|noaccess|crawlprevention|error|(?:mis|ab)use|\\?denied|gateway|defaultwebpage|sorryserver|(?<!response_type=)cookie|(?:page-)?not[-]?found"
				+ "|(?:(?:error)?404(?:_response)?|accessibility|invalid|catalog(?:ue|ar|o)?)\\." + htOrPhpExtensionsPattern

				// Add pages with a specific blocking-reason, in "capturing-groups", in order to be able to get the matched-group-number and know the exact reason the block occurred.
				+ "|(.*/view/" + docOrDatasetNegativeLookAroundPattern + ")"	// 1. Avoid pages having their DocUrls in larger depth (internalPagesToDocUrls or PreviousOfDocUrls).
				+ "|(.*sharedsitesession)"	// 2. Avoid urls which contain either "getSharedSiteSession" or "consumeSharedSiteSession" as these cause an infinite loop.
				+ "|(doi.org/https://doi.org/.*pangaea." + (!ArgsUtils.retrieveDatasets ? "|pangaea.)" : ")")	// 3. Avoid "PANGAEA."-urls with problematic form and non docUrl internal links (yes WITH the "DOT", it's not a domain-name!).

				// The following pattern is the reason we need to set this regex in runtime.
				+ (!ArgsUtils.retrieveDatasets ? ").*)|(?:bibtext|dc(?:terms)?|[^/]*(?:tei|endnote))$)" : ")).*)")
			);

		// We check the above rules, mostly as directories to avoid discarding publications' urls about these subjects. There's "acesso" (single "c") in Portuguese.. Also there's "autore" & "contatto" in Italian.
		if ( logger.isTraceEnabled() )
			logger.trace("URL_DIRECTORY_FILTER:\n" + URL_DIRECTORY_FILTER);

		INTERNAL_LINKS_FILE_FORMAT_FILTER =
				Pattern.compile(".+format=(?:" + (!ArgsUtils.retrieveDatasets ? "xml|" : "") + htOrPhpExtensionsPattern + "|rss|ris|bib|citation_|events_kml).*");
	}

	
	/**
	 * This method matches the given pageUrl against general regex-es.
	 * It returns "true" if the givenUrl should not be accepted, otherwise, it returns "false".
	 *
	 * @param urlId
	 * @param sourceUrl
	 * @param pageUrl
	 * @param lowerCaseUrl
	 * @param calledForPageUrl
	 * @return true / false
	 */
	public static boolean shouldNotAcceptPageUrl(String urlId, String sourceUrl, String pageUrl, String lowerCaseUrl, boolean calledForPageUrl)
	{
		if ( lowerCaseUrl == null )
			lowerCaseUrl = pageUrl.toLowerCase();
		// If it's not "null", it means we have already done the transformation in the calling method.

		String loggingMessage = null;
		String wasUrlValid = "N/A";	// Default value to be used, in case the given url matches an unwanted type. We do not know if the url is valid (i.e. if it can be connected and give a non 4XX response) at this point.
		String groupMatch = null;

		Matcher matcher = URL_DIRECTORY_FILTER.matcher(lowerCaseUrl);
		if ( matcher.matches() ) {	// This regex also matches with many other rules, which we do not care to individually capture.
			if (calledForPageUrl ) {	// For internal-links we don't want to make further checks nor write results in the output, as further links will be checked for that page..
				if ( ((groupMatch = matcher.group(1)) != null) && !groupMatch.isEmpty() ) {
					loggingMessage = "Discarded after matching to a site having its DocUrls in larger depth: '" + groupMatch + "'.";
					UrlUtils.addOutputData(urlId, sourceUrl, pageUrl, UrlUtils.unreachableDocOrDatasetUrlIndicator, loggingMessage, "N/A", null, true, "true", wasUrlValid, "false", "false", "false", null, "null", "N/A");
					pagesWithLargerCrawlingDepth.incrementAndGet();
				}
				else if ( ((groupMatch = matcher.group(2)) != null) && !groupMatch.isEmpty() ) {
					ConnSupportUtils.blockSharedSiteSessionDomains(pageUrl, null);
					loggingMessage = "It was discarded after participating in a 'sharedSiteSession-endlessRedirectionPack': '" + groupMatch + "'.";
					UrlUtils.addOutputData(urlId, sourceUrl, pageUrl, UrlUtils.unreachableDocOrDatasetUrlIndicator, loggingMessage, "N/A", null, true, "true", wasUrlValid, "false", "false", "false", null, "null", "N/A");
					LoaderAndChecker.connProblematicUrls.incrementAndGet();
				}
				else if ( ((groupMatch = matcher.group(3)) != null) && !groupMatch.isEmpty() ) {
					loggingMessage = "Discarded after matching to a 'PANGAEA.' url with invalid form and non-docUrls in their internal links: '" + groupMatch + "'.";
					UrlUtils.addOutputData(urlId, sourceUrl, pageUrl, UrlUtils.unreachableDocOrDatasetUrlIndicator, loggingMessage, "N/A", null, true, "true", wasUrlValid, "false", "false", "false", null, "null", "N/A");
					pangaeaUrls.incrementAndGet();
				} else {
					loggingMessage = "Discarded after matching to a directory with problems.";
					UrlUtils.addOutputData(urlId, sourceUrl, pageUrl, UrlUtils.unreachableDocOrDatasetUrlIndicator, loggingMessage, "N/A", null, true, "true", wasUrlValid, "false", "false", "false", null, "null", "N/A");
				}
				logger.debug("Url-\"" + pageUrl + "\": " + loggingMessage);
			}
			return true;
		}

		matcher = SPECIFIC_DOMAIN_FILTER.matcher(lowerCaseUrl);
		if ( matcher.matches() ) {
			if ( calledForPageUrl ) {    // For internal-links we don't want to make further checks nor write results in the output, as further links will be checked for that page..
				if ( ((groupMatch = matcher.group(1)) != null) && !groupMatch.isEmpty() ) {
					loggingMessage = "Discarded after matching to a JavaScript-using domain, other than the 'sciencedirect.com': '" + groupMatch + "'.";
					UrlUtils.addOutputData(urlId, sourceUrl, pageUrl, UrlUtils.unreachableDocOrDatasetUrlIndicator, loggingMessage, "N/A", null, true, "true", wasUrlValid, "false", "false", "false", null, "null", "N/A");
					javascriptPageUrls.incrementAndGet();
				} else if ( ((groupMatch = matcher.group(2)) != null) && !groupMatch.isEmpty() ) {
					loggingMessage = "Discarded after matching to the Results-directory: 'doaj.org/toc/': '" + groupMatch + "'.";
					UrlUtils.addOutputData(urlId, sourceUrl, pageUrl, UrlUtils.unreachableDocOrDatasetUrlIndicator, loggingMessage, "N/A", null, true, "true", wasUrlValid, "false", "false", "false", null, "null", "N/A");
					doajResultPageUrls.incrementAndGet();
				} else if ( ((groupMatch = matcher.group(3)) != null) && !groupMatch.isEmpty() ) {
					loggingMessage = "Discarded after matching to a site containing the full-text as plain-text inside its HTML: '" + groupMatch + "'.";
					UrlUtils.addOutputData(urlId, sourceUrl, pageUrl, UrlUtils.unreachableDocOrDatasetUrlIndicator, loggingMessage, "N/A", null, true, "true", wasUrlValid, "false", "false", "false", null, "null", "N/A");
					pagesWithHtmlDocUrls.incrementAndGet();
				} else if ( ((groupMatch = matcher.group(4)) != null) && !groupMatch.isEmpty() ) {
					loggingMessage = "Discarded after matching to a domain which doesn't provide docUrls: '" + groupMatch + "'.";
					UrlUtils.addOutputData(urlId, sourceUrl, pageUrl, UrlUtils.unreachableDocOrDatasetUrlIndicator, loggingMessage, "N/A", null, true, "true", wasUrlValid, "false", "false", "false", null, "null", "N/A");
					pagesNotProvidingDocUrls.incrementAndGet();
				} else if ( ((groupMatch = matcher.group(5)) != null) && !groupMatch.isEmpty() ) {
					loggingMessage = "Discarded after matching to a domain which needs login to access docFiles: '" + groupMatch + "'.";
					UrlUtils.addOutputData(urlId, sourceUrl, pageUrl, UrlUtils.unreachableDocOrDatasetUrlIndicator, loggingMessage, "N/A", null, true, "true", wasUrlValid, "false", "false", "false", null, "null", "N/A");
					pagesRequireLoginToAccessDocFiles.incrementAndGet();
				} else if ( ((groupMatch = matcher.group(6)) != null) && !groupMatch.isEmpty() ) {
					loggingMessage = "Discarded after matching to a site having its DocUrls in larger depth: '" + groupMatch + "'.";
					UrlUtils.addOutputData(urlId, sourceUrl, pageUrl, UrlUtils.unreachableDocOrDatasetUrlIndicator, loggingMessage, "N/A", null, true, "true", wasUrlValid, "false", "false", "false", null, "null", "N/A");
					pagesWithLargerCrawlingDepth.incrementAndGet();
				} else if ( ((groupMatch = matcher.group(7)) != null) && !groupMatch.isEmpty() ) {
					loggingMessage = "Discarded after matching to known domains with connectivity problems: '" + groupMatch + "'.";
					UrlUtils.addOutputData(urlId, sourceUrl, pageUrl, UrlUtils.unreachableDocOrDatasetUrlIndicator, loggingMessage, "N/A", null, true, "true", wasUrlValid, "false", "false", "false", null, "null", "N/A");
					LoaderAndChecker.connProblematicUrls.incrementAndGet();
				} else {
					loggingMessage = "Discarded after matching to a domain with problems.";
					UrlUtils.addOutputData(urlId, sourceUrl, pageUrl, UrlUtils.unreachableDocOrDatasetUrlIndicator, loggingMessage, "N/A", null, true, "true", wasUrlValid, "false", "false", "false", null, "null", "N/A");
				}
				logger.debug("Url-\"" + pageUrl + "\": " + loggingMessage);
			}
			return true;
		}

		matcher = PageCrawler.NON_VALID_DOCUMENT.matcher(lowerCaseUrl);
		if ( matcher.matches() ) {
			if ( calledForPageUrl ) {    // For internal-links we don't want to make further checks nor write results in the output, as further links will be checked for that page..
				loggingMessage = "Discarded after matching to a url leading to an invalid document!";
				logger.debug("Url-\"" + pageUrl + "\": " + loggingMessage);
				UrlUtils.addOutputData(urlId, sourceUrl, pageUrl, UrlUtils.unreachableDocOrDatasetUrlIndicator, loggingMessage, "N/A", null, true, "true", wasUrlValid, "false", "false", "false", null, "null", "N/A");
				PageCrawler.contentProblematicUrls.incrementAndGet();
			}
			return true;
		}

		matcher = PLAIN_DOMAIN_FILTER.matcher(lowerCaseUrl);
		if ( matcher.matches() ) {
			if ( calledForPageUrl ) {    // For internal-links we don't want to make further checks nor write results in the output, as further links will be checked for that page..
				loggingMessage = "Discarded after matching to a url having only the domain part!";
				logger.debug("Url-\"" + pageUrl + "\": " + loggingMessage);
				UrlUtils.addOutputData(urlId, sourceUrl, pageUrl, UrlUtils.unreachableDocOrDatasetUrlIndicator, loggingMessage, "N/A", null, true, "true", wasUrlValid, "false", "false", "false", null, "null", "N/A");
			}
			return true;
		}

		matcher = URL_FILE_EXTENSION_FILTER.matcher(lowerCaseUrl);
		if ( matcher.matches() ) {
			if ( calledForPageUrl ) {    // For internal-links we don't want to make further checks nor write results in the output, as further links will be checked for that page..
				loggingMessage = "Discarded after matching to a url having an irrelevant extension!";
				logger.debug("Url-\"" + pageUrl + "\": " + loggingMessage);
				UrlUtils.addOutputData(urlId, sourceUrl, pageUrl, UrlUtils.unreachableDocOrDatasetUrlIndicator, loggingMessage, "N/A", null, true, "true", wasUrlValid, "false", "false", "false", null, "null", "N/A");
			}
			return true;
		}

		if ( ArgsUtils.shouldDownloadDocFiles ) {
			matcher = CURRENTLY_UNSUPPORTED_DOC_EXTENSION_FILTER.matcher(lowerCaseUrl);
			if ( matcher.matches() ) {	// TODO - To be removed when these docExtensions get supported for download.
				if ( calledForPageUrl ) {    // For internal-links we don't want to make further checks nor write results in the output, as further links will be checked for that page..
					loggingMessage = "Discarded after matching to a url having an unsupported document extension!";
					logger.debug("Url-\"" + pageUrl + "\": " + loggingMessage);
					UrlUtils.addOutputData(urlId, sourceUrl, pageUrl, UrlUtils.unreachableDocOrDatasetUrlIndicator, loggingMessage, "N/A", null, true, "true", wasUrlValid, "false", "false", "false", null, "null", "N/A");
				}
				return true;
			}
		}

		return false;
	}
	
	
	public static boolean shouldNotAcceptInternalLink(String linkStr, String lowerCaseLink)
	{
		if ( lowerCaseLink == null )
			lowerCaseLink = linkStr.toLowerCase();
		// If it's not "null", it means we have already done the transformation in the calling method.

		return	shouldNotAcceptPageUrl(null, null, linkStr, lowerCaseLink, false)
				|| INTERNAL_LINKS_KEYWORDS_FILTER.matcher(lowerCaseLink).matches()
				|| INTERNAL_LINKS_FILE_FORMAT_FILTER.matcher(lowerCaseLink).matches()
				|| PLAIN_PAGE_EXTENSION_FILTER.matcher(lowerCaseLink).matches();
	}
	
}
