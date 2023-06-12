package eu.openaire.publications_retriever.crawler;

import eu.openaire.publications_retriever.PublicationsRetriever;
import eu.openaire.publications_retriever.exceptions.*;
import eu.openaire.publications_retriever.util.http.ConnSupportUtils;
import eu.openaire.publications_retriever.util.http.HttpConnUtils;
import eu.openaire.publications_retriever.util.url.LoaderAndChecker;
import eu.openaire.publications_retriever.util.url.UrlTypeChecker;
import eu.openaire.publications_retriever.util.url.UrlUtils;
import org.apache.commons.lang3.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Attribute;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.net.HttpURLConnection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * @author Lampros Smyrnaios
 */
public class PageCrawler
{
	private static final Logger logger = LoggerFactory.getLogger(PageCrawler.class);

	private static final Pattern INTERNAL_LINKS_STARTING_FROM_FILTER = Pattern.compile("^(?:(?:mailto|tel|fax|file|data|whatsapp|visible|click|text|attr):|\\{openurl}|[/]*\\?(?:locale(?:-attribute)?|ln)=).*");

	public static final Pattern JAVASCRIPT_DOC_LINK = Pattern.compile("javascript:pdflink.*'(http.+)'[\\s]*,.*", Pattern.CASE_INSENSITIVE);

	public static final Pattern JAVASCRIPT_CODE_PDF_LINK = Pattern.compile(".*\"pdfUrl\":\"([^\"]+)\".*");	// TODO - Check if this case is common, in order to handle it.

	public static final ConcurrentHashMap<String, Integer> timesDomainNotGivingInternalLinks = new ConcurrentHashMap<String, Integer>();
	public static final ConcurrentHashMap<String, Integer> timesDomainNotGivingDocUrls = new ConcurrentHashMap<String, Integer>();

	public static final int timesToGiveNoInternalLinksBeforeBlocked = 200;
	public static final int timesToGiveNoDocUrlsBeforeBlocked = 100;

	public static AtomicInteger contentProblematicUrls = new AtomicInteger(0);

	private static final int MAX_INTERNAL_LINKS_TO_ACCEPT_PAGE = 500;	// If a page has more than 500 internal links, then discard it. Example: "https://dblp.uni-trier.de/db/journals/corr/corr1805.html"
	private static final int MAX_POSSIBLE_DOC_OR_DATASET_LINKS_TO_CONNECT = 5;	// The < 5 > is the optimal value, figured out after experimentation. Example: "https://doaj.org/article/acf5f095dc0f49a59d98a6c3abca7ab6".

	public static boolean should_check_remaining_links = true;	// The remaining links very rarely give docUrls.. so, for time-performance, we can disable them.
	private static final int MAX_REMAINING_INTERNAL_LINKS_TO_CONNECT = 10;	// The < 10 > is the optimal value, figured out after experimentation.

	private static final String spaceOrDashes = "(?:\\s|%20|-|_)*";	// This includes the encoded space inside the url-string.

	public static final Pattern DOCUMENT_TEXT = Pattern.compile("pdf|full" + spaceOrDashes + "text|download|t[ée]l[ée]charger|descargar|texte" + spaceOrDashes + "intégral");

	// The following regex is used both in the text around the links and in the links themselves. Everything should be LOWERCASE, from the regex-rules to the link to be matched against them.
	public static final Pattern NON_VALID_DOCUMENT = Pattern.compile(".*(?:[^e]manu[ae]l|(?:\\|\\|" + spaceOrDashes + ")?gu[ií](?:de|a)|directive[s]?|preview|leaflet|agreement(?!.*thesis" + spaceOrDashes + "(?:19|20)[\\d]{2}.*)|accessibility|journal" + spaceOrDashes + "catalog|disclose" + spaceOrDashes + "file|poli(?:c(?:y|ies)|tika(?:si)?)"	// "policy" can be a lone word or a word after: repository|embargo|privacy|data protection|take down|supplement|access
																		// We may have the "Emanuel" writer's name in the url-string. Also, we may have the "agreement"-keyword in a valid pub-url like: https://irep.ntu.ac.uk/id/eprint/40188/1/__Opel.ads.ntu.ac.uk_IRep-PGR%24_2020%20Theses%20and%20deposit%20agreement%20forms_BLSS_NBS_FARRIER-WILLIAMS%2C%20Elizabeth_EFW%20Thesis%202020.pdf
																		+ "|licen(?:se|cia)" + spaceOrDashes + "(?:of|de)" + spaceOrDashes + "us[eo]|(?:governance|safety)" + spaceOrDashes + "statement|normativa|(?:consumer|hazard|copyright)" + spaceOrDashes + "(?:information|(?:release" + spaceOrDashes + ")?form)|copyright|permission|(?:editorial|review)" + spaceOrDashes + "board|d[ée](?:p(?:ôt[s]?|oser|osit)|butez)|cr[ée]er" + spaceOrDashes + "(?:votre|son)|orcid|subscription|instruction|code" + spaceOrDashes + "of" + spaceOrDashes + "conduct|request|join|compte|[^_]account"
																		+ "|table" + spaceOrDashes + "of" + spaceOrDashes + "contents|(?:front|back|end)" + spaceOrDashes + "matter|information" + spaceOrDashes + "for" + spaceOrDashes + "authors|pdf(?:/a)?" + spaceOrDashes + "conversion|catalogue|factsheet|classifieds"	// classifieds = job-ads
																		+ "|pdf-viewer|certificate" + spaceOrDashes + "of|conflict[s]?" + spaceOrDashes + "of" + spaceOrDashes + "interest|(?:recommendation|order)" + spaceOrDashes + "form|adverti[sz]e|mandatory" + spaceOrDashes + "open" + spaceOrDashes + "access|recommandations" + spaceOrDashes + "pour" + spaceOrDashes + "s'affilier|hal.*collections|terms|conditions|hakuohjeet|logigramme|export_liste_publi|yearbook|pubs_(?:brochure|overview)|thermal-letter|réutiliser" + spaceOrDashes + "des" + spaceOrDashes + "images" + spaceOrDashes + "dans" + spaceOrDashes + "des" + spaceOrDashes + "publications"
																		+ "|procedure|規程|運営規程"	// 規程 == procedure, 運営規程 = Operating regulations  (in japanese)
																		+ "|(?:peer|mini)" + spaceOrDashes + "review|(?:case|annual)" + spaceOrDashes + "report|review" + spaceOrDashes + "article|short" + spaceOrDashes + "communication|letter" + spaceOrDashes + "to" + spaceOrDashes + "editor|how" + spaceOrDashes + "to" + spaceOrDashes + "(?:create|submit|contact)|tutori[ae]l|survey-results|calendar" + spaceOrDashes + "of" + spaceOrDashes + "events|know" + spaceOrDashes + "your" + spaceOrDashes + "rights|your(?:" + spaceOrDashes + "id|cv)" + spaceOrDashes + "hal|présentation" + spaceOrDashes + "portail" + spaceOrDashes + "hal"
																		+ "|data-sharing-guidance|rate(?:" + spaceOrDashes + ")?cards|press" + spaceOrDashes + "release|liability" + spaceOrDashes + "disclaimer|(?:avec|dans)" + spaceOrDashes + "(?:ocd|x2)?hal|online" + spaceOrDashes + "flyer|publishing" + spaceOrDashes + "process|book" + spaceOrDashes + "of" + spaceOrDashes + "abstracts|academic" + spaceOrDashes + "social" + spaceOrDashes + "networks|ijcseugcjournalno|manuscript(?:" + spaceOrDashes + "preparation)?" + spaceOrDashes + "checklist|by" + spaceOrDashes + "laws"
																		+ "|^(?:licen[cs]e|help|reprints|pol[ií]ti[kc][sa](?:" + spaceOrDashes + "de" + spaceOrDashes + "informação)?|for" + spaceOrDashes + "recruiters|charte" + spaceOrDashes + "de" + spaceOrDashes + "signature|weekly" + spaceOrDashes + "visitors|publication" + spaceOrDashes + "(?:ethics" + spaceOrDashes + "and" + spaceOrDashes + "malpractice|fees)|redaktion|sample" + spaceOrDashes + "manuscript|open" + spaceOrDashes + "access)$"	// Single words/phrases inside the html-text.
																		+ "|/(?:entry|information|opinion|(?:rapportannuel|publerkl|utt_so_|atsc_|tjg_|ictrp_|oproep_voor_artikels_|[^/]*call_for_contributions_)[\\w_()-]*|accesorestringido|library_recommendation_form|research-article|loi_republique_numerique_publis|nutzungsbedingungen|autorenhinweise|mediadaten|canceledpresentations|sscc-facme_cirugia|bir_journals_reprint_form|transparencia|wfme|evolution_de_l_ergonomie|que_pouvez_vous_deposer|ethic-comittee-approval|restri(?:ngido|cted)|ofi[c]+ial|asn" + spaceOrDashes + "tips|aidehelp|.*_doi|(?:b-ent|aces)_.*).pdf(?:\\?.*)?$"	// The plain "research-article.pdf" is the template provided by journals.
																		+ "|kilavuzu"	// "guide" in Turkish
																		+ "|(?:公表|登録)届出書|取扱要領|リポジトリ(?:要項|運用指針)|検索のポイント|について|閲覧方法|ープンアクセスポリシー|されたみなさまへ|(?:論文の|登録)許諾書|著作権利用許諾要件|削除依頼書).*");	// registration/notification form/statement, instructions, repository requirements/operation guidelines, search point, how to browse (all in japanese), open access guide, to all of you, dissertation consent form / Registration License / Copyright license requirements / deletion request form

	// Example of docUrl having the "editorial" keyword: https://publikationen.ub.uni-frankfurt.de/opus4/frontdoor/deliver/index/docId/45461/file/daek_et_al_2017_editorial.pdf

	public static void visit(String urlId, String sourceUrl, String pageUrl, String pageContentType, HttpURLConnection conn, String firstHTMLlineFromDetectedContentType, BufferedReader bufferedReader)
	{
		logger.debug("Visiting pageUrl: \"" + pageUrl + "\".");

		String pageDomain = UrlUtils.getDomainStr(pageUrl, null);
		if ( pageDomain == null ) {    // If the domain is not found, it means that a serious problem exists with this docPage, and we shouldn't crawl it.
			logger.warn("Problematic URL in \"PageCrawler.visit()\": \"" + pageUrl + "\"");
			UrlUtils.logOutputData(urlId, sourceUrl, null, UrlUtils.unreachableDocOrDatasetUrlIndicator, "Discarded in PageCrawler.visit() method, after the occurrence of a domain-retrieval error.", null, true, "true", "false", "false", "false", "false", null, "null");
			LoaderAndChecker.connProblematicUrls.incrementAndGet();
			ConnSupportUtils.closeBufferedReader(bufferedReader);	// This page's content-type was auto-detected, and the process fails before re-requesting the conn-inputStream, then make sure we close the last one.
			return;
		}

		String pageHtml = null;	// Get the pageHtml to parse the page.
		if ( (pageHtml = ConnSupportUtils.getHtmlString(conn, bufferedReader)) == null ) {
			logger.warn("Could not retrieve the HTML-code for pageUrl: " + pageUrl);
			UrlUtils.logOutputData(urlId, sourceUrl, null, UrlUtils.unreachableDocOrDatasetUrlIndicator, "Discarded in 'PageCrawler.visit()' method, as there was a problem retrieving its HTML-code. Its contentType is: '" + pageContentType + "'.", null, true, "true", "true", "false", "false", "true", null, "null");
			LoaderAndChecker.connProblematicUrls.incrementAndGet();
			// The "bufferedReader" is closed inside the above method.
			return;
		}
		else if ( firstHTMLlineFromDetectedContentType != null ) {
			pageHtml = firstHTMLlineFromDetectedContentType + pageHtml;
		}
		//logger.debug(pageHtml);	// DEBUG!

		if ( pageDomain.contains("turkjgastroenterol.org") ) {
			SpecialUrlsHandler.extractAndCheckTurkjgastroenterolDocUrl(pageHtml, urlId, sourceUrl, pageUrl, pageDomain);
			return;
		}

		// Check if the docLink is provided in a metaTag and connect to it directly.
		if ( MetaDocUrlsHandler.checkIfAndHandleMetaDocUrl(urlId, sourceUrl, pageUrl, pageDomain, pageHtml) )
			return;	// The sourceUrl is already logged inside the called method.

		HashSet<String> currentPageLinks = null;	// We use "HashSet" to avoid duplicates.
		if ( (currentPageLinks = retrieveInternalLinks(urlId, sourceUrl, pageUrl, pageDomain, pageHtml, pageContentType)) == null )
			return;	// The necessary logging is handled inside.

		String urlToCheck = null;
		boolean shouldRunPrediction = false;

		// Check if we want to use AND if so, if we should run, the MLA.
		if ( MachineLearning.useMLA ) {
			MachineLearning.totalPagesReachedMLAStage.incrementAndGet();	// Used for M.L.A.'s execution-manipulation.
			shouldRunPrediction = MachineLearning.shouldRunPrediction();
			if ( shouldRunPrediction ) {
				HashSet<String> newPageLinks = new HashSet<>(currentPageLinks.size());	// We use "HashSet" to avoid duplicates.
				for ( String currentLink : currentPageLinks )
				{
					// Produce fully functional internal links, NOT internal paths or non-normalized (if possible). The M.L.A. will evaluate whether the predictedDocUrls exist in the Set of internal-links.
					if ( ((urlToCheck = ConnSupportUtils.getFullyFormedUrl(pageUrl, currentLink, null)) == null)	// Make it a full-URL.
						|| ((urlToCheck = LoaderAndChecker.basicURLNormalizer.filter(urlToCheck)) == null) ) {    // Normalize it.
						logger.warn("Could not normalize internal url: " + currentLink);
						continue;
					}
					newPageLinks.add(urlToCheck);
				}
				currentPageLinks = newPageLinks;

				if ( MachineLearning.predictInternalDocUrl(urlId, sourceUrl, pageUrl, pageDomain, currentPageLinks) )    // Check if we can find the docUrl based on previous runs. (Still in experimental stage)
					return;	// If we were able to find the right path.. and hit a docUrl successfully.. return. The Quadruple is already logged.
			}
		}

		HashSet<String> remainingLinks = new HashSet<>(currentPageLinks.size());	// Used later. Initialize with the total num of links (less will actually get stored there, but their num is unknown).
		String lowerCaseLink = null;
		int possibleDocOrDatasetUrlsCounter = 0;

		// Do a fast-loop, try connecting only to a handful of promising links first.
		// Check if urls inside this page, match to a docUrl or to a datasetUrl regex, if they do, try connecting with them and see if they truly are docUrls. If they are, return.
		for ( String currentLink : currentPageLinks )
		{
			if ( !shouldRunPrediction) {	// If we used the MLA for this pageUrl, then this process is already handled for all urls. Otherwise, here we normalize only few links at best.
				// Produce fully functional internal links, NOT internal paths or non-normalized (if possible). The M.L.A. will evaluate whether the predictedDocUrls exist in the Set of internal-links.
				if ( ((urlToCheck = ConnSupportUtils.getFullyFormedUrl(pageUrl, currentLink, null)) == null)	// Make it a full-URL.
						|| ((urlToCheck = LoaderAndChecker.basicURLNormalizer.filter(urlToCheck)) == null) ) {    // Normalize it.
					logger.warn("Could not normalize internal url: " + currentLink);
					continue;
				}
			} else
				urlToCheck = currentLink;

            if ( UrlUtils.docOrDatasetUrlsWithIDs.containsKey(urlToCheck) ) {	// If we got into an already-found docUrl, log it and return.
				ConnSupportUtils.handleReCrossedDocUrl(urlId, sourceUrl, pageUrl, urlToCheck, false);
				return;
            }

            lowerCaseLink = urlToCheck.toLowerCase();
            if ( (LoaderAndChecker.retrieveDocuments && LoaderAndChecker.DOC_URL_FILTER.matcher(lowerCaseLink).matches())
				|| (LoaderAndChecker.retrieveDatasets && LoaderAndChecker.DATASET_URL_FILTER.matcher(lowerCaseLink).matches()) )
			{
				// Some docUrls may be in different domain, so after filtering the urls based on the possible type.. then we can allow to check for links in different domains.

				if ( UrlUtils.duplicateUrls.contains(urlToCheck) )
					continue;

				if ( UrlTypeChecker.shouldNotAcceptInternalLink(urlToCheck, lowerCaseLink) ) {    // Avoid false-positives, such as images (a common one: ".../pdf.png").
					UrlUtils.duplicateUrls.add(urlToCheck);
					continue;	// Disclaimer: This way we might lose some docUrls like this: "http://repositorio.ipen.br:8080/xmlui/themes/Mirage/images/Portaria-387.pdf".
				}	// Example of problematic url: "http://thredds.d4science.org/thredds/catalog/public/netcdf/AquamapsNative/catalog.html"

				if ( (++possibleDocOrDatasetUrlsCounter) > MAX_POSSIBLE_DOC_OR_DATASET_LINKS_TO_CONNECT ) {
					logger.warn("The maximum limit (" + MAX_POSSIBLE_DOC_OR_DATASET_LINKS_TO_CONNECT + ") of possible doc or dataset links to be connected was reached for pageUrl: \"" + pageUrl + "\". The page was discarded.");
					handlePageWithNoDocUrls(urlId, sourceUrl, pageUrl, pageDomain, true, false);
					return;
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
					String blockedDomain = dbe.getMessage();
					if ( (blockedDomain != null) && blockedDomain.contains(pageDomain) ) {
						logger.warn("Page: \"" + pageUrl + "\" left \"PageCrawler.visit()\" after it's domain was blocked.");
						String couldRetry = (LoaderAndChecker.COULD_RETRY_URLS.matcher(pageUrl).matches() ? "true" : "false");
						UrlUtils.logOutputData(urlId, sourceUrl, null, UrlUtils.unreachableDocOrDatasetUrlIndicator, "Logged in 'PageCrawler.visit()' method, as its domain was blocked during crawling.", null, true, "true", "true", "false", "false", couldRetry, null, "null");
						LoaderAndChecker.connProblematicUrls.incrementAndGet();
						return;
					}
					continue;
				} catch (ConnTimeoutException cte) {
					if ( urlToCheck.contains(pageDomain) ) {	// In this case, it's unworthy to stay and check other internalLinks here.
						logger.warn("Page: \"" + pageUrl + "\" left \"PageCrawler.visit()\" after a potentialDocUrl caused a ConnTimeoutException.");
						UrlUtils.logOutputData(urlId, sourceUrl, null, UrlUtils.unreachableDocOrDatasetUrlIndicator, "Logged in 'PageCrawler.visit()' method, as an internalLink of this page caused 'ConnTimeoutException'.", null, true, "true", "true", "false", "false", "true", null, "null");
						LoaderAndChecker.connProblematicUrls.incrementAndGet();
						return;
					}
					continue;
				} catch (Exception e) {	// The exception: "DomainWithUnsupportedHEADmethodException" should never be caught here, as we use "GET" for possibleDocOrDatasetUrls.
					logger.error("" + e);
					continue;
				}
            }

            remainingLinks.add(urlToCheck);	// Add the fully-formed & accepted remaining links into a new hashSet to be iterated.
		}// end for-loop

		// If we reached here, it means that we couldn't find a docUrl the quick way.. so we have to check some (we exclude lots of them) of the internal links one by one.

		if ( should_check_remaining_links && (remainingLinks.size() > 0) )
			checkRemainingInternalLinks(urlId, sourceUrl, pageUrl, pageDomain, remainingLinks);
		else
			handlePageWithNoDocUrls(urlId, sourceUrl, pageUrl, pageDomain, false, false);
	}


	/**
	 * This method handles
	 * @param urlId
	 * @param sourceUrl
	 * @param pageUrl
	 * @param pageDomain
	 * @param hasWarningLogBeenShown
	 * @param isAlreadyLoggedToOutput
	 */
	private static void handlePageWithNoDocUrls(String urlId, String sourceUrl, String pageUrl, String pageDomain, boolean hasWarningLogBeenShown, boolean isAlreadyLoggedToOutput)
	{
		// If we get here it means that this pageUrl is not a docUrl itself, nor it contains a docUrl..
		if ( !hasWarningLogBeenShown )
			logger.warn("Page: \"" + pageUrl + "\" does not contain a docUrl.");

		UrlTypeChecker.pagesNotProvidingDocUrls.incrementAndGet();
		if ( !isAlreadyLoggedToOutput )	// This check is used in error-cases, where we have already logged the Quadruple.
			UrlUtils.logOutputData(urlId, sourceUrl, null, UrlUtils.unreachableDocOrDatasetUrlIndicator, "Logged in 'PageCrawler.visit()' method, as no " + PublicationsRetriever.targetUrlType + " was found inside.", null, true, "true", "true", "false", "false", "false", null, "null");

		if ( ConnSupportUtils.countAndBlockDomainAfterTimes(HttpConnUtils.blacklistedDomains, PageCrawler.timesDomainNotGivingDocUrls, pageDomain, PageCrawler.timesToGiveNoDocUrlsBeforeBlocked, true) )
			logger.warn("Domain: \"" + pageDomain + "\" was blocked after giving no docUrls more than " + PageCrawler.timesToGiveNoDocUrlsBeforeBlocked + " times.");
	}


	public static HashSet<String> retrieveInternalLinks(String urlId, String sourceUrl, String pageUrl, String pageDomain, String pageHtml, String pageContentType)
	{
		HashSet<String> currentPageLinks = null;
		try {
			currentPageLinks = extractInternalLinksFromHtml(pageHtml, pageUrl);
		} catch (RuntimeException re) {
			String exceptionMessage = re.getMessage();
			exceptionMessage = ((exceptionMessage == null) ? "No reason was given!" : exceptionMessage);
			logger.warn(exceptionMessage + " This page was discarded.");
			UrlUtils.logOutputData(urlId, sourceUrl, null, UrlUtils.unreachableDocOrDatasetUrlIndicator, "Discarded in 'PageCrawler.retrieveInternalLinks()' method, with reason: " + exceptionMessage, null, true, "true", "true", "false", "false", "false", null, "null");
			contentProblematicUrls.incrementAndGet();
			return null;
		} catch (DynamicInternalLinksFoundException dilfe) {
			HttpConnUtils.blacklistedDomains.add(pageDomain);
			logger.warn("Page: \"" + pageUrl + "\" left \"PageCrawler.visit()\" after found to have dynamic links. Its domain \"" + pageDomain + "\"  was blocked.");	// Refer "PageCrawler.visit()" here for consistency with other similar messages.
			UrlUtils.logOutputData(urlId, sourceUrl, null, UrlUtils.unreachableDocOrDatasetUrlIndicator, "Logged in 'PageCrawler.retrieveInternalLinks()', as it belongs to a domain with dynamic-links.", null, true, "true", "true", "false", "false", "false", null, "null");
			PageCrawler.contentProblematicUrls.incrementAndGet();
			return null;
		} catch ( DocLinkFoundException dlfe) {
			if ( !verifyDocLink(urlId, sourceUrl, pageUrl, pageDomain, pageContentType, dlfe) )	// url-logging is handled inside.
				handlePageWithNoDocUrls(urlId, sourceUrl, pageUrl, pageDomain, false, true);
			return null;	// This DocLink is the only docLink we will ever go to get from this page. The sourceUrl is logged inside the called method.
			// If this "DocLink" is a DocUrl, then returning "null" here, will trigger the 'PageCrawler.retrieveInternalLinks()' method to exit immediately (and normally).
		} catch ( DocLinkInvalidException dlie ) {
			//logger.warn("An invalid docLink < " + dlie.getMessage() + " > was found for pageUrl: \"" + pageUrl + "\". Search was stopped.");	// DEBUG!
			UrlUtils.logOutputData(urlId, sourceUrl, null, UrlUtils.unreachableDocOrDatasetUrlIndicator, "Discarded in 'PageCrawler.retrieveInternalLinks()' method, as there was an invalid docLink. Its contentType is: '" + pageContentType + "'", null, true, "true", "true", "false", "false", "false", null, "null");
			handlePageWithNoDocUrls(urlId, sourceUrl, pageUrl, pageDomain, false, true);
			return null;
		} catch (Exception e) {
			logger.warn("Could not retrieve the internalLinks for pageUrl: " + pageUrl);
			UrlUtils.logOutputData(urlId, sourceUrl, null, UrlUtils.unreachableDocOrDatasetUrlIndicator, "Discarded in 'PageCrawler.retrieveInternalLinks()' method, as there was a problem retrieving its internalLinks. Its contentType is: '" + pageContentType + "'", null, true, "true", "true", "false", "false", "false", null, "null");
			PageCrawler.contentProblematicUrls.incrementAndGet();
			return null;
		}

		boolean isNull = (currentPageLinks == null);	// This is the case, only when Jsoup could not extract any link-elements from the html.
		boolean isEmpty = false;

		if ( !isNull )
			isEmpty = (currentPageLinks.size() == 0);

		if ( isNull || isEmpty ) {	// If no links were retrieved (e.g. the pageUrl was some kind of non-page binary content)
			logger.warn("No " + (isEmpty ? "valid" : "available") + " links were able to be retrieved from pageUrl: \"" + pageUrl + "\". Its contentType is: " + pageContentType);
			PageCrawler.contentProblematicUrls.incrementAndGet();
			UrlUtils.logOutputData(urlId, sourceUrl, null, UrlUtils.unreachableDocOrDatasetUrlIndicator, "Discarded in PageCrawler.retrieveInternalLinks() method, as no " + (isEmpty ? "valid " : "") + "links were able to be retrieved from it. Its contentType is: '" + pageContentType + "'", null, true, "true", "true", "false", "false", "false", null, "null");
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
	 * @throws DocLinkInvalidException
	 * @throws RuntimeException
	 */
	public static HashSet<String> extractInternalLinksFromHtml(String pageHtml, String pageUrl) throws DocLinkFoundException, DynamicInternalLinksFoundException, DocLinkInvalidException, RuntimeException
	{
		Document document = Jsoup.parse(pageHtml);
		Elements elementLinksOnPage = document.select("a, link[href][type*=pdf], form[action]");
		if ( elementLinksOnPage.isEmpty() ) {	// It will surely NOT be null, by Jsoup-documentation.
			//logger.warn("Jsoup did not extract any links from pageUrl: \"" + pageUrl + "\"");	// DEBUG!
			return null;
		}

		HashSet<String> urls = new HashSet<>(elementLinksOnPage.size()/2);	// Only some links will be added in the final set.
		String linkAttr, internalLink;
		int curNumOfInternalLinks = 0;

		// Iterate through the elements and extract the internal link.
		// The internal link might be half-link, or it may have normalization issues. That's ok, it is made whole and normalized later, if needed.

		if ( pageUrl.contains("aup-online.com") ) {
			SpecialUrlsHandler.handleAupOnlinePage(pageUrl, elementLinksOnPage);
			// The above method will always throw an exception, either a "DocLinkFoundException" or a "RuntimeException".
		}

		for ( Element el : elementLinksOnPage )
		{
			if ( hasUnacceptableStructure(el, pageUrl) )
				continue;

			if ( LoaderAndChecker.retrieveDocuments)	// Currently, these smart-checks are only available for specific docFiles (not for datasets).
			{
				// TODO - Somehow I need to detect if a link has the parameter "?isAllowd=n" or "&isAllowd=n".
				// In that case the whole page should be discarded as not having any docUrls!

				// TODO - ALso, each individual link coming in the program, containing the above, should be discarded, so such rules should be added in some regex.

				// Check the text appearing next-to or as the link, inside the html.
				linkAttr = el.text().trim();
				if ( !linkAttr.isEmpty() && checkTextOrTitleAlongWithLink(el, linkAttr) )	// This may throw "DocLinkFoundException" or "DocLinkInvalidException".
					continue;

				linkAttr = el.attr("title").trim();
				if ( !linkAttr.isEmpty() && checkTextOrTitleAlongWithLink(el, linkAttr) )	// This may throw "DocLinkFoundException" or "DocLinkInvalidException".
					continue;

				// Check if we have a "link[href][type*=pdf]" get the docUrl. This also check all the "types" even from the HTML-"a" elements.
				linkAttr = el.attr("type").trim();
				if ( !linkAttr.isEmpty() && ConnSupportUtils.knownDocMimeTypes.contains(linkAttr) ) {
					internalLink = el.attr("href").trim();
					String tempInternalLink;
					if ( internalLink.isEmpty() || internalLink.equals("#")
						|| ( ((tempInternalLink = ConnSupportUtils.getFullyFormedUrl(pageUrl, internalLink, null)) != null)
							&& UrlTypeChecker.shouldNotAcceptInternalLink(tempInternalLink, null) )) {
						//logger.debug("Avoiding invalid full-text with context: \"" + linkAttr + "\", internalLink: " + el.attr("href"));	// DEBUG!
						throw new DocLinkInvalidException(internalLink);
					} else {
						//logger.debug("Found the docLink < " + internalLink + " > from link-type: \"" + linkAttr + "\"");	// DEBUG
						internalLink = StringUtils.replace(internalLink, "/view/", "/download/", 1);	// It may be the case, where the provided PDF-link is the view and not the download-url.
						throw new DocLinkFoundException(internalLink);
					}
				}
			}

			internalLink = el.attr("href").trim();
			if ( internalLink.isEmpty() || internalLink.equals("#") ) {
				// In case there is no "href" attribute inside the "a"-tag, try to extract the rare "data"-like attribute.
				if ( (internalLink = getInternalDataLink(el)) == null ) {
					// Check if we have a "form"-tag, if so, then go and check the "action" attribute.
					internalLink = el.attr("action").trim();
					if ( internalLink.isEmpty() || internalLink.equals("#")	// If this element is not a "form" or just a "#".
						|| !LoaderAndChecker.DOC_URL_FILTER.matcher(internalLink.toLowerCase()).matches() )	// If it's a form without a worthy "action".
						continue;

					String tempInternalLink;
					if ( ((tempInternalLink = ConnSupportUtils.getFullyFormedUrl(pageUrl, internalLink, null)) != null)
							&& UrlTypeChecker.shouldNotAcceptInternalLink(tempInternalLink, null) ) {
						//logger.debug("Avoiding invalid full-text with context: \"" + linkAttr + "\", internalLink: " + el.attr("href"));	// DEBUG!
						throw new DocLinkInvalidException(internalLink);
					} else {
						//logger.debug("Found the docLink < " + internalLink + " > from link-type: \"" + linkAttr + "\"");	// DEBUG
						throw new DocLinkFoundException(internalLink);
					}
				}
			}

			if ( (internalLink = gatherInternalLink(internalLink)) != null ) {	// Throws exceptions which go to the caller method.
				urls.add(internalLink);
				if ( (++curNumOfInternalLinks) > MAX_INTERNAL_LINKS_TO_ACCEPT_PAGE )
					throw new RuntimeException("Avoid checking more than " + MAX_INTERNAL_LINKS_TO_ACCEPT_PAGE + " internal links which were found in pageUrl \"" + pageUrl + "\".");
			}
		}
		return urls;
	}


	private static boolean checkTextOrTitleAlongWithLink(Element el, String linkAttr) throws DocLinkFoundException, DocLinkInvalidException
	{
		String lowerCaseLinkAttr = linkAttr.toLowerCase();
		if ( NON_VALID_DOCUMENT.matcher(lowerCaseLinkAttr).matches() ) {	// If it's not a valid full-text, by checking the TEXT or the TITLE in the html..
			//logger.debug("Avoiding invalid full-text with context: \"" + linkAttr + "\", internalLink: " + el.attr("href"));	// DEBUG!
			return true;	// Avoid collecting it. continue with the next element.
		}
		else if ( DOCUMENT_TEXT.matcher(lowerCaseLinkAttr).matches() ) {
			String internalLink = el.attr("href").trim();
			if ( internalLink.isEmpty() || internalLink.equals("#") )
				if ( (internalLink = getInternalDataLink(el)) == null )
					return true;	// This means that no attributes containing the word "data" was found in this element.

			if ( !UrlTypeChecker.shouldNotAcceptInternalLink(internalLink, null) ) {
				//logger.debug("Found the docLink < " + internalLink + " > from link-text: \"" + linkAttr + "\"");	// DEBUG
				internalLink = StringUtils.replace(internalLink, "/view/", "/download/", 1);	// It may be the case, where the provided PDF-link is the view and not the download-url.
				throw new DocLinkFoundException(internalLink);	// This will be connected and tested by the caller-method.
			}
			throw new DocLinkInvalidException(internalLink);
		}
		else
			return false;
	}


	private static String getInternalDataLink(Element element)
	{
		String internalLink = null;
		List<Attribute> attributes = element.attributes().asList();
		for ( Attribute attribute : attributes ) {
			String name = attribute.getKey();
			if ( name.contains("data") ) {	// For example: "data", "data-popup", "data-article-url". Example-url: https://www.ingentaconnect.com/content/cscript/cvia/2017/00000002/00000003/art00008
				internalLink = attribute.getValue().trim();
				if ( !internalLink.isEmpty() && !internalLink.equals("#") )
					break;	// Upon finding the first real link, the method returns it.
			}
		}
		return internalLink;
	}


	private static final String commonPattern = "website-navigation|reference|su[m]{1,2}ar(?:io|y)|author|logo";

	private static final Pattern PARENT_CLASS_NAME_FILTER_PATTERN = Pattern.compile("(?:^(?:tab|product-head-bnrs)$|.*(?:" + commonPattern + "|breadcrumb|su[b]?scri(?:p[tc]i[oó]n|b(?:a|ir)se)|reco[m]{1,2}enda(?:tion|do)|metric|stats|cookie|kapak|accesos-usuario).*)");
	// Exclude links which are "tabs". For example those hidden in submenus, of the main-menu (not submenus of fulltext-choices).
	// "kapak" = "cover" in Turkish

	private static final Pattern PARENT_ID_FILTER_PATTERN = Pattern.compile(".*(?:" + commonPattern + "|other).*");
	
	private static boolean hasUnacceptableStructure(Element element, String pageUrl)
	{
		// Exclude links which have the class "state-published" and have a different domain, than the pageUrl.
		if ( element.className().trim().equals("state-published") ) {	// The  equality will fail if the className does not exist.
			String internalLink = element.attr("href").trim();
			if ( internalLink.startsWith("http", 0) ) {	// This check covers the case were the "internalLink" may be empty.
				String linkDomain = UrlUtils.getDomainStr(internalLink, null);
				if ( (linkDomain != null) && !pageUrl.contains(linkDomain) )
					return true;
			}
		}

		// Avoid collecting internal-links which are inside the "footer", "header", "article-references" or other sections (we make it more general with the ending-s).
		Element parentElement = element.parent();
		if ( parentElement == null )
			return false;

		// Check for text inside the immediate parent only.
		String parentLowerText = parentElement.text().trim().toLowerCase();
		if ( !parentLowerText.isEmpty() && NON_VALID_DOCUMENT.matcher(parentLowerText).matches() ) {
			//logger.debug("Text of false-positive elements: " + parentLowerText);
			return true;
		}

		// Check all the ancestors.
		do {
			String parentTag = parentElement.tagName().trim();
			if ( parentTag.equals("footer") || parentTag.equals("header")
					|| PARENT_CLASS_NAME_FILTER_PATTERN.matcher(parentElement.className().trim().toLowerCase()).matches()
					|| PARENT_ID_FILTER_PATTERN.matcher(parentElement.id().toLowerCase()).matches() )	// No "trimming" is necessary.
				return true;

			parentElement = parentElement.parent();	// Climb up to the ancestor.
		} while ( parentElement != null );

		return false;
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
			if ( (LoaderAndChecker.retrieveDocuments && LoaderAndChecker.DOC_URL_FILTER.matcher(lowerCaseInternalLink).matches())
					|| (LoaderAndChecker.retrieveDatasets && LoaderAndChecker.DATASET_URL_FILTER.matcher(lowerCaseInternalLink).matches()) ) {
				// There are some docURLs with anchors. We should get the docUrl but remove the anchors to keep them clean and connectable.
				// Like this: https://www.redalyc.org/pdf/104/10401515.pdf#page=1&zoom=auto,-13,792
				internalLink = UrlUtils.removeAnchor(internalLink);
				//logger.debug("Filtered InternalLink: " + internalLink);	// DEBUG!
				return internalLink;
			}
			else if ( !lowerCaseInternalLink.contains("/#/") )
				return null;	// Else if it has not a hashtag-directory we reject it (don't add it in the hashSet)..
		}
		else if ( lowerCaseInternalLink.contains("\"") || lowerCaseInternalLink.contains("[error") )	// They cannot be normalized and especially the second one is not wanted.
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
			UrlUtils.logOutputData(urlId, sourceUrl, null, UrlUtils.unreachableDocOrDatasetUrlIndicator, "Discarded in 'PageCrawler.visit()' method, as there was a problem retrieving its internalLinks. Its contentType is: '" + pageContentType + "'", null, true, "true", "true", "false", "false", "true", null, "null");
			return false;
		}

		// Produce fully functional internal links, NOT internal paths or non-normalized.
		String tempLink = docLink;
		if ( ((docLink = ConnSupportUtils.getFullyFormedUrl(pageUrl, docLink, null)) == null)	// Make it a full-URL.
				|| ((docLink = LoaderAndChecker.basicURLNormalizer.filter(docLink)) == null) ) {	// Normalize it.
			logger.warn("Could not normalize internal url: " + tempLink);
			UrlUtils.logOutputData(urlId, sourceUrl, null, UrlUtils.unreachableDocOrDatasetUrlIndicator, "Discarded in 'PageCrawler.visit()' method, as there were normalization problems with the 'possibleDocUrl' found inside: " + tempLink, null, true, "true", "false", "false", "false", "false", null, "null");
			return false;
		}

		if ( UrlUtils.docOrDatasetUrlsWithIDs.containsKey(docLink) ) {    // If we got into an already-found docUrl, log it and return.
			ConnSupportUtils.handleReCrossedDocUrl(urlId, sourceUrl, pageUrl, docLink, false);
			return true;
		}

		//logger.debug("Going to check DocLink: " + docLink);	// DEBUG!
		try {
			if ( !HttpConnUtils.connectAndCheckMimeType(urlId, sourceUrl, pageUrl, docLink, pageDomain, false, true) ) {    // We log the docUrl inside this method.
				logger.warn("The DocLink < " + docLink + " > was not a docUrl (unexpected)!");
				UrlUtils.logOutputData(urlId, sourceUrl, null, UrlUtils.unreachableDocOrDatasetUrlIndicator, "Discarded in 'PageCrawler.visit()' method, as the retrieved DocLink: < " + docLink + " > was not a docUrl.", null, true, "true", "true", "false", "false", "false", null, "null");
				return false;
			}
			return true;
		} catch (Exception e) {	// After connecting to the metaDocUrl.
			logger.warn("The DocLink < " + docLink + " > was not reached!");
			if (e instanceof RuntimeException)
				ConnSupportUtils.printEmbeddedExceptionMessage(e, docLink);
			UrlUtils.logOutputData(urlId, sourceUrl, null, UrlUtils.unreachableDocOrDatasetUrlIndicator, "Discarded in 'PageCrawler.visit()' method, as the retrieved DocLink: < " + docLink + " > had connectivity problems.", null, true, "true", "true", "false", "false", "false", null, "null");
			return false;
		}
	}


	public static final int timesToCheckInternalLinksBeforeEvaluate = 20;
	public static final AtomicInteger timesCheckedRemainingLinks = new AtomicInteger(0);
	public static final AtomicInteger timesFoundDocOrDatasetUrlFromRemainingLinks = new AtomicInteger(0);
	private static final double leastPercentageOfHitsFromRemainingLinks = 0.20;

	public static boolean checkRemainingInternalLinks(String urlId, String sourceUrl, String pageUrl, String pageDomain, HashSet<String> remainingLinks)
	{
		int temp_timesCheckedRemainingLinks = timesCheckedRemainingLinks.incrementAndGet();
		if ( temp_timesCheckedRemainingLinks >= timesToCheckInternalLinksBeforeEvaluate ) {
			// After this threshold, evaluate the percentage of found docUrls, if it's too low, then stop handling the remaining-links for any pageUrl.
			double percentage = (timesFoundDocOrDatasetUrlFromRemainingLinks.get() * 100.0 / temp_timesCheckedRemainingLinks);
			if ( percentage < leastPercentageOfHitsFromRemainingLinks ) {
				logger.warn("The percentage of found docUrls from the remaining links is too low ( " + percentage + "% ). Stop checking the remaining-internalLinks for any pageUrl..");
				should_check_remaining_links = false;
				handlePageWithNoDocUrls(urlId, sourceUrl, pageUrl, pageDomain, false, false);
				return false;
			}
		}

		int remainingUrlsCounter = 0;

		for ( String currentLink : remainingLinks )    // Here we don't re-check already-checked links, as this is a new list. All the links here are full-normalized-urls.
		{
			// Make sure we avoid connecting to different domains to save time. We allow to check different domains only after matching to possible-urls in the previous fast-loop.
			if ( !currentLink.contains(pageDomain)
				|| UrlUtils.duplicateUrls.contains(currentLink) )
				continue;

			// We re-check here, as, in the fast-loop not all the links are checked against this.
			if ( UrlTypeChecker.shouldNotAcceptInternalLink(currentLink, null) ) {    // If this link matches certain blackListed criteria, move on..
				//logger.debug("Avoided link: " + currentLink );
				UrlUtils.duplicateUrls.add(currentLink);
				continue;
			}

			if ( (++remainingUrlsCounter) > MAX_REMAINING_INTERNAL_LINKS_TO_CONNECT ) {    // The counter is incremented only on "aboutToConnect" links, so no need to pre-clean the "remainingLinks"-set.
				logger.warn("The maximum limit (" + MAX_REMAINING_INTERNAL_LINKS_TO_CONNECT + ") of remaining links to be connected was reached for pageUrl: \"" + pageUrl + "\". The page was discarded.");
				handlePageWithNoDocUrls(urlId, sourceUrl, pageUrl, pageDomain, true, false);
				return false;
			}

			// We have already checked for "already-found-docOrDatasetUrl", in the previous loop in side the "visit" method. So here we just go ahead and connect.

			//logger.debug("InternalLink to connect with: " + currentLink);	// DEBUG!
			try {
				if ( HttpConnUtils.connectAndCheckMimeType(urlId, sourceUrl, pageUrl, currentLink, null, false, false) )    // We log the docUrl inside this method.
				{    // Log this in order to find ways to make these docUrls get found sooner..!
					//logger.debug("Page \"" + pageUrl + "\", gave the \"remaining\" docOrDatasetUrl \"" + currentLink + "\"");    // DEBUG!!
					timesFoundDocOrDatasetUrlFromRemainingLinks.incrementAndGet();
					return true;
				} else
					UrlUtils.duplicateUrls.add(currentLink);
			} catch (DomainBlockedException dbe) {
				String blockedDomain = dbe.getMessage();
				if ( (blockedDomain != null) && blockedDomain.contains(pageDomain) ) {
					logger.warn("Page: \"" + pageUrl + "\" left \"PageCrawler.checkRemainingInternalLinks()\" after it's domain was blocked.");
					String couldRetry = (LoaderAndChecker.COULD_RETRY_URLS.matcher(pageUrl).matches() ? "true" : "false");
					UrlUtils.logOutputData(urlId, sourceUrl, null, UrlUtils.unreachableDocOrDatasetUrlIndicator, "Logged in 'PageCrawler.checkRemainingInternalLinks()' method, as its domain was blocked during crawling.", null, true, "true", "true", "false", "false", couldRetry, null, "null");
					LoaderAndChecker.connProblematicUrls.incrementAndGet();
					return false;
				}
			} catch (DomainWithUnsupportedHEADmethodException dwuhe) {
				if ( currentLink.contains(pageDomain) ) {
					logger.warn("Page: \"" + pageUrl + "\" left \"PageCrawler.checkRemainingInternalLinks()\" after it's domain was caught to not support the HTTP HEAD method, as a result, the internal-links will stop being checked.");
					UrlUtils.logOutputData(urlId, sourceUrl, null, UrlUtils.unreachableDocOrDatasetUrlIndicator, "Logged in 'PageCrawler.checkRemainingInternalLinks()' method, as its domain was caught to not support the HTTP HEAD method.", null, true, "true", "true", "false", "false", "false", null, "null");
					LoaderAndChecker.connProblematicUrls.incrementAndGet();
					return false;
					// This domain is not blocked, because we do not want to lose all the urls of this domain; maybe next time, we get the docUrl itself and not the pageUrl, in that case, the "GET" method will be used.
				}
			} catch (ConnTimeoutException cte) {    // In this case, it's unworthy to stay and check other internalLinks here.
				if ( currentLink.contains(pageDomain) ) {
					logger.warn("Page: \"" + pageUrl + "\" left \"PageCrawler.checkRemainingInternalLinks()\" after an internalLink caused a ConnTimeoutException.");
					UrlUtils.logOutputData(urlId, sourceUrl, null, UrlUtils.unreachableDocOrDatasetUrlIndicator, "Logged in 'PageCrawler.checkRemainingInternalLinks()' method, as an internalLink of this page caused 'ConnTimeoutException'.", null, true, "true", "true", "false", "false", "true", null, "null");
					LoaderAndChecker.connProblematicUrls.incrementAndGet();
					return false;
					// This domain is not blocked, because the "timeout-exception" is usually temporal.
				}
			} catch (RuntimeException e) {
				// No special handling here.. nor logging..
			}
		}// end for-loop

		handlePageWithNoDocUrls(urlId, sourceUrl, pageUrl, pageDomain, false, false);
		return false;
	}

	
	public static void printInternalLinksForDebugging(HashSet<String> currentPageLinks)
	{
		for ( String url : currentPageLinks ) {
			//if ( url.contains(<keyWord> | <url>) )	// In case we want to print only specific-linkTypes.
				logger.debug(url);
		}
	}
	
}
