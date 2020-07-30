package eu.openaire.doc_urls_retriever.crawler;

import edu.uci.ics.crawler4j.url.URLCanonicalizer;
import eu.openaire.doc_urls_retriever.exceptions.ConnTimeoutException;
import eu.openaire.doc_urls_retriever.exceptions.DomainBlockedException;
import eu.openaire.doc_urls_retriever.exceptions.DomainWithUnsupportedHEADmethodException;
import eu.openaire.doc_urls_retriever.util.http.ConnSupportUtils;
import eu.openaire.doc_urls_retriever.util.http.HttpConnUtils;
import eu.openaire.doc_urls_retriever.util.url.UrlTypeChecker;
import eu.openaire.doc_urls_retriever.util.url.UrlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MetaDocUrlsHandler {

    private static final Logger logger = LoggerFactory.getLogger(MetaDocUrlsHandler.class);

    // Order-independent META_DOC_URL-regex.
    // (?:<meta(?:[\\s]*name=\"(?:citation_pdf_url|eprints.document_url)\"[\\s]*content=\"(http[\\w/.,\\-_%&;:~()\\[\\]?=]+)(?:\"))|(?:[\\s]*content=\"(http[\\w/.,\\-_%&;:~()\\[\\]?=]+)(?:\")[\\s]*name=\"(?:citation_pdf_url|eprints.document_url)\"))(?:[\\s]*(?:/)?>)
    private static final String metaName = "name=\"(?:citation_pdf_url|eprints.document_url)\"";
    private static final String metaContent = "content=\"(http[\\w/.,\\-_%&;:~()\\[\\]?=]+)\"";
    public static final Pattern META_DOC_URL = Pattern.compile("(?:<meta(?:[\\s]*" + metaName + "[\\s]*" + metaContent + ")|(?:[\\s]*" + metaContent + "[\\s]*" + metaName + ")(?:[\\s]*(?:/)?>))");

    public static final Pattern COMMON_UNSUPPORTED_META_DOC_URL_EXTENSIONS = Pattern.compile("\".+\\.(?:zip|rar|apk|jpg)(?:\\?.+)?$");

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
        String metaDocUrl = null;
        try {
            Matcher metaDocUrlMatcher = META_DOC_URL.matcher(pageHtml);
            if ( !metaDocUrlMatcher.find() )
                return false;    // It was not found and so it was not handled. We don't log the sourceUrl, since it will be handled later.

            if ( (metaDocUrl = getMetaDocUrlFromMatcher(metaDocUrlMatcher)) == null ) {
                logger.error("Could not retrieve the metaDocUrl, continue by crawling the pageUrl.");
                return false;   // We don't log the sourceUrl, since it will be handled later.
            }
            //logger.debug("MetaDocUrl: " + metaDocUrl);  // DEBUG!

            if ( metaDocUrl.contains("{{") || metaDocUrl.contains("<?") )   // Dynamic link! The only way to handle it is by blocking the "currentPageUrlDomain".
            {
                logger.debug("The metaDocUrl is a dynamic-link. Abort the process nd block the domain of the pageUrl.");
                // Block the domain and return "true" to indicate handled-state.
                HttpConnUtils.blacklistedDomains.add(currentPageDomain);
                UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Discarded in 'PageCrawler.visit()' method, as its metaDocUrl was a dynamic-link.", null);  // We log the source-url, and that was discarded in "PageCrawler.visit()".
                return true;
            }

            String lowerCaseMetaDocUrl = metaDocUrl.toLowerCase();

            if ( UrlTypeChecker.CURRENTLY_UNSUPPORTED_DOC_EXTENSION_FILTER.matcher(lowerCaseMetaDocUrl).matches()
                || UrlTypeChecker.PLAIN_PAGE_EXTENSION_FILTER.matcher(lowerCaseMetaDocUrl).matches()
                || COMMON_UNSUPPORTED_META_DOC_URL_EXTENSIONS.matcher(lowerCaseMetaDocUrl).matches() )
            {
                logger.debug("The retrieved metaDocUrl ( " + metaDocUrl + " ) is pointing to an unsupported file.");
                UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Discarded in 'PageCrawler.visit()' method, as its metaDocUrl was unsupported.", null);  // We log the source-url, and that was discarded in "PageCrawler.visit()".
                return true;    // It was found and handled.
            }

            if ( (metaDocUrl = URLCanonicalizer.getCanonicalURL(metaDocUrl, null, StandardCharsets.UTF_8)) == null ) {
                logger.warn("Could not cannonicalize metaDocUrl: " + metaDocUrl);
                UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Discarded in \"checkIfAndHandleMetaDocUrl()\", due to cannibalization's problems.", null);
                return true;
            }

            // Connect to it directly.
            if ( !HttpConnUtils.connectAndCheckMimeType(urlId, sourceUrl, pageUrl, metaDocUrl, currentPageDomain, false, true) ) {    // On success, we log the docUrl inside this method.
                logger.warn("The retrieved metaDocUrl was not a docUrl (unexpected): " + metaDocUrl);
                UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Discarded in 'MetaDocUrlsHandler.visit()' method, as the retrieved metaDocUrl was not a docUrl.", null);
            }
            return true;    // It should be the docUrl and it was handled.. so we don't continue checking the internalLink even if this wasn't a docUrl.

        } catch (RuntimeException re) {
            ConnSupportUtils.printEmbeddedExceptionMessage(re, metaDocUrl);
            UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Discarded in 'PageCrawler.visit()' method, as there was a problem with the metaTag-url.", null);  // We log the source-url, and that was discarded in "PageCrawler.visit()".
            return true;
        } catch ( DomainBlockedException | ConnTimeoutException | DomainWithUnsupportedHEADmethodException ex ) {
            UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Discarded in 'PageCrawler.visit()' method, as there was a problem with the metaTag-url.", null);  // We log the source-url, and that was discarded in "PageCrawler.visit()".
            return true;	// It was found and handled. Even if an exception was thrown, we don't want to check any other internalLinks in that page.
        } catch (Exception e) {	// After connecting to the metaDocUrl.
            logger.warn("", e);
            UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Discarded in 'PageCrawler.visit()' method, as there was a problem with the metaTag-url.", null);  // We log the source-url, and that was discarded in "PageCrawler.visit()".
            return true;	// It was found and handled. Even if an exception was thrown, we don't want to check any other internalLinks in that page.
        }
    }


    public static String getMetaDocUrlFromMatcher(Matcher metaDocUrlMatcher)
    {
        if ( metaDocUrlMatcher == null ) {
            logger.error("\"PageCrawler.getMetaDocUrlMatcher()\" received a \"null\" matcher!");
            return null;
        }

        //logger.debug("Matched meta-doc-url-line: " + metaDocUrlMatcher.group(0));	// DEBUG!!

        String metaDocUrl = null;
        try {
            metaDocUrl = metaDocUrlMatcher.group(1);
        } catch (Exception e) { logger.error("", e); }
        if ( metaDocUrl == null )
            metaDocUrl = metaDocUrlMatcher.group(2);	// Try the other group.

        //logger.debug("MetaDocUrl: " + metaDocUrl);	// DEBUG!

        return metaDocUrl;	// IT MAY BE NULL.. Handling happens in the caller method.
    }
}
