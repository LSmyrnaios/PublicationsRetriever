package eu.openaire.publications_retriever.crawler;

import edu.uci.ics.crawler4j.url.URLCanonicalizer;
import eu.openaire.publications_retriever.util.http.ConnSupportUtils;
import eu.openaire.publications_retriever.util.http.HttpConnUtils;
import eu.openaire.publications_retriever.util.url.LoaderAndChecker;
import eu.openaire.publications_retriever.util.url.UrlTypeChecker;
import eu.openaire.publications_retriever.util.url.UrlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MetaDocUrlsHandler {

    private static final Logger logger = LoggerFactory.getLogger(MetaDocUrlsHandler.class);

    // Order-independent META_DOC_URL-regex.
    // <meta(?:[^<]*name=\"(?:[^<]*citation_pdf|eprints.document)_url\"[^<]*content=\"(http[^\"]+)\"|[^<]*content=\"(http[^\"]+)\"[^<]*name=\"(?:[^<]*citation_pdf|eprints.document)_url\")[^>]*[/]?>
    private static final String metaName = "name=\"(?:[^<]*citation_pdf|eprints.document)_url\"";
    private static final String metaContent = "content=\"(http[^\"]+)\"";
    public static final Pattern META_DOC_URL = Pattern.compile("<meta(?:[^<]*" + metaName + "[^<]*" + metaContent + "|[^<]*" + metaContent + "[^<]*" + metaName + ")[^>]*[/]?>");

    public static Pattern COMMON_UNSUPPORTED_META_DOC_OR_DATASET_URL_EXTENSIONS;    // Its pattern gets compiled at runtime, only one time, depending on the Datatype.
    static {
        // Depending on the datatype, the pattern is formed differently.
        String pattern = "\".+\\.(?:";

        if ( !LoaderAndChecker.retrieveDatasets )
            pattern += "zip|rar|";  // If no datasets retrieved, block these types.
        else if ( !LoaderAndChecker.retrieveDocuments )
            pattern += "pdf|doc[x]?|";  // If no documents retrieved, block these types.
        //else -> no more datatype-dependent additions

        pattern += "apk|jpg)(?:\\?.+)?$";
        logger.debug("MetaDocUrlsHandler -> Pattern: " + pattern);
        COMMON_UNSUPPORTED_META_DOC_OR_DATASET_URL_EXTENSIONS = Pattern.compile(pattern);
    }

    public static AtomicInteger numOfMetaDocUrlsFound = new AtomicInteger(0);


    /**
     * This method takes in the "pageHtml" of an already-connected url and checks if there is a metaDocUrl inside.
     * If such url exist, then it connects to it and checks if it's really a docUrl, and it may also download the full-text-document, if wanted.
     * It returns "true" when the metaDocUrl was found and handled (independently of how it was handled),
     * otherwise, if the metaDocUrl was not-found, it returns "false".
     * @param urlId
     * @param sourceUrl
     * @param pageUrl
     * @param pageDomain
     * @param pageHtml
     * @return
     */
    public static boolean checkIfAndHandleMetaDocUrl(String urlId, String sourceUrl, String pageUrl, String pageDomain, String pageHtml)
    {
        // Check if the docLink is provided in a metaTag and connect to it directly.
        String metaDocUrl = null;
        Matcher metaDocUrlMatcher = META_DOC_URL.matcher(pageHtml);
        if ( !metaDocUrlMatcher.find() )
            return false;    // It was not found and so it was not handled. We don't log the sourceUrl, since it will be handled later.

        if ( (metaDocUrl = getMetaDocUrlFromMatcher(metaDocUrlMatcher)) == null ) {
            logger.error("Could not retrieve the metaDocUrl, continue by crawling the pageUrl.");
            return false;   // We don't log the sourceUrl, since it will be handled later.
        }
        //logger.debug("MetaDocUrl: " + metaDocUrl);  // DEBUG!

        if ( metaDocUrl.equals(pageUrl) || ConnSupportUtils.haveOnlyProtocolDifference(metaDocUrl, pageUrl) ) {
            logger.warn("The metaDocUrl was found to be the same as the pageUrl! Continue by crawling the page..");
            return false;   // This metaDocUrl cannot be handled, return to "PageCrawler.visit()" to continue.
        }

        if ( metaDocUrl.contains("{{") || metaDocUrl.contains("<?") )   // Dynamic link! The only way to handle it is by blocking the "currentPageUrlDomain".
        {
            logger.debug("The metaDocUrl is a dynamic-link. Abort the process and block the domain of the pageUrl.");
            // Block the domain and return "true" to indicate handled-state.
            HttpConnUtils.blacklistedDomains.add(pageDomain);
            logger.debug("Domain: \"" + pageDomain + "\" was blocked, after giving a dynamic metaDocUrl: " + metaDocUrl);
            UrlUtils.logOutputData(urlId, sourceUrl, null, UrlUtils.unreachableDocOrDatasetUrlIndicator, "Discarded in 'PageCrawler.visit()' method, as its metaDocUrl was a dynamic-link.", null, true, "true", "true", "false", "false", "false", null, "null");  // We log the source-url, and that was discarded in "PageCrawler.visit()".
            PageCrawler.contentProblematicUrls.incrementAndGet();
            return true;
        }

        String lowerCaseMetaDocUrl = metaDocUrl.toLowerCase();
        boolean hasUnsupportedDocExtension = UrlTypeChecker.CURRENTLY_UNSUPPORTED_DOC_EXTENSION_FILTER.matcher(lowerCaseMetaDocUrl).matches();

        if ( hasUnsupportedDocExtension
            || UrlTypeChecker.PLAIN_PAGE_EXTENSION_FILTER.matcher(lowerCaseMetaDocUrl).matches()
            || UrlTypeChecker.URL_DIRECTORY_FILTER.matcher(lowerCaseMetaDocUrl).matches()
            || COMMON_UNSUPPORTED_META_DOC_OR_DATASET_URL_EXTENSIONS.matcher(lowerCaseMetaDocUrl).matches()
            || PageCrawler.NON_VALID_DOCUMENT.matcher(lowerCaseMetaDocUrl).matches() )
        {
            logger.debug("The retrieved metaDocUrl ( " + metaDocUrl + " ) is pointing to an unsupported file.");
            UrlUtils.logOutputData(urlId, sourceUrl, null, UrlUtils.unreachableDocOrDatasetUrlIndicator, "Discarded in 'PageCrawler.visit()' method, as its metaDocUrl was unsupported.", null, true, "true", "true", "false", "false", (hasUnsupportedDocExtension ? "true" : "false"), null, "null");  // We log the source-url, and that was discarded in "PageCrawler.visit()".
            PageCrawler.contentProblematicUrls.incrementAndGet();
            //UrlUtils.duplicateUrls.add(metaDocUrl);   //  TODO - Would this make sense?
            return true;    // It was found and handled. Do not continue crawling as we won't find any docUrl..
        }

        String tempMetaDocUrl = metaDocUrl;
        if ( (metaDocUrl = URLCanonicalizer.getCanonicalURL(metaDocUrl, null, StandardCharsets.UTF_8)) == null ) {
            logger.warn("Could not canonicalize metaDocUrl: " + tempMetaDocUrl);
            UrlUtils.logOutputData(urlId, sourceUrl, null, UrlUtils.unreachableDocOrDatasetUrlIndicator, "Discarded in 'checkIfAndHandleMetaDocUrl()', due to canonicalization's problems.", null, true, "true", "false", "false", "false", "false", null, "null");
            PageCrawler.contentProblematicUrls.incrementAndGet();
            //UrlUtils.duplicateUrls.add(metaDocUrl);   //  TODO - Would this make sense?
            return true;
        }

        if ( UrlUtils.docOrDatasetUrlsWithIDs.containsKey(metaDocUrl) ) {    // If we got into an already-found docUrl, log it and return.
            ConnSupportUtils.handleReCrossedDocUrl(urlId, sourceUrl, pageUrl, metaDocUrl, false);
            numOfMetaDocUrlsFound.incrementAndGet();
            return true;
        }

        // Connect to it directly.
        try {
            if ( HttpConnUtils.connectAndCheckMimeType(urlId, sourceUrl, pageUrl, metaDocUrl, pageDomain, false, true) ) {    // On success, we log the docUrl inside this method.
                numOfMetaDocUrlsFound.incrementAndGet();
                return true;    // It should be the docUrl, and it was handled.. so we don't continue checking the internalLink even if this wasn't an actual docUrl.
            }
            logger.warn("The retrieved metaDocUrl was not a docUrl (unexpected): " + metaDocUrl);
            //UrlUtils.duplicateUrls.add(metaDocUrl);   //  TODO - Would this make sense?
            return false;   // Continue crawling the page..
        } catch (Exception e) {
            logger.debug("The MetaDocUrl < " + metaDocUrl + " > had connectivity problems!");
            return false;   // Continue crawling the page..
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
        } catch ( Exception e ) { logger.error("", e); }
        if ( metaDocUrl == null ) {
            try {
                metaDocUrl = metaDocUrlMatcher.group(2);    // Try the other group.
            } catch ( Exception e ) { logger.error("", e); }
        }

        //logger.debug("MetaDocUrl: " + metaDocUrl);	// DEBUG!

        return metaDocUrl;	// IT MAY BE NULL.. Handling happens in the caller method.
    }
}
