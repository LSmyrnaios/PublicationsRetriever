package eu.openaire.publications_retriever.crawler;

import eu.openaire.publications_retriever.exceptions.DomainBlockedException;
import eu.openaire.publications_retriever.models.IdUrlMimeTypeTriple;
import eu.openaire.publications_retriever.util.args.ArgsUtils;
import eu.openaire.publications_retriever.util.http.ConnSupportUtils;
import eu.openaire.publications_retriever.util.http.HttpConnUtils;
import eu.openaire.publications_retriever.util.url.LoaderAndChecker;
import eu.openaire.publications_retriever.util.url.UrlTypeChecker;
import eu.openaire.publications_retriever.util.url.UrlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MetadataHandler {

    private static final Logger logger = LoggerFactory.getLogger(MetadataHandler.class);


    // Order-independent META_RESTRICTED_ACCESS_RIGHTS-regex.
    // <meta(?:[^<]*name=\"DC.AccessRights\"[^<]*content=\"restricted\"|[^<]*content=\"restricted\"[^<]*name=\"DC.AccessRights\")[^>]*[/]?>
    private static final String metaAccessName = "name=\"DC.(?:Access)?Rights\"";
    private static final String metaAccessContent = "content=\"([^\"]+)\"";
    // It may be "restricted", "info:eu-repo/semantics/(openAccess|closedAccess|embargoedAccess|restrictedAccess)", "Open Access", etc..
    public static final Pattern META_RESTRICTED_ACCESS_RIGHTS = Pattern.compile("<(?:(?i)meta)(?:[^<]*" + metaAccessName + "[^<]*" + metaAccessContent + "|[^<]*" + metaAccessContent + "[^<]*" + metaAccessName + ")[^>]*[/]?>", Pattern.CASE_INSENSITIVE);

    public static final Pattern NO_ACCESS_RIGHTS = Pattern.compile(".*(?:(close[d]?|embargo(?:ed)?|restrict(?:ed)?|metadata" + PageCrawler.spaceOrDashes + "only|paid)(?:" + PageCrawler.spaceOrDashes + "access)?|(?:no[t]?|není)" + PageCrawler.spaceOrDashes + "(?:accessible|přístupná)"
            + "|inaccessible|(?:acceso" + PageCrawler.spaceOrDashes + ")?cerrado).*");
    // We want the "capturing-group" to know which is the case for each url and write it in the logs. The text given to this regex is made lowercase.
    // "není přístupná" = "not accessible" in Czech


    // Order-independent META_DOC_URL-regex.
    // <meta(?:[^<]*name=\"(?:[^<]*(?:citation|wkhealth)_pdf|eprints.document)_url\"[^<]*content=\"(http[^\"]+)\"|[^<]*content=\"(http[^\"]+)\"[^<]*name=\"(?:[^<]*(?:citation|wkhealth)_pdf|eprints.document)_url\")[^>]*[/]?>
    private static final String metaName = "name=\"(?:[^<]*(?:(?:citation|wkhealth)(?:_fulltext)?_)?pdf|eprints.document)_url\"";
    private static final String metaContent = "content=\"(http[^\"]+)\"";
    public static final Pattern META_DOC_URL = Pattern.compile("<meta(?:[^<]*" + metaName + "[^<]*" + metaContent + "|[^<]*" + metaContent + "[^<]*" + metaName + ")[^>]*[/]?>", Pattern.CASE_INSENSITIVE);

    public static Pattern COMMON_UNSUPPORTED_META_DOC_OR_DATASET_URL_EXTENSIONS;    // Its pattern gets compiled at runtime, only one time, depending on the Datatype.
    static {
        // Depending on the datatype, the regex is formed differently.
        String regex = ".+\\.(?:";

        if ( !ArgsUtils.retrieveDatasets )
            regex += LoaderAndChecker.dataset_formats;  // If no datasets retrieved, block these types.
        else if ( !ArgsUtils.retrieveDocuments )
            regex += "pdf|" + UrlTypeChecker.unsupportedDocFileTypes;  // If no documents retrieved, block these types.
        //else -> no more datatype-dependent additions

        regex += "|apk|jpg|png)(?:\\?.+)?$";
        logger.debug("COMMON_UNSUPPORTED_META_DOC_OR_DATASET_URL_EXTENSIONS -> REGEX: " + regex);
        COMMON_UNSUPPORTED_META_DOC_OR_DATASET_URL_EXTENSIONS = Pattern.compile(regex);
    }


    public static Pattern LOCALHOST_DOMAIN_REPLACEMENT_PATTERN = Pattern.compile("://(?:localhost|127.0.0.1)(?:\\:\\d+)?");

    public static AtomicInteger numOfProhibitedAccessPagesFound = new AtomicInteger(0);
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
    public static boolean checkAndHandleMetadata(String urlId, String sourceUrl, String pageUrl, String pageDomain, String pageHtml)
    {
        // Before checking for the MetaDocUrl, check whether this publication is restricted or not. It may have a metaDocUrl, but it will redirect to the landing page.
        // e.g.: https://le.uwpress.org/content/78/2/260

        // Some websites use upper of mixed-case meta tags, names or contents or even the values.
        // We cannot make the HTML lower-case, or we will make the metaDocUrls invalid.
        // So the REGEXes have to be case-insensitive..!

        String metaAccessRights;
        if ( (metaAccessRights = getMetaAccessRightsFromHTML(pageHtml)) == null ) { // This is mostly the case when the page does not include any info about "access rights". It may or may not provide access to the docUrl.
            if ( logger.isTraceEnabled() )
                logger.trace("Could not retrieve the metaAccessRights for url \"" + pageUrl + "\", continue by checking the metaDocUrl..");
        } else if ( logger.isTraceEnabled() )
            logger.trace("metaAccessRights: " + metaAccessRights);  // DEBUG!

        if ( metaAccessRights != null ) {
            String lowercaseMetaAccessRights = metaAccessRights.toLowerCase();
            Matcher noAccessRightsMatcher = NO_ACCESS_RIGHTS.matcher(lowercaseMetaAccessRights);
            if ( noAccessRightsMatcher.matches() ) {
                String noAccessCase = noAccessRightsMatcher.group(1);
                if ( (noAccessCase == null) || noAccessCase.isEmpty() )
                    noAccessCase = "prohibited";    // This is not an "official" status, but a good description of the situation and makes it clear that there was a minor problem when determining the exact reason.
                logger.debug("The metaAccessRights were found to be \"" + noAccessCase + "\"! Do not check the metaDocUrl, nor crawl the page!");
                UrlUtils.addOutputData(urlId, sourceUrl, pageUrl, UrlUtils.unreachableDocOrDatasetUrlIndicator, "Discarded in 'MetaDocUrlsHandler.checkIfAndHandleMetaDocUrl()' method, as its accessRight were '" + noAccessCase + "'.", "null", null, true, "true", "true", "false", "false", "true", null, "null", "null");
                numOfProhibitedAccessPagesFound.incrementAndGet();
                return true;   // This publication has "restricted" metaAccessRights, so it will not be handled in another way. Although, it may be rechecked in the future.
            }
        }

        if ( !ArgsUtils.retrieveDocuments )
            return false;    // There was no definitive handling, nor we want to check for metaDocUrl, go get the internal-links.

        // Check if the docLink is provided in a metaTag and connect to it directly.
        String metaDocUrl = null;
        if ( (metaDocUrl = getMetaDocUrlFromHTML(pageHtml)) == null ) { // This is mostly the case when the page does not have a docUrl, although not always, so we continue crawling it.
            if ( logger.isTraceEnabled() )
                logger.trace("Could not retrieve the metaDocUrl, continue by crawling the page..");
            return false;   // We don't log the sourceUrl, since it will be handled later.
        } else if ( logger.isTraceEnabled() )
            logger.trace("MetaDocUrl: " + metaDocUrl);  // DEBUG!

        if ( metaDocUrl.equals(pageUrl) || ConnSupportUtils.haveOnlyProtocolDifference(pageUrl, metaDocUrl) || ConnSupportUtils.isJustASlashRedirect(pageUrl, metaDocUrl) ) {
            logger.debug("The metaDocUrl was found to be the same as the pageUrl! Continue by crawling the page..");
            return false;   // This metaDocUrl cannot be handled, return to "PageCrawler.visit()" to continue.
        }

        if ( metaDocUrl.contains("{{") || metaDocUrl.contains("<?") )   // Dynamic link! The only way to handle it is by blocking the "currentPageUrlDomain".
        {
            if ( logger.isTraceEnabled() )
                logger.trace("The metaDocUrl is a dynamic-link. Abort the process and block the domain of the pageUrl.");
            // Block the domain and return "true" to indicate handled-state.
            HttpConnUtils.blacklistedDomains.add(pageDomain);
            logger.warn("Domain: \"" + pageDomain + "\" was blocked, after giving a dynamic metaDocUrl: " + metaDocUrl);
            UrlUtils.addOutputData(urlId, sourceUrl, pageUrl, UrlUtils.unreachableDocOrDatasetUrlIndicator, "Discarded in 'MetaDocUrlsHandler.checkIfAndHandleMetaDocUrl()' method, as its metaDocUrl was a dynamic-link.", "null", null, true, "true", "true", "false", "false", "false", null, "null", "null");
            PageCrawler.contentProblematicUrls.incrementAndGet();
            return true;    // Since the domain is blocked, there is no point in continuing to crawl.
        }

        String lowerCaseMetaDocUrl = metaDocUrl.toLowerCase();

        if ( (ArgsUtils.shouldDownloadDocFiles && UrlTypeChecker.CURRENTLY_UNSUPPORTED_DOC_EXTENSION_FILTER.matcher(lowerCaseMetaDocUrl).matches())
            || UrlTypeChecker.PLAIN_PAGE_EXTENSION_FILTER.matcher(lowerCaseMetaDocUrl).matches()
            || UrlTypeChecker.URL_DIRECTORY_FILTER.matcher(lowerCaseMetaDocUrl).matches()
            || COMMON_UNSUPPORTED_META_DOC_OR_DATASET_URL_EXTENSIONS.matcher(lowerCaseMetaDocUrl).matches() )   // These do not lead us to avoid crawling the page, since the metaDocUrl may be an image, but the page may also include a full-text inside.
        {
            logger.warn("The retrieved metaDocUrl ( " + metaDocUrl + " ) is pointing to an unsupported file, continue by crawling the page..");
            //UrlUtils.duplicateUrls.add(metaDocUrl);   //  TODO - Would this make sense?
            return false;   // Continue crawling the page.. The page may give the correct file inside, like this one: https://scholarlypublications.universiteitleiden.nl/handle/1887/66271
        }

        // In case we are certain we do not have an "unsupported-file" as a metaDocUrl (which would lead us to crawl the page)..
        // Check if we have a false-positive full-text file. In this case we can avoid crawling the page.
        // Since, it is almost certain that whatever full-text we retrieve from crawling, will be a false-positive as well.
        if ( PageCrawler.NON_VALID_DOCUMENT.matcher(lowerCaseMetaDocUrl).matches() ) {
            logger.warn("The retrieved metaDocUrl ( " + metaDocUrl + " ) is pointing to a false-positive full-text file, avoid crawling the page..!");
            /*Matcher matcher = PageCrawler.NON_VALID_DOCUMENT.matcher(lowerCaseMetaDocUrl);
            if ( matcher.matches() )    // TODO - In case we use this test, change temporarily th regex to use "capturing" groups.
                logger.warn("reason: " + matcher.group(1));*/
            //UrlUtils.duplicateUrls.add(metaDocUrl);   //  TODO - Would this make sense?
            UrlUtils.addOutputData(urlId, sourceUrl, pageUrl, UrlUtils.unreachableDocOrDatasetUrlIndicator, "Discarded in 'MetaDocUrlsHandler.checkIfAndHandleMetaDocUrl()' method, as its metaDocUrl is pointing to a false-positive full-text file.", "null", null, true, "true", "true", "false", "false", "false", null, "null", "null");
            return true;    // This pageUrl was handled. Nothing more can be done.
        }

        // Canonnicalize the metaDocUrl before connecting with it, to avoid encoding problems. We assume the metaDocUrl to be a full-url (including the protocol, domain etc.)
        String tempMetaDocUrl = metaDocUrl;
        if ( ((metaDocUrl = ConnSupportUtils.getFullyFormedUrl(pageUrl, metaDocUrl, null)) == null)	// Make it a full-URL.
                || ((metaDocUrl = LoaderAndChecker.basicURLNormalizer.filter(metaDocUrl)) == null) ) {	// Normalize it.
            logger.warn("Could not normalize metaDocUrl: " + tempMetaDocUrl + " , continue by crawling the page..");
            //UrlUtils.duplicateUrls.add(metaDocUrl);   //  TODO - Would this make sense?
            return false;   // Continue crawling the page..
        }

        // Sometimes, the metaDocUrl contains the "localhost" instead of the page' domain. So we have to search and replace.
        // For example: http://localhost:4000/bitstreams/98e649e7-a656-4a90-ad69-534178e63fbb/download
        metaDocUrl = LOCALHOST_DOMAIN_REPLACEMENT_PATTERN.matcher(metaDocUrl).replaceFirst("://" +  pageDomain);

        IdUrlMimeTypeTriple originalIdUrlMimeTypeTriple = UrlUtils.resultUrlsWithIDs.get(metaDocUrl);
        if ( originalIdUrlMimeTypeTriple != null ) {    // If we got into an already-found docUrl, log it and return.
            ConnSupportUtils.handleReCrossedTargetUrl(urlId, sourceUrl, pageUrl, metaDocUrl, originalIdUrlMimeTypeTriple, false);
            numOfMetaDocUrlsFound.incrementAndGet();
            return true;
        }

        // Connect to it directly.
        try {
            if ( HttpConnUtils.connectAndCheckMimeType(urlId, sourceUrl, pageUrl, metaDocUrl, pageDomain, false, true) ) {    // On success, we log the docUrl inside this method.
                numOfMetaDocUrlsFound.incrementAndGet();
                return true;    // It should be the docUrl, and it was handled.. so we don't continue checking the internalLink even if this wasn't an actual docUrl.
            }
            logger.warn("The retrieved metaDocUrl was NOT a docUrl (unexpected): " + metaDocUrl + " , continue by crawling the page..");
            //UrlUtils.duplicateUrls.add(metaDocUrl);   //  TODO - Would this make sense?
            return false;   // Continue crawling the page..
        } catch (DomainBlockedException dbe) {
            String metaDocUrlDomain = UrlUtils.getDomainStr(metaDocUrl, null);
            if ( (metaDocUrlDomain != null) && metaDocUrlDomain.equals(pageDomain) ) {
                UrlUtils.addOutputData(urlId, sourceUrl, pageUrl, UrlUtils.unreachableDocOrDatasetUrlIndicator, "Discarded in 'MetaDocUrlsHandler.checkIfAndHandleMetaDocUrl()' method, as its domain was blocked.", "null", null, true, "true", "true", "false", "false", "false", null, "null", "null");
                return true;    // Stop crawling the page.
            }
            return false;   // Continue crawling the page.
            // The metaDocUrlDomain may be inside a subdomain which has the problems. The page being in the main domain should not be excluded from crawling if the subdomain gets blocked.
            // If the domain is the same, and it's blocked, then stop crawling it. It's very rare that a page will get its domain blocked but will provide docUrl in another domain.
        } catch (Exception e) {
            if ( e instanceof RuntimeException ) {
                String exceptionMessage = e.getMessage();
                if ( (exceptionMessage != null) && (exceptionMessage.contains("HTTP 401") || exceptionMessage.contains("HTTP 403")) ) {
                    logger.warn("The MetaDocUrl < " + metaDocUrl + " > had authorization issues, so further crawling of this page is aborted.");
                    UrlUtils.addOutputData(urlId, sourceUrl, pageUrl, UrlUtils.unreachableDocOrDatasetUrlIndicator, "Discarded in 'MetaDocUrlsHandler.checkIfAndHandleMetaDocUrl()' method, as its metaDocUrl had authorization issues.", "null", null, true, "true", "true", "false", "false", "false", null, "null", "null");
                    return true;    // It was handled, avoid crawling the page.
                }
            }
            logger.warn("The MetaDocUrl < " + metaDocUrl + " > had connectivity or redirection problems! Continue by crawling the page..");
            return false;   // Continue crawling the page..
        }
    }


    /**
     * Scan the HTML-code for the metaDocUrl.
     * It may be located either in the start or at the end of the HTML.
     * An HTML-Code may be only a few VERY-LONG lines of code, instead of hundreds of "normal-sized" lines.
     * */
    public static String getMetaAccessRightsFromHTML(String pageHtml)
    {
        Matcher metaAccessRightsMatcher = META_RESTRICTED_ACCESS_RIGHTS.matcher(pageHtml);

        // There may be multiple meta-tags about "access rights". Find them all and concatenate them to check them later.

        final StringBuilder stringBuilder = new StringBuilder(500);

        while ( metaAccessRightsMatcher.find() )
        {
            String currentMetaAccessRights = null;
            //logger.debug("Matched metaAccessRights-line: " + metaAccessRightsMatcher.group(0));	// DEBUG!!
            try {
                currentMetaAccessRights = metaAccessRightsMatcher.group(1);
            } catch ( Exception e ) { logger.error("", e); }
            if ( currentMetaAccessRights == null ) {
                try {
                    currentMetaAccessRights = metaAccessRightsMatcher.group(2);    // Try the other group.
                } catch ( Exception e ) { logger.error("", e); }
            }
            if ( (currentMetaAccessRights != null)
                    && !currentMetaAccessRights.startsWith("http") && (currentMetaAccessRights.length() <= 200) )
                stringBuilder.append(currentMetaAccessRights).append(" -- ");
        }

        if ( stringBuilder.length() == 0 )
            return null;    // It was not found and so it was not handled. We don't log the sourceUrl, since it will be handled later.
        else
            return stringBuilder.toString();
    }


    /**
     * Scan the HTML-code for the metaDocUrl.
     * It may be located either in the start or at the end of the HTML.
     * An HTML-Code may be only a few VERY-LONG lines of code, instead of hundreds of "normal-sized" lines.
     * */
    public static String getMetaDocUrlFromHTML(String pageHtml)
    {
        Matcher metaDocUrlMatcher = META_DOC_URL.matcher(pageHtml);
        if ( !metaDocUrlMatcher.find() )
            return null;    // It was not found and so it was not handled. We don't log the sourceUrl, since it will be handled later.

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
        // It does not need "trimming".
    }
}
