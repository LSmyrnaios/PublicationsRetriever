package eu.openaire.publications_retriever.util.url;

import eu.openaire.publications_retriever.crawler.PageCrawler;
import eu.openaire.publications_retriever.util.file.FileUtils;
import eu.openaire.publications_retriever.util.http.ConnSupportUtils;
import eu.openaire.publications_retriever.util.http.HttpConnUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;

public class GenericUtils {

    private static final Logger logger = LoggerFactory.getLogger(GenericUtils.class);


    public static boolean checkInternetConnectivity()
    {
        try {
            new URL("https://www.google.com/").openConnection().connect();
            logger.info("The internet connection is successful.");
            return true;
        } catch (Exception e) {
            logger.error("The internet connection has failed!", e);
            return false;
        }
    }


    /**
     * This method clears all domain tracking data from the HashSets and HashMaps.
     * It can be used to allow both reduced memory consumption and a second chance for some domains after a very long time.
     * For example, after a month, a domain might be more responsive, and it should not be blocked anymore.
     * It empties all those data-structures without de-allocating the existing space.
     * This guarantees than the memory-space will not get infinitely large, while avoiding re-allocation of the memory for the next id-url pairs to be handled.
     */
    public static void clearDomainAndPathTrackingData()
    {
        // Domain data, related to domain-blocking.
        HttpConnUtils.blacklistedDomains.clear();
        HttpConnUtils.timesDomainsHadInputNotBeingDocNorPage.clear();
        HttpConnUtils.timesDomainsReturnedNoType.clear();
        ConnSupportUtils.timesDomainsReturned5XX.clear();
        ConnSupportUtils.timesDomainsHadTimeoutEx.clear();
        PageCrawler.timesDomainNotGivingInternalLinks.clear();
        PageCrawler.timesDomainNotGivingDocUrls.clear();
        UrlUtils.docOrDatasetUrlsWithIDs.clear();
        UrlUtils.domainsAndHits.clear();

        // Domain additional data, which does not contribute in blocking the domains, but they do contribute in performance.
        HttpConnUtils.domainsSupportingHTTPS.clear();
        HttpConnUtils.domainsWithSlashRedirect.clear();
        HttpConnUtils.domainsWithUnsupportedHeadMethod.clear();
        HttpConnUtils.domainsWithUnsupportedAcceptLanguageParameter.clear();

        // Paths' data.
        ConnSupportUtils.timesPathsReturned403.clear();
        ConnSupportUtils.domainsMultimapWithPaths403BlackListed.clear();

        // Other data which is handled per-batch by the PDF-AggregationService. These are commented-out here, as they will be cleared anyway.
        //ConnSupportUtils.domainsWithConnectionData.clear();
        //UrlUtils.docOrDatasetUrlsWithIDs.clear();

        // The data-structures from the "MachineLearning" class are not added here, since it is in experimental phase, and thus these data-structures will most likely be empty.
    }


    public static String getSelectiveStackTrace(Throwable thr, String initialMessage, int numOfLines)
    {
        StackTraceElement[] stels = thr.getStackTrace();
        StringBuilder sb = new StringBuilder(numOfLines *100);
        if ( initialMessage != null )
            sb.append(initialMessage).append(" Stacktrace:").append(FileUtils.endOfLine);	// This StringBuilder is thread-safe as a local-variable.
        for ( int i = 0; (i < stels.length) && (i <= numOfLines); ++i ) {
            sb.append(stels[i]);
            if (i < numOfLines) sb.append(FileUtils.endOfLine);
        }
        return sb.toString();
    }

}
