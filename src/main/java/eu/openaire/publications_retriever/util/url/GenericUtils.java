package eu.openaire.publications_retriever.util.url;

import eu.openaire.publications_retriever.crawler.PageCrawler;
import eu.openaire.publications_retriever.util.file.FileUtils;
import eu.openaire.publications_retriever.util.http.ConnSupportUtils;
import eu.openaire.publications_retriever.util.http.HttpConnUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

public class GenericUtils {

    private static final Logger logger = LoggerFactory.getLogger(GenericUtils.class);


    public static boolean checkInternetConnectivity()
    {
        try {
            URI.create("https://www.google.com/").toURL().openConnection().connect();
            logger.info("The internet connection is successful.");
            return true;
        } catch (Exception e) {
            logger.error("The internet connection has failed!", e);
            return false;
        }
    }


    /**
     * This method deletes all data related to blocking domains.
     * This is useful, when the app is processing hundreds of thousands of urls, and we want to give a "second chance", every now and then.
     * */
    public static void clearBlockingData()
    {
        // Domains' blocking data.
        HttpConnUtils.blacklistedDomains.clear();
        HttpConnUtils.timesDomainsHadInputNotBeingDocNorPage.clear();
        HttpConnUtils.timesDomainsReturnedNoType.clear();
        ConnSupportUtils.timesDomainsReturned5XX.clear();
        ConnSupportUtils.timesDomainsHadTimeoutEx.clear();
        PageCrawler.timesDomainNotGivingInternalLinks.clear();
        PageCrawler.timesDomainNotGivingDocUrls.clear();
        UrlUtils.domainsAndNumHits.clear();    // This hashMap is used to determine if a domain is successful enough to keep it non-blocked.

        // Paths' data, which also contribute to domain-blocking.
        ConnSupportUtils.timesPathsReturned403.clear();
        ConnSupportUtils.domainsMultimapWithPaths403BlackListed.clear();

        // Clear tracking data for the "check_remaining_links"-procedure, which BLOCKS the search of the remaining-internal-links FOR EACH page.
        PageCrawler.should_check_remaining_links = true;
        PageCrawler.timesCheckedRemainingLinks.set(0);
        PageCrawler.timesFoundDocOrDatasetUrlFromRemainingLinks.set(0);
    }


    /**
     * This method clears all domain tracking data from the HashSets and HashMaps.
     * It can be used to allow both reduced memory consumption and a second chance for some domains after a very long time.
     * For example, after a month, a domain might be more responsive, and it should not be blocked anymore.
     * It empties all those data-structures without de-allocating the existing space.
     * This guarantees than the memory-space will not get infinitely large, while avoiding re-allocation of the memory for the next id-url pairs to be handled.
     */
    public static void clearTrackingData()
    {
        clearBlockingData();

        // Clear tracking data of successful detections and downloads.
        UrlUtils.resultUrlsWithIDs.clear();
        ConnSupportUtils.fileHashesWithLocations.clear();

        // Domain additional data, which does not contribute in blocking the domains, but they do contribute in performance.
        HttpConnUtils.domainsSupportingHTTPS.clear();
        HttpConnUtils.domainsWithSlashRedirect.clear();
        HttpConnUtils.domainsWithUnsupportedHeadMethod.clear();
        HttpConnUtils.domainsWithUnsupportedAcceptLanguageParameter.clear();
        HttpConnUtils.domainsWithUnsupportedNewerHTTPVersion.clear();

        // Other data.
        ConnSupportUtils.domainsWithConnectionData.clear();

        // The data-structures from the "MachineLearning" class are not added here, since it is in experimental phase and not running in production, thus these data-structures will most likely be empty.
    }


    public static String getSelectedStackTraceForCausedException(Throwable thr, String firstMessage, String additionalMessage, int numOfLines)
    {
        // The stacktrace of the "ExecutionException" is the one of the current code and not the code which ran inside the background-task. Try to get the cause.
        Throwable causedThrowable = thr.getCause();
        if ( causedThrowable == null ) {
            logger.warn("No cause was retrieved for the \"ExecutionException\"!");
            causedThrowable = thr;
        }
        String initialMessage = firstMessage + causedThrowable.getMessage() + ((additionalMessage != null) ? additionalMessage : "");
        return getSelectiveStackTrace(causedThrowable, initialMessage, numOfLines);
    }


    public static String getSelectiveStackTrace(Throwable thr, String initialMessage, int numOfLines)
    {
        StackTraceElement[] stels = thr.getStackTrace();
        StringBuilder sb = new StringBuilder(numOfLines *100);  // This StringBuilder is thread-safe as a local-variable.
        if ( initialMessage != null )
            sb.append(initialMessage).append(FileUtils.endOfLine);
        sb.append("Stacktrace:").append(FileUtils.endOfLine);
        for ( int i = 0; (i < stels.length) && (i <= numOfLines); ++i ) {
            sb.append(stels[i]);
            if (i < numOfLines) sb.append(FileUtils.endOfLine);
        }
        return sb.toString();
    }

}
