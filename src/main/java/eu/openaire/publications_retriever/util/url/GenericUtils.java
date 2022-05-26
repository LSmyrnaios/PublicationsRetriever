package eu.openaire.publications_retriever.util.url;

import eu.openaire.publications_retriever.util.file.FileUtils;
import eu.openaire.publications_retriever.util.http.ConnSupportUtils;
import eu.openaire.publications_retriever.util.http.HttpConnUtils;

public class GenericUtils {


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
        UrlUtils.domainsAndHits.clear();

        // Domain additional data, which does not contribute in blocking the domains, but they do contribute in performance.
        HttpConnUtils.domainsSupportingHTTPS.clear();
        HttpConnUtils.domainsWithUnsupportedAcceptLanguageParameter.clear();

        // Paths' data.
        ConnSupportUtils.timesPathsReturned403.clear();
        ConnSupportUtils.domainsMultimapWithPaths403BlackListed.clear();
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
