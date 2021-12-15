package eu.openaire.publications_retriever.util.url;

import eu.openaire.publications_retriever.util.file.FileUtils;

public class GenericUtils {

    public static String getSelectiveStackTrace(Throwable thr, String initialMessage, int numOfLines)
    {
        StackTraceElement[] stels = thr.getStackTrace();
        StringBuilder sb = new StringBuilder(22);
        if ( initialMessage != null )
            sb.append(initialMessage).append(" Stacktrace:").append(FileUtils.endOfLine);	// This StringBuilder is thread-safe as a local-variable.
        for ( int i = 0; (i < stels.length) && (i <= numOfLines); ++i ) {
            sb.append(stels[i]);
            if (i < numOfLines) sb.append(FileUtils.endOfLine);
        }
        return sb.toString();
    }

}
