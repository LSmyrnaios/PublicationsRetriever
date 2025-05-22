package eu.openaire.publications_retriever.util.file;

import eu.openaire.publications_retriever.exceptions.FileNotRetrievedException;
import eu.openaire.publications_retriever.util.args.ArgsUtils;
import eu.openaire.publications_retriever.util.url.UrlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;

public class HtmlFileUtils {

	private static final Logger logger = LoggerFactory.getLogger(HtmlFileUtils.class);

	public static final AtomicInteger htmlFilesNum = new AtomicInteger(0);

	public static final HashMap<String, Integer> numbersOfDuplicateHtmlFileNames = new HashMap<>();	// Holds htmlFileNames with their duplicatesNum.
	// We do not need to make it a "ConcurrentHashMap", as any read/write operation happens inside thread-"Locked" code.

	public static FileData getFinalHtmlFilePath(String urlId, String pageUrl, Matcher urlMatcher, int contentSize) throws FileNotRetrievedException
	//, NoSpaceLeftException
	{
		String fileName = urlId;	// For "ArgsUtils.fileNameTypeEnum.idName".
		if ( ArgsUtils.fileNameType.equals(ArgsUtils.fileNameTypeEnum.originalName) ) {
			if ( (fileName = UrlUtils.getDocIdStr(pageUrl, urlMatcher)) == null )
				fileName = urlId;
		} else if ( ArgsUtils.fileNameType.equals(ArgsUtils.fileNameTypeEnum.numberName) )
			fileName = String.valueOf(HtmlFileUtils.htmlFilesNum.incrementAndGet());

		return FileUtils.getFileAndHandleExisting(fileName, ".html", false, contentSize, ArgsUtils.storeHtmlFilesDir, numbersOfDuplicateHtmlFileNames);
	}

}
