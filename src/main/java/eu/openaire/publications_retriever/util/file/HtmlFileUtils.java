package eu.openaire.publications_retriever.util.file;

import eu.openaire.publications_retriever.util.args.ArgsUtils;
import eu.openaire.publications_retriever.util.url.UrlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;

public class HtmlFileUtils {

	private static final Logger logger = LoggerFactory.getLogger(HtmlFileUtils.class);

	public static final AtomicInteger htmlFilesNum = new AtomicInteger(0);


	public static boolean downloadHtmlFile(String urlId, String pageUrl, String pageHtml, Matcher urlMatcher)
	{
		String fileName = urlId;
		if ( ArgsUtils.fileNameType.equals(ArgsUtils.fileNameTypeEnum.idName) )
			fileName = urlId;
		else if ( ArgsUtils.fileNameType.equals(ArgsUtils.fileNameTypeEnum.originalName) )
			if ( (fileName = UrlUtils.getDocIdStr(pageUrl, urlMatcher)) == null )
				fileName = urlId;
		else if ( ArgsUtils.fileNameType.equals(ArgsUtils.fileNameTypeEnum.numberName) )
			fileName = String.valueOf(HtmlFileUtils.htmlFilesNum.incrementAndGet());

		try ( FileWriter writer = new FileWriter(ArgsUtils.storeHtmlFilesDir + fileName + ".html") ) {
			writer.write(pageHtml);
		} catch (Exception e) {
			logger.error("Could not write output data to the file.", e);
			if ( ArgsUtils.fileNameType.equals(ArgsUtils.fileNameTypeEnum.numberName) )
				HtmlFileUtils.htmlFilesNum.decrementAndGet();
			return false;
		}
		logger.info("HTML file '" + fileName + ".html' successfully downloaded.");
		if ( !ArgsUtils.fileNameType.equals(ArgsUtils.fileNameTypeEnum.numberName) )
			htmlFilesNum.incrementAndGet();
		return true;
	}
}
