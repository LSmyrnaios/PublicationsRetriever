package eu.openaire.publications_retriever.util.file;

import eu.openaire.publications_retriever.util.args.ArgsUtils;
import eu.openaire.publications_retriever.util.url.UrlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;

public class HtmlFileUtils {

	private static final Logger logger = LoggerFactory.getLogger(HtmlFileUtils.class);

	public static final AtomicInteger htmlFilesNum = new AtomicInteger(0);


	public static boolean downloadHtmlFile(String urlId, String sourceUrl, String pageUrl, String pageHtml, Matcher urlMatcher)
	{
		String fileName = urlId;
		if ( ArgsUtils.fileNameType.equals(ArgsUtils.fileNameTypeEnum.idName) )
			fileName = urlId;
		else if ( ArgsUtils.fileNameType.equals(ArgsUtils.fileNameTypeEnum.originalName) )
			if ( (fileName = UrlUtils.getDocIdStr(pageUrl, urlMatcher)) == null )
				fileName = urlId;
		else if ( ArgsUtils.fileNameType.equals(ArgsUtils.fileNameTypeEnum.numberName) )
			fileName = String.valueOf(HtmlFileUtils.htmlFilesNum.incrementAndGet());

		String fullPathFileName = ArgsUtils.storeHtmlFilesDir + fileName + ".html";

		// TODO - Add check for existing file, like we do with the full-texts.

		try ( FileOutputStream fileOutputStream = new FileOutputStream(fullPathFileName) )
		{
			fileOutputStream.write(pageHtml.getBytes(StandardCharsets.UTF_8));
			if ( ArgsUtils.shouldJustDownloadHtmlFiles ) {
				FileData fileData = new FileData(new File(fullPathFileName), fileOutputStream);
				fileData.calculateAndSetHashAndSize();
				UrlUtils.addOutputData(urlId, sourceUrl, pageUrl, "N/A", "N/A", fullPathFileName, null, true, "true", "true", "N/A", "N/A", "true", fileData.getSize(), fileData.getHash(), "text/html");    // we send the urls, before and after potential redirections.
			}
		} catch (Exception e) {
			logger.error("Could not write output data to the html-file.", e);
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
