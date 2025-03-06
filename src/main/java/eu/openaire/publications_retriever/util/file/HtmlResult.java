package eu.openaire.publications_retriever.util.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HtmlResult {

	private static final Logger logger = LoggerFactory.getLogger(HtmlResult.class);

	String htmlString;
	FileData htmlFileData;

	public HtmlResult(String htmlString, FileData htmlFileData) {
		this.htmlString = htmlString;
		this.htmlFileData = htmlFileData;
	}


	public String getHtmlString() {
		return htmlString;
	}

	public void setHtmlString(String htmlString) {
		this.htmlString = htmlString;
	}

	public FileData getHtmlFileData() {
		return htmlFileData;
	}

	public void setHtmlFileData(FileData htmlFileData) {
		this.htmlFileData = htmlFileData;
	}

	@Override
	public String toString() {
		return "HtmlResult{" + "htmlString='" + htmlString + '\'' + ", htmlFileData=" + htmlFileData + '}';
	}
}
