package eu.openaire.doc_urls_retriever.test;

import eu.openaire.doc_urls_retriever.crawler.PageCrawler;
import eu.openaire.doc_urls_retriever.util.http.ConnSupportUtils;
import eu.openaire.doc_urls_retriever.util.url.UrlTypeChecker;
import eu.openaire.doc_urls_retriever.util.url.UrlUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.util.ArrayList;

import static eu.openaire.doc_urls_retriever.util.http.HttpConnUtils.handleConnection;
import static org.junit.jupiter.api.Assertions.assertEquals;


/**
 * This class contains unit-testing for internalLinks-extraction.
 * @author Lampros Smyrnaios
 */
public class LinkExtraction {
	
	private static final Logger logger = LoggerFactory.getLogger(LinkExtraction.class);
	
	private static String exampleHtml;
	private static String exampleUrl;
	
	
	@BeforeAll
	static void setExampleHtml() {
		exampleHtml = "<head><head>" +
				"<body>" +
					"<p>Select a link from below!</p>" +
					"<a href=\"http://www.example.com/examplePath1\"></a>" +
					"<a href=\"http://www.example.com/examplePath2\"></a>" +
				"<body>";
	}
	
	
	@BeforeAll
	static void setExampleUrl()
	{
		//exampleUrl = "http://epress.lib.uts.edu.au/journals/index.php/mcs/article/view/5655";
		//exampleUrl = "https://halshs.archives-ouvertes.fr/halshs-01698574";
		//exampleUrl = "https://doors.doshisha.ac.jp/duar/repository/ir/127/?lang=0";
		//exampleUrl = "https://www.sciencedirect.com/science/article/pii/S0042682297988747?via%3Dihub";
		//exampleUrl = "https://ieeexplore.ieee.org/document/8998177";
		//exampleUrl = "http://kups.ub.uni-koeln.de/1052/";
		//exampleUrl = "https://www.competitionpolicyinternational.com/from-collective-dominance-to-coordinated-effects-in-eu-competition-policy/";
		//exampleUrl = "https://upcommons.upc.edu/handle/2117/20502";
		//exampleUrl = "https://gala.gre.ac.uk/id/eprint/11492/";
		//exampleUrl = "https://edoc.hu-berlin.de/handle/18452/16660";
		exampleUrl = "https://docs.lib.purdue.edu/jtrp/124/";
	}

	
	//@Disabled
	@Test
	public void testExtractOneLinkFromHtml()
	{
		String link;
		try {
			link = new ArrayList<>(PageCrawler.extractInternalLinksFromHtml(exampleHtml)).get(0);
			logger.info("The single-retrieved internalLink is: \"" + link + "\"");
			
		} catch (Exception e) {
			logger.error("", e);
			link = null;
			assertEquals("retrievedLink", link);
		}
		
		if ( link == null )
			assertEquals("retrievedLink", link);
	}
	
	
	@Test
	public void testExtractOneLinkFromUrl()
	{
		// This is actually a test of how link-extraction from an HTTP-300-page works.
		
		String link;
		try {
			HttpURLConnection conn = handleConnection(null, exampleUrl, exampleUrl, exampleUrl, UrlUtils.getDomainStr(exampleUrl), true, false);

			String html = null;
			if ( (html = ConnSupportUtils.getHtmlString(conn)) == null ) {
				logger.error("Could not retrieve the HTML-code for pageUrl: " + conn.getURL().toString());
				link = null;
			}
			else {
				link = new ArrayList<>(PageCrawler.extractInternalLinksFromHtml(html)).get(0);
				logger.info("The single-retrieved internalLink is: \"" + link + "\"");
			}
		} catch (Exception e) {
			logger.error("", e);
			link = null;
		}
		
		if ( link == null )
			assertEquals("retrievedLink", link);
	}
	
	
	//@Disabled
	@Test
	public void testExtractAllLinksFromHtml()
	{
		try {
			ArrayList<String> links = new ArrayList<>(PageCrawler.extractInternalLinksFromHtml(exampleHtml));
			
			logger.info("The list of all the internalLinks of \"" + exampleUrl + "\" is:");
			for ( String link: links )
				logger.info(link);

			logger.info("\nThe accepted links from the above are:");
			for ( String link : links )
				if ( !UrlTypeChecker.shouldNotAcceptInternalLink(link, null) )
					logger.info(link);
			
		} catch (Exception e) {
			logger.error("", e);
		}
	}
	
	
	//@Disabled
	@Test
	public void testExtractAllLinksFromUrl()
	{
		try {
			HttpURLConnection conn = handleConnection(null, exampleUrl, exampleUrl, exampleUrl, UrlUtils.getDomainStr(exampleUrl), true, false);

			String html = null;
			if ( (html = ConnSupportUtils.getHtmlString(conn)) == null ) {
				logger.error("Could not retrieve the HTML-code for pageUrl: " + conn.getURL().toString());
				return;
			}
			//logger.debug("HTML:\n" + html);

			ArrayList<String> links = new ArrayList<>(PageCrawler.extractInternalLinksFromHtml(html));
			
			logger.info("The list of all the internalLinks of \"" + exampleUrl + "\" is:");
			for ( String link: links )
				logger.info(link);

			logger.info("\nThe accepted links from the above are:");
			for ( String link : links )
			{
				String targetUrl = ConnSupportUtils.getFullyFormedUrl(null, link, conn.getURL());
				if ( targetUrl == null ) {
					logger.debug("Could not create target url for resourceUrl: " + conn.getURL().toString() + " having location: " + link);
					continue;
				}
				if ( !UrlTypeChecker.shouldNotAcceptInternalLink(targetUrl, null) )
					logger.info(targetUrl);
			}
			
		} catch (Exception e) {
			logger.error("", e);
		}
	}
	
}
