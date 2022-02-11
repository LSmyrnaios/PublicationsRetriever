package eu.openaire.publications_retriever.test;

import eu.openaire.publications_retriever.crawler.PageCrawler;
import eu.openaire.publications_retriever.util.http.ConnSupportUtils;
import eu.openaire.publications_retriever.util.url.UrlTypeChecker;
import eu.openaire.publications_retriever.util.url.UrlUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.HashSet;

import static eu.openaire.publications_retriever.util.http.HttpConnUtils.handleConnection;
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
		ConnSupportUtils.setKnownMimeTypes();
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
		//exampleUrl = "https://docs.lib.purdue.edu/jtrp/124/";
		//exampleUrl = "https://www.rug.nl/research/portal/en/publications/op-welke-partijen-richten-lobbyisten-zich(9d42d785-f6a2-4630-b850-61b63d9bfc35).html";
		//exampleUrl = "https://hal-iogs.archives-ouvertes.fr/hal-01576150";
		//exampleUrl = "https://academic.microsoft.com/#/detail/2945595536";
		//exampleUrl = "https://www.ingentaconnect.com/content/cscript/cvia/2017/00000002/00000003/art00008";
		//exampleUrl = "http://europepmc.org/article/PMC/7392279";
		//exampleUrl = "https://www.ingentaconnect.com/content/cscript/cvia/2017/00000002/00000003/art00008";
		//exampleUrl = "https://www.atlantis-press.com/journals/artres/125928993";
		//exampleUrl = "https://pubmed.ncbi.nlm.nih.gov/1461747/";
		//exampleUrl = "https://core.ac.uk/display/91816393";
		//exampleUrl = "https://escholarship.org/uc/item/97b0t7th";
		//exampleUrl = "https://datadryad.org/stash/dataset/doi:10.5061/dryad.v1c28";
		//exampleUrl = "https://zenodo.org/record/3483813";
		//exampleUrl = "http://sedici.unlp.edu.ar/handle/10915/30810";
		//exampleUrl = "https://www.ejinme.com/article/S0953-6205(21)00400-3/fulltext";
		//exampleUrl = "https://direct.mit.edu/neco/article-abstract/21/6/1642/7449/Generation-of-Spike-Trains-with-Controlled-Auto?redirectedFrom=fulltext";
		//exampleUrl = "https://www.eurekaselect.com/51112/chapter/introduction";
		//exampleUrl = "https://www.hal.inserm.fr/inserm-02159846";
		//exampleUrl = "https://ashpublications.org/blood/article/132/Supplement%201/2876/263920/Long-Term-Follow-up-of-Acalabrutinib-Monotherapy";
		//exampleUrl = "https://hal-univ-lyon3.archives-ouvertes.fr/hal-00873244";
		//exampleUrl = "https://journals.lww.com/ijo/Fulltext/2020/68040/Comparative_clinical_trial_of_intracameral.8.aspx";
		exampleUrl = "https://www.ans.org/pubs/journals/nse/article-27191/";
	}

	
	//@Disabled
	@Test
	public void testExtractOneLinkFromHtml()
	{
		String link;
		try {
			HashSet<String> extractedLinksHashSet = PageCrawler.extractInternalLinksFromHtml(exampleHtml, null);
			if ( (extractedLinksHashSet == null) || (extractedLinksHashSet.size() == 0) )
				return;	// Logging is handled inside..

			link = new ArrayList<>(extractedLinksHashSet).get(0);
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
			HttpURLConnection conn = handleConnection(null, exampleUrl, exampleUrl, exampleUrl, UrlUtils.getDomainStr(exampleUrl, null), true, false);

			String newUrl = conn.getURL().toString();
			String html = null;
			if ( (html = ConnSupportUtils.getHtmlString(conn, null)) == null ) {
				logger.error("Could not retrieve the HTML-code for pageUrl: " + newUrl);
				link = null;
			}
			else {
				HashSet<String> extractedLinksHashSet = PageCrawler.extractInternalLinksFromHtml(html, newUrl);
				if ( extractedLinksHashSet == null || extractedLinksHashSet.size() == 0 )
					return;	// Logging is handled inside..

				link = new ArrayList<>(extractedLinksHashSet).get(0);
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
			HashSet<String> extractedLinksHashSet = PageCrawler.extractInternalLinksFromHtml(exampleHtml, null);
			if ( extractedLinksHashSet == null || extractedLinksHashSet.size() == 0 )
				return;	// Logging is handled inside..

			logger.info("The list of all the internalLinks of \"" + exampleUrl + "\" is:");
			for ( String link: extractedLinksHashSet )
				logger.info(link);

			logger.info("\n\nThe accepted links from the above are:");
			for ( String link : extractedLinksHashSet )
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
			HttpURLConnection conn = handleConnection(null, exampleUrl, exampleUrl, exampleUrl, UrlUtils.getDomainStr(exampleUrl, null), true, false);

			String newUrl = conn.getURL().toString();
			String html = null;
			if ( (html = ConnSupportUtils.getHtmlString(conn, null)) == null ) {
				logger.error("Could not retrieve the HTML-code for pageUrl: " + newUrl);
				return;
			}
			//logger.debug("HTML:\n" + html);

			HashSet<String> extractedLinksHashSet = PageCrawler.extractInternalLinksFromHtml(html, newUrl);
			if ( extractedLinksHashSet == null || extractedLinksHashSet.size() == 0 )
				return;	// Logging is handled inside..

			logger.info("The list of all the internalLinks of \"" + exampleUrl + "\" is:");
			for ( String link: extractedLinksHashSet )
				logger.info(link);

			logger.info("\nThe accepted links from the above are:");
			for ( String link : extractedLinksHashSet )
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
