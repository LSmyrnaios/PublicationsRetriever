package eu.openaire.publications_retriever.test;

import eu.openaire.publications_retriever.crawler.PageCrawler;
import eu.openaire.publications_retriever.exceptions.DocLinkFoundException;
import eu.openaire.publications_retriever.exceptions.DocLinkInvalidException;
import eu.openaire.publications_retriever.util.http.ConnSupportUtils;
import eu.openaire.publications_retriever.util.url.LoaderAndChecker;
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
		LoaderAndChecker.retrieveDocuments = true;
		LoaderAndChecker.retrieveDatasets = true;
		ConnSupportUtils.setKnownMimeTypes();
		UrlTypeChecker.setRuntimeInitializedRegexes();
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
		//exampleUrl = "https://www.ans.org/pubs/journals/nse/article-27191/";
		//exampleUrl = "https://www.hal.inserm.fr/inserm-00348834";
		//exampleUrl = "https://juniperpublishers.com/ofoaj/OFOAJ.MS.ID.555572.php";
		//exampleUrl = "https://iovs.arvojournals.org/article.aspx?articleid=2166142";
		//exampleUrl = "https://www.erudit.org/fr/revues/irrodl/2019-v20-n3-irrodl04799/1062522ar/";
		//exampleUrl = "https://academic.oup.com/nar/article/24/1/125/2359312";
		//exampleUrl = "https://www.thieme-connect.com/products/ejournals/abstract/10.1055/s-2008-1075002";
		//exampleUrl = "https://archiv.ub.uni-marburg.de/ubfind/Record/urn:nbn:de:hebis:04-z2017-0572";
		//exampleUrl = "https://science-of-synthesis.thieme.com/app/text/?id=SD-139-00109";
		//exampleUrl = "https://acikerisim.sakarya.edu.tr/handle/20.500.12619/66006";
		//exampleUrl = "https://www.sciencedirect.com/science/article/pii/0093934X9290124W";
		//exampleUrl = "https://openaccess.marmara.edu.tr/entities/publication/959ebf2d-4e2f-4f4f-a397-b0c2793170ee";
		//exampleUrl = "https://www.aup-online.com/content/journals/10.5117/MEM2015.4.JANS";
		exampleUrl = "https://www.ijcseonline.org/full_paper_view.php?paper_id=4547";
		//exampleUrl = "https://meetingorganizer.copernicus.org/EGU2020/EGU2020-6296.html";
	}

	
	//@Disabled
	@Test
	public void testExtractOneLinkFromHtml()
	{
		String link;
		try {
			HashSet<String> extractedLinksHashSet = getLinksList(exampleHtml, null);
			if ( extractedLinksHashSet == null )
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
			String finalUrl = conn.getURL().toString();
			String html = null;
			if ( (html = ConnSupportUtils.getHtmlString(conn, finalUrl, null, false)) == null ) {
				logger.error("Could not retrieve the HTML-code for pageUrl: " + finalUrl);
				link = null;
			}
			else {
				HashSet<String> extractedLinksHashSet = getLinksList(html, finalUrl);
				if ( extractedLinksHashSet == null )
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
			HashSet<String> extractedLinksHashSet = getLinksList(exampleHtml, null);
			if ( extractedLinksHashSet == null )
				return;	// Logging is handled inside..

			int numberOfExtractedLinks = extractedLinksHashSet.size();
			logger.info("The list of the " + numberOfExtractedLinks + " extracted internalLinks of \"" + exampleUrl + "\" is:");
			for ( String link: extractedLinksHashSet )
				logger.info(link);

			int acceptedLinksCount = 0;
			logger.info("\n\nThe accepted links from the above are:");
			for ( String link : extractedLinksHashSet ) {
				if ( !UrlTypeChecker.shouldNotAcceptInternalLink(link, null) ) {
					logger.info(link);
					acceptedLinksCount ++;
				}
			}

			logger.info("The number of accepted links is: " + acceptedLinksCount + " (out of " + numberOfExtractedLinks + ").");
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
			String finalUrl = conn.getURL().toString();
			String html = null;
			if ( (html = ConnSupportUtils.getHtmlString(conn, finalUrl, null, false)) == null ) {
				logger.error("Could not retrieve the HTML-code for pageUrl: " + finalUrl);
				return;
			}
			//logger.debug("HTML:\n" + html);

			HashSet<String> extractedLinksHashSet = getLinksList(html, finalUrl);
			if ( extractedLinksHashSet == null )
				return;	// Logging is handled inside..

			int numberOfExtractedLinks = extractedLinksHashSet.size();
			logger.info("The list of the " + numberOfExtractedLinks + " extracted internalLinks of \"" + exampleUrl + "\" is:");
			for ( String link: extractedLinksHashSet )
				logger.info(link);

			int acceptedLinksCount = 0;
			logger.info("\nThe accepted links from the above are:");
			for ( String link : extractedLinksHashSet )
			{
				String targetUrl = ConnSupportUtils.getFullyFormedUrl(exampleUrl, link, conn.getURL());
				if ( targetUrl == null ) {
					logger.debug("Could not create target url for resourceUrl: " + conn.getURL().toString() + " having location: " + link);
					continue;
				}
				if ( !UrlTypeChecker.shouldNotAcceptInternalLink(targetUrl, null) ) {
					logger.info(targetUrl);
					acceptedLinksCount ++;
				}
			}

			logger.info("The number of accepted links is: " + acceptedLinksCount + " (out of " + numberOfExtractedLinks + ").");

		} catch (Exception e) {
			logger.error("", e);
		}
	}


	private static HashSet<String> getLinksList(String html, String url)
	{
		HashSet<String> extractedLinksHashSet = null;
		try {
			extractedLinksHashSet = PageCrawler.extractInternalLinksFromHtml(html, url);
			if ( extractedLinksHashSet == null || extractedLinksHashSet.size() == 0 )
				return null;    // Logging is handled inside..
		} catch (Exception e) {
			String link = e.getMessage();
			if ( e instanceof DocLinkFoundException ) {
				// A true-pdf link was found. The only problem is that the list of the links is missing now, since the method exited early.
				// Using step-by-step debugging can reveal all the available HTML-elements captured (which include the pre-extracted links).
				PageCrawler.verifyDocLink("urlId", url, url, null, (DocLinkFoundException) e);
			} else if ( e instanceof DocLinkInvalidException ) {
				logger.warn("A invalid docLink was found: " + link);
			}
			logger.warn("The \"PageCrawler.extractInternalLinksFromHtml()\" method exited early, so the list with the can-be-extracted links was not returned!");
			return null;
		}
		return extractedLinksHashSet;
	}

}
