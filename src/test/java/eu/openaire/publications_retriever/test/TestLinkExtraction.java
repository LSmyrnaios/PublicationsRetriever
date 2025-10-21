package eu.openaire.publications_retriever.test;

import eu.openaire.publications_retriever.crawler.PageCrawler;
import eu.openaire.publications_retriever.exceptions.*;
import eu.openaire.publications_retriever.util.args.ArgsUtils;
import eu.openaire.publications_retriever.util.http.ConnSupportUtils;
import eu.openaire.publications_retriever.util.url.UrlTypeChecker;
import eu.openaire.publications_retriever.util.url.UrlUtils;
import org.junit.jupiter.api.BeforeAll;
//import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.HashMap;

import static eu.openaire.publications_retriever.util.http.HttpConnUtils.handleConnection;


/**
 * This class contains unit-testing for internalLinks-extraction.
 * @author Lampros Smyrnaios
 */
public class TestLinkExtraction {
	
	private static final Logger logger = LoggerFactory.getLogger(TestLinkExtraction.class);
	
	private static String exampleHtml;
	private static String exampleUrl;
	
	
	@BeforeAll
	static void setExampleHtml() {
		ArgsUtils.retrieveDocuments = true;
		ArgsUtils.retrieveDatasets = true;
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
		//exampleUrl = "https://www.ijcseonline.org/full_paper_view.php?paper_id=4547";
		//exampleUrl = "https://meetingorganizer.copernicus.org/EGU2020/EGU2020-6296.html";
		//exampleUrl = "https://www.cell.com/comments/0092-8674(79)90026-6";
		//exampleUrl = "https://www.vr-elibrary.de/doi/book/10.14220/9783737007535";
		//exampleUrl = "https://www.euppublishing.com/doi/abs/10.3366/more.2004.41.4.5";
		//exampleUrl = "https://www.vr-elibrary.de/doi/10.13109/wdor.2010.40.2.244";
		//exampleUrl = "https://rjpdft.com/AbstractView.aspx?PID=2015-7-3-10";
		//exampleUrl = "https://journals.lww.com/jwocnonline/Fulltext/2005/11000/Enhancing_Rigor_in_Qualitative_Description.14.aspx";
		//exampleUrl = "https://www.karger.com/Article/Abstract/291962";
		//exampleUrl = "https://www.archivestsc.com/jvi.aspx?un=TKDA-33903";
		//exampleUrl = "https://www.elsevier.es/es-revista-actas-urologicas-espanolas-292-articulo-segunda-neoplasia-tras-el-tratamiento-S0210480611003913";
		//exampleUrl = "https://hal-insu.archives-ouvertes.fr/insu-00355090";
		//exampleUrl = "https://doaj.org/article/ad3d3e8d0fc242d198575400c59cd11f";
		//exampleUrl = "https://riuma.uma.es/xmlui/handle/10630/24721";
		//exampleUrl = "https://pubmed.ncbi.nlm.nih.gov/7379515/";
		//exampleUrl = "https://osf.io/2xpq7/";
		//exampleUrl = "https://openportal.isti.cnr.it/doc?id=people______::4f82377b58548f5a2c910b412841932e";
		//exampleUrl = "https://ri.conicet.gov.ar/handle/11336/82552";
		//exampleUrl = "https://doi.pangaea.de/10.1594/PANGAEA.883146";
		//exampleUrl = "https://doi.pangaea.de/10.1594/PANGAEA.902422";
        exampleUrl = "https://dash.harvard.edu/entities/publication/73120378-c27d-6bd4-e053-0100007fdf3b";
	}

	
	@Test
	public void testExtractOneLinkFromHtml()
	{
		String link = null;
		try {
			HashMap<String, String> extractedLinksHashSet = getLinksList(exampleHtml, null);
			if ( extractedLinksHashSet == null )
				throw new RuntimeException("No links were extracted from html!");	// Logging is handled inside..
            else if ( extractedLinksHashSet.isEmpty() )
                return;
			link = new ArrayList<>(extractedLinksHashSet.keySet()).getFirst();
			logger.info("The single-retrieved internalLink is: \"" + link + "\"");
		} catch (Exception e) {
			logger.error("", e);
		}
	}


    //@Disabled
    @Test
	public void testExtractOneLinkFromUrl()
	{
		// This is actually a test of how link-extraction from an HTTP-300-page works.
		String link;
		try {
			HttpURLConnection conn = handleConnection(null, exampleUrl, exampleUrl, exampleUrl, UrlUtils.getDomainStr(exampleUrl, null), true, false);
			String finalUrl = conn.getURL().toString();
            String html;
			if ( (html = ConnSupportUtils.getHtmlString(conn, finalUrl, null, false, null)) == null ) {
				logger.error("Could not retrieve the HTML-code for pageUrl: " + finalUrl);
			} else {
                //logger.debug("HTML:\n" + html);
                HashMap<String, String> extractedLinksHashSet = getLinksList(html, finalUrl);
                if ( extractedLinksHashSet == null )
                    throw new RuntimeException("No links were extracted from url: " + exampleUrl);	// Logging is handled inside..
                else if ( extractedLinksHashSet.isEmpty() )
                    return;
                link = new ArrayList<>(extractedLinksHashSet.keySet()).getFirst();
                logger.info("The single-retrieved internalLink is: \"" + link + "\"");
            }
		} catch (Exception e) {
			logger.error("", e);
		}
	}
	
	
	@Test
	public void testExtractAllLinksFromHtml()
	{
		try {
			HashMap<String, String> extractedLinksHashSet = getLinksList(exampleHtml, null);
			if ( extractedLinksHashSet == null )
				return;	// Logging is handled inside..

			int numberOfExtractedLinks = extractedLinksHashSet.size();
			logger.info("The list of the " + numberOfExtractedLinks + " extracted internalLinks of \"" + exampleUrl + "\" is:");
			for ( String link: extractedLinksHashSet.keySet() )
				logger.info(link);

			int acceptedLinksCount = 0;
			logger.info("\n\nThe accepted links from the above are:");
			for ( String link : extractedLinksHashSet.keySet() ) {
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
	public void testExtractAllLinksFromUrl() throws AlreadyFoundDocUrlException, ConnTimeoutException, DomainBlockedException, DomainWithUnsupportedHEADmethodException, IOException {
		try {
			HttpURLConnection conn = handleConnection(null, exampleUrl, exampleUrl, exampleUrl, UrlUtils.getDomainStr(exampleUrl, null), true, false);
			String finalUrl = conn.getURL().toString();
            String html;
            if ( (html = ConnSupportUtils.getHtmlString(conn, finalUrl, null, false, null)) == null ) {
                String errorMsg = "Could not retrieve the HTML-code for pageUrl: " + finalUrl;
                logger.error(errorMsg);
                throw new RuntimeException(errorMsg);
            }
			//logger.debug("HTML:\n" + html);

			HashMap<String, String> extractedLinksHashSet = getLinksList(html, finalUrl);
			if ( extractedLinksHashSet == null )
				throw new RuntimeException("No links could be extracted from html!");	// Logging is handled inside..

			int numberOfExtractedLinks = extractedLinksHashSet.size();
			logger.info("The list of the " + numberOfExtractedLinks + " extracted internalLinks of \"" + exampleUrl + "\" is:");
			for ( String link: extractedLinksHashSet.keySet() )
				logger.info(link);

			int acceptedLinksCount = 0;
			logger.info("\nThe accepted links from the above are:");
			for ( String link : extractedLinksHashSet.keySet() )
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
            throw e;
		}
	}


	private static HashMap<String, String> getLinksList(String html, String url)
	{
		HashMap<String, String> extractedLinksHashMap = new HashMap<>();
		try {
			extractedLinksHashMap = PageCrawler.extractInternalLinksFromHtml(html, url);
			if ( extractedLinksHashMap == null || extractedLinksHashMap.size() == 0 )
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
			return extractedLinksHashMap;
		}
		return extractedLinksHashMap;
	}

}
