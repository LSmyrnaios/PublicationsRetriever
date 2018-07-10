package eu.openaire.doc_urls_retriever.crawler;

import eu.openaire.doc_urls_retriever.util.url.LoadAndCheckUrls;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import eu.openaire.doc_urls_retriever.util.file.FileUtils;
import eu.openaire.doc_urls_retriever.util.url.UrlUtils;

import eu.openaire.doc_urls_retriever.util.http.HttpUtils;


/**
 * @author Lampros A. Smyrnaios
 */
public class CrawlerController
{	
	private static final Logger logger = LoggerFactory.getLogger(CrawlerController.class);
	
	public static long urlsReachedCrawler = 0;	// Potentially useful for statistics.
	public static boolean useIdUrlPairs = true;
	
	
	public CrawlerController() throws RuntimeException
	{
		logger.info("Starting crawler..");
		
		try {
			if ( CrawlerController.useIdUrlPairs )
				LoadAndCheckUrls.loadAndCheckIdUrlPairs();
			else
				LoadAndCheckUrls.loadAndCheckUrls();
			
			//runIndividualTests();
			
		} catch (Exception e) {
			logger.error("", e);
			throw new RuntimeException(e);
		}
		finally {
			// Write any remaining urls from memory to disk.
			if ( !FileUtils.quadrupleToBeLoggedOutputList.isEmpty() ) {
				logger.debug("Writing last set(s) of (\"SourceUrl\", \"DocUrl\"), to disk.");
				FileUtils.writeToFile();
			}
		}
	}
	
	
	public static void runIndividualTests()
	{
		// Here test individual urls.
		//String url = "http://repositorio.ipen.br:8080/xmlui/bitstream/handle/123456789/11176/09808.pdf?sequence=1&isAllowed=y";
		//String url = "https://ris.utwente.nl/ws/portalfiles/portal/5118887";
		//String url = "http://biblioteca.ucm.es/tesis/19972000/X/0/X0040301.pdf";
		//String url = "http://vddb.library.lt/fedora/get/LT-eLABa-0001:E.02~2008~D_20080618_115819-91936/DS.005.0.02.ETD";
		//String url = "http://dx.doi.org/10.1016/0042-6989(95)90089-6";
		//String url = "https://www.sciencedirect.com/science/article/pii/S221478531500694X?via%3Dihub";
		//String url = "https://www.sciencedirect.com/science/article/pii/S221478531500694X/pdf?md5=580457b09a692401774fe0069b8ca507&amp;pid=1-s2.0-S221478531500694X-main.pdf";
		//String url = "https://jual.nipissingu.ca/wp-content/uploads/sites/25/2016/03/v10202.pdf\" rel=\"";
		//String url = "https://ac.els-cdn.com/S221478531500694X/1-s2.0-S221478531500694X-main.pdf?_tid=8cce02f3-f78e-4593-9828-87b40fcb4f18&acdnat=1527114470_60086f5255bb56d2eb01950734b17fb1";
		//String url = "http://www.teses.usp.br/teses/disponiveis/5/5160/tde-08092009-112640/pt-br.php";
		//String url = "http://www.lib.kobe-u.ac.jp/infolib/meta_pub/G0000003kernel_81004636";
		//String url = "https://link.springer.com/article/10.1186/s12889-016-3866-3";
		//String url = "http://ajcmicrob.com/en/index.html";
		//String url = "http://kar.kent.ac.uk/57872/1/Fudge-Modern_slavery_%26_migrant_workers.pdf";
		//String url = "http://summit.sfu.ca/item/12554";
		//String url = "http://www.journal.ac/sub/view2/273";
		//String url = "https://www.youtube.com/watch?v=KuW2VMNArKs";
		String url = "http://epic.awi.de/5818/";
		
		String urlToCheck = url;
		/*if ( (urlToCheck = URLCanonicalizer.getCanonicalURL(url, null, StandardCharsets.UTF_8)) == null ) {
			logger.warn("Could not cannonicalize url: " + url);
			return;
		}*/
		
		try {
			HttpUtils.connectAndCheckMimeType(null, urlToCheck, urlToCheck, urlToCheck, null, true, false);
		} catch (Exception e) {
			UrlUtils.logQuadruple(null, urlToCheck, null, "unreachable", "Discarded at loading time, due to connectivity problems.", null);
		}
	}
	
}
