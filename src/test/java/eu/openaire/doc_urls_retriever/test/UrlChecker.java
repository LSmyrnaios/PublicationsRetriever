package eu.openaire.doc_urls_retriever.test;

import eu.openaire.doc_urls_retriever.util.http.HttpConnUtils;
import eu.openaire.doc_urls_retriever.util.url.UrlUtils;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;


/**
 * This class contains unit-testing for urls-connectivity.
 * @author Lampros A. Smyrnaios
 */
public class UrlChecker {
	
	private static final Logger logger = LoggerFactory.getLogger(UrlChecker.class);
	
	@Test
	public void runIndividualTests()
	{
		// Here test individual urls.
		
		ArrayList<String> urlList = new ArrayList<>();
		
		//urlList.add("http://repositorio.ipen.br:8080/xmlui/bitstream/handle/123456789/11176/09808.pdf?sequence=1&isAllowed=y");
		//urlList.add("https://ris.utwente.nl/ws/portalfiles/portal/5118887");
		//urlList.add("http://biblioteca.ucm.es/tesis/19972000/X/0/X0040301.pdf");
		//urlList.add("http://vddb.library.lt/fedora/get/LT-eLABa-0001:E.02~2008~D_20080618_115819-91936/DS.005.0.02.ETD");
		//urlList.add("http://dx.doi.org/10.1016/0042-6989(95)90089-6");
		//urlList.add("https://www.sciencedirect.com/science/article/pii/S221478531500694X?via%3Dihub");
		//urlList.add("https://www.sciencedirect.com/science/article/pii/S221478531500694X/pdf?md5=580457b09a692401774fe0069b8ca507&amp;pid=1-s2.0-S221478531500694X-main.pdf");
		//urlList.add("https://jual.nipissingu.ca/wp-content/uploads/sites/25/2016/03/v10202.pdf\" rel=\"");
		//urlList.add("https://ac.els-cdn.com/S221478531500694X/1-s2.0-S221478531500694X-main.pdf?_tid=8cce02f3-f78e-4593-9828-87b40fcb4f18&acdnat=1527114470_60086f5255bb56d2eb01950734b17fb1");
		//urlList.add("http://www.teses.usp.br/teses/disponiveis/5/5160/tde-08092009-112640/pt-br.php");
		//urlList.add("http://www.lib.kobe-u.ac.jp/infolib/meta_pub/G0000003kernel_81004636");
		//urlList.add("https://link.springer.com/article/10.1186/s12889-016-3866-3");
		//urlList.add("http://ajcmicrob.com/en/index.html");
		//urlList.add("http://kar.kent.ac.uk/57872/1/Fudge-Modern_slavery_%26_migrant_workers.pdf");
		//urlList.add("http://summit.sfu.ca/item/12554");	// MetaDocUrl.
		//urlList.add("http://www.journal.ac/sub/view2/273");
		//urlList.add("https://docs.lib.purdue.edu/cgi/viewcontent.cgi?referer&httpsredir=1&params=%2Fcontext%2Fphysics_articles%2Farticle%2F1964%2Ftype%2Fnative%2F&path_info");
		urlList.add("http://epic.awi.de/5818/");
		urlList.add("https://www.sciencedirect.com/science?_ob=MImg&_imagekey=B6TXW-4CCNV6H-1-1G&_cdi=5601&_user=532038&_orig=browse&_coverDate=06%2F30%2F2004&_sk=999549986&view=c&wchp=dGLbVtz-zSkzS&md5=134f1be3418b6d6bdf0325c19562a489&ie=/sdarticle.pdf");
		
		logger.info("Urls to check:");
		for ( String url: urlList )
			logger.info(url);
		
		for ( String url : urlList )
		{
			String urlToCheck = url;
			/*if ( (urlToCheck = URLCanonicalizer.getCanonicalURL(url, null, StandardCharsets.UTF_8)) == null ) {
				logger.warn("Could not cannonicalize url: " + url);
				return;
			}*/
			
			try {
				HttpConnUtils.connectAndCheckMimeType(null, urlToCheck, urlToCheck, urlToCheck, null, true, false);
			} catch (Exception e) {
				UrlUtils.logQuadruple(null, urlToCheck, null, "unreachable", "Discarded at loading time, due to connectivity problems.", null);
			}
		}
	}
	
}
