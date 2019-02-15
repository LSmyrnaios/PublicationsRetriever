package eu.openaire.doc_urls_retriever.test;

import eu.openaire.doc_urls_retriever.DocUrlsRetriever;
import eu.openaire.doc_urls_retriever.util.http.HttpConnUtils;
import eu.openaire.doc_urls_retriever.util.url.UrlUtils;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
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
		//urlList.add("http://epic.awi.de/5818/");
		//urlList.add("https://www.sciencedirect.com/science?_ob=MImg&_imagekey=B6TXW-4CCNV6H-1-1G&_cdi=5601&_user=532038&_orig=browse&_coverDate=06%2F30%2F2004&_sk=999549986&view=c&wchp=dGLbVtz-zSkzS&md5=134f1be3418b6d6bdf0325c19562a489&ie=/sdarticle.pdf");
		urlList.add("http://eprints.rclis.org/11525/");
		//urlList.add("https://doors.doshisha.ac.jp/duar/repository/ir/127/?lang=0");	// This case is providing a docUrl but we can't find it!
		//urlList.add("https://engine.surfconext.nl/authentication/idp/single-sign-on?SAMLRequest=fZLBToNAEIbvPgXZOyyLbVM2habaNJpobAp68LaFga6BWdxZGh9fpG3Ugx43%2BefbP%2FPNYvnRNt4RLGmDCRNByDzAwpQa64Q95xt%2Fzpbp1YJU20SdXPXugDt474GctyIC64a5W4PUt2AzsEddwPPuIWEH5zqSnHe9hcD2dYANV2WrkX%2BheJY9cdVoRWOAeesBqFG5scVlFrDWCAH1tioMwocbIUMFQKeLMcx12XEayjbgk67RN8i8jbEFjFUTVqmGBv79OmFKzMpyGgnxNpkf4tm1mNdiWk73U13Hh6oaQrRVRPoI32NEPdwjOYUuYVEoYj8UvghzMZGTuQxnQTyLX5m3tcaZwjQ3Gk%2Bb6y1Ko0iTRNUCSVfIbPX4IKMglPtTiORdnm%2F97VOWM%2B%2FlYiD6MjA4QZKnnf%2FP6s4fs%2FSkSI6N7U%2FC%2FwB1kcjSP5Ut%2BE92en7%2Bvob0Ew%3D%3D&SigAlg=http%3A%2F%2Fwww.w3.org%2F2000%2F09%2Fxmldsig%23rsa-sha1&Signature=bUnOAaMLkaAT9dgvgntSvE0Sg4VaZXphPaYefmumeVGStqfdh9Gucd%2BfVpEHEP1IUmnPsY%2FXRAS%2FieNmfptxetxfOUpfgrBWkbmIRoth95N2p3PJAAQbrX0Mz2AtCpQ0%2BHXJ%2BgSyVrv%2BZVKQkf%2F6SySMcFovyngpvwovZzGmQ4psf%2F0uY1B1aifJ0X2zlxnUmTJWA3Guk1ucQGqTAaTl0DJwn%2BlfS01kJvRpLVtt4ecnFBx%2FZg8Yl7BmqpBiTJgw%2BQFHIIl%2B7fRBpe9uU%2FlnUPsqvDBGUbS6rUce8IImSV%2BjWyB8yryeUzWrWhKUvvemwBOalBp5FLm5eVkN0GqSBw%3D%3D");
			// Problematic Science-Direct urls.
		//urlList.add("https://linkinghub.elsevier.com/retrieve/pii/S0890540184710054");
		//urlList.add("https://linkinghub.elsevier.com/retrieve/pii/S0002929707623672");
		//urlList.add("https://linkinghub.elsevier.com/retrieve/pii/S0042682297988747");
		//urlList.add("https://www.sciencedirect.com/science/article/pii/S0042682297988747?via%3Dihub");
		
		logger.info("Urls to check:");
		for ( String url: urlList )
			logger.info(url);
		
		Instant start = Instant.now();
		
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
		
		Instant finish = Instant.now();
		
		DocUrlsRetriever.calculateAndPrintElapsedTime(start, finish);
	}
	
}