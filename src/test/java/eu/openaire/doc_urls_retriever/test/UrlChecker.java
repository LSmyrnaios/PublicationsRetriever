package eu.openaire.doc_urls_retriever.test;

import com.google.common.collect.HashMultimap;
import eu.openaire.doc_urls_retriever.DocUrlsRetriever;
import eu.openaire.doc_urls_retriever.util.file.FileUtils;
import eu.openaire.doc_urls_retriever.util.http.HttpConnUtils;
import eu.openaire.doc_urls_retriever.util.url.UrlTypeChecker;
import eu.openaire.doc_urls_retriever.util.url.UrlUtils;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Set;

import static eu.openaire.doc_urls_retriever.test.TestNonStandardInputOutput.setInputOutput;
import static eu.openaire.doc_urls_retriever.util.url.LoaderAndChecker.isFinishedLoading;


/**
 * This class contains unit-testing for urls.
 * @author Lampros A. Smyrnaios
 */
public class UrlChecker {
	
	private static final Logger logger = LoggerFactory.getLogger(UrlChecker.class);
	
	@Test
	public void checkUrlConnectivity()
	{
		//FileUtils.shouldDownloadDocFiles = false;	// Default is: "true".
		FileUtils.shouldUseOriginalDocFileNames = true;
		
		// Here test individual urls.
		
		ArrayList<String> urlList = new ArrayList<>();
		
		//urlList.add("http://repositorio.ipen.br:8080/xmlui/bitstream/handle/123456789/11176/09808.pdf?sequence=1&isAllowed=y");
		//urlList.add("https://ris.utwente.nl/ws/portalfiles/portal/5118887");
		//urlList.add("http://biblioteca.ucm.es/tesis/19972000/X/0/X0040301.pdf");
		//urlList.add("http://vddb.library.lt/fedora/get/LT-eLABa-0001:E.02~2008~D_20080618_115819-91936/DS.005.0.02.ETD");
		//urlList.add("http://dx.doi.org/10.1016/0042-6989(95)90089-6");
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
		//urlList.add("http://eprints.rclis.org/11525/");
		//urlList.add("https://doors.doshisha.ac.jp/duar/repository/ir/127/?lang=0");	// This case is providing a docUrl but we can't find it!
		//urlList.add("https://engine.surfconext.nl/authentication/idp/single-sign-on?SAMLRequest=fZLBToNAEIbvPgXZOyyLbVM2habaNJpobAp68LaFga6BWdxZGh9fpG3Ugx43%2BefbP%2FPNYvnRNt4RLGmDCRNByDzAwpQa64Q95xt%2Fzpbp1YJU20SdXPXugDt474GctyIC64a5W4PUt2AzsEddwPPuIWEH5zqSnHe9hcD2dYANV2WrkX%2BheJY9cdVoRWOAeesBqFG5scVlFrDWCAH1tioMwocbIUMFQKeLMcx12XEayjbgk67RN8i8jbEFjFUTVqmGBv79OmFKzMpyGgnxNpkf4tm1mNdiWk73U13Hh6oaQrRVRPoI32NEPdwjOYUuYVEoYj8UvghzMZGTuQxnQTyLX5m3tcaZwjQ3Gk%2Bb6y1Ko0iTRNUCSVfIbPX4IKMglPtTiORdnm%2F97VOWM%2B%2FlYiD6MjA4QZKnnf%2FP6s4fs%2FSkSI6N7U%2FC%2FwB1kcjSP5Ut%2BE92en7%2Bvob0Ew%3D%3D&SigAlg=http%3A%2F%2Fwww.w3.org%2F2000%2F09%2Fxmldsig%23rsa-sha1&Signature=bUnOAaMLkaAT9dgvgntSvE0Sg4VaZXphPaYefmumeVGStqfdh9Gucd%2BfVpEHEP1IUmnPsY%2FXRAS%2FieNmfptxetxfOUpfgrBWkbmIRoth95N2p3PJAAQbrX0Mz2AtCpQ0%2BHXJ%2BgSyVrv%2BZVKQkf%2F6SySMcFovyngpvwovZzGmQ4psf%2F0uY1B1aifJ0X2zlxnUmTJWA3Guk1ucQGqTAaTl0DJwn%2BlfS01kJvRpLVtt4ecnFBx%2FZg8Yl7BmqpBiTJgw%2BQFHIIl%2B7fRBpe9uU%2FlnUPsqvDBGUbS6rUce8IImSV%2BjWyB8yryeUzWrWhKUvvemwBOalBp5FLm5eVkN0GqSBw%3D%3D");
			// Problematic Science-Direct urls....
		//urlList.add("https://linkinghub.elsevier.com/retrieve/pii/S0890540184710054");
		//urlList.add("https://linkinghub.elsevier.com/retrieve/pii/S0002929707623672");
		//urlList.add("https://linkinghub.elsevier.com/retrieve/pii/S0042682297988747");
		//urlList.add("https://www.sciencedirect.com/science/article/pii/S0042682297988747?via%3Dihub");
		//urlList.add("https://www.sciencedirect.com/science/article/pii/S221478531500694X?via%3Dihub");
		//urlList.add("https://www.sciencedirect.com/science/article/pii/S221478531500694X/pdf?md5=580457b09a692401774fe0069b8ca507&amp;pid=1-s2.0-S221478531500694X-main.pdf");
		//urlList.add("https://www.sciencedirect.com/science?_ob=MImg&_imagekey=B6TXW-4CCNV6H-1-1G&_cdi=5601&_user=532038&_orig=browse&_coverDate=06%2F30%2F2004&_sk=999549986&view=c&wchp=dGLbVtz-zSkzS&md5=134f1be3418b6d6bdf0325c19562a489&ie=/sdarticle.pdf");
			// .....
		//urlList.add("http://vddb.library.lt/fedora/get/LT-eLABa-0001:E.02~2006~D_20081203_194425-33518/DS.005.0.01.ETD");
		//urlList.add("http://darwin.bth.rwth-aachen.de/opus3/volltexte/2008/2605/");
		//urlList.add("http://publikationen.ub.uni-frankfurt.de/frontdoor/index/index/docId/26920");
		//urlList.add("http://www.grid.uns.ac.rs/jged/download.php?fid=108");
		//urlList.add("http://www.esocialsciences.org/Download/repecDownload.aspx?fname=Document18112005270.6813013.doc&fcategory=Articles&AId=236&fref=repec");
		//urlList.add("https://wwwfr.uni.lu/content/download/35522/427398/file/2011-05%20-%20Demographic%20trends%20and%20international%20capital%20flows%20in%20an%20integrated%20world.pdf");
		//urlList.add("http://www.grid.uns.ac.rs/jged/download.php?fid=108");
		//urlList.add("https://wwwfr.uni.lu/content/download/35522/427398/file/2011-05%20-%20Demographic%20trends%20and%20international%20capital%20flows%20in%20an%20integrated%20world.pdf");
		//urlList.add("https://www.scribd.com/document/397997565/Document-2-Kdashnk");
		//urlList.add("https://stella.repo.nii.ac.jp/?action=pages_view_main&active_action=repository_view_main_item_detail&item_id=103&item_no=1&page_id=13&block_id=21");
		//urlList.add("https://hal.archives-ouvertes.fr/hal-00328350");
		//urlList.add("https://www.clim-past-discuss.net/8/3043/2012/cpd-8-3043-2012.html");
		//urlList.add("http://www.nature.com/cdd/journal/v22/n3/pdf/cdd2014169a.pdf");
		//urlList.add("https://www.ssoar.info/ssoar/handle/document/20820");
		//urlList.add("https://upcommons.upc.edu/bitstream/handle/2117/11500/FascinatE-D1.1.1-Requirements.pdf?sequence=1&isAllowed=y");
		//urlList.add("https://gala.gre.ac.uk/id/eprint/11492/1/11492_Digges_Marketing%20of%20banana%20%28working%20paper%29%201994.pdf");
		//urlList.add("https://zenodo.org/record/1157336");
		urlList.add("https://zenodo.org/record/1157336/files/Impact%20of%20Biofield%20Energy%20Treated%20%28The%20Trivedi%20Effect%C2%AE%29%20Herbomineral%20Formulation%20on%20the%20Immune%20Biomarkers%20and%20Blood%20Related%20Parameters%20of%20Female%20Sprague%20Dawley%20Rats.pdf");
		urlList.add("http://amcor.asahikawa-med.ac.jp/modules/xoonips/download.php?file_id=3140");
		
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
			
			if ( UrlTypeChecker.matchesUnwantedUrlType(null, urlToCheck, urlToCheck.toLowerCase()) )
				continue;
			
			try {
				HttpConnUtils.connectAndCheckMimeType(null, urlToCheck, urlToCheck, urlToCheck, null, true, false);
			} catch (Exception e) {
				UrlUtils.logQuadruple(null, urlToCheck, null, "unreachable", "Discarded at loading time, due to connectivity problems.", null);
			}
		}
		
		Instant finish = Instant.now();
		
		DocUrlsRetriever.calculateAndPrintElapsedTime(start, finish);
	}
	
	
	//@Test
	public void checkUrlRegex()
	{
		logger.info("Going to test url-triple-regex on multiple urls..");
		
		// List contains urls for REGEX-check
		ArrayList<String> urlList = new ArrayList<>();
		
		urlList.add("http://example.com/path/to/page?name=ferret&color=purple");
		urlList.add("https://upcommons.upc.edu/bitstream/handle/2117/11500/FascinatE-D1.1.1-Requirements.pdf?sequence=1&isAllowed=y");
		urlList.add("https://upcommons.upc.edu/bitstream/handle/2117/11500/?sequence=1&isAllowed=y");
		urlList.add("https://upcommons.upc.edu/bitstream/handle/2117/11500/FascinatE-D1.1.1-Requirements.pdf");
		urlList.add("http://ena.lp.edu.ua:8080/bitstream/ntb/12073/1/17_КОНЦЕПТУАЛЬНІ ЗАСАДИ ФОРМУВАННЯ ДИСТАНЦІЙНОГО.pdf");
		urlList.add("https://hal.archives-ouvertes.fr/hal-01558509/file/locomotion_B&B.pdf");
		urlList.add("https://zenodo.org/record/1157336/files/Impact of Biofield Energy Treated (The Trivedi Effect®) Herbomineral Formulation on the Immune Biomarkers and Blood Related Parameters of Female Sprague Dawley Rats.pdf");
		urlList.add("http://dspace.ou.nl/bitstream/1820/9091/1/Methodological triangulation of the students' use of recorded lectures.pdf");
		urlList.add("http://orca.cf.ac.uk/29804/1/BourneTrust&FinancialElites Ful.pdf");
		urlList.add("https://repository.nwu.ac.za/bitstream/handle/10394/5642/Y&T_2006(SE)_Mthembu.pdf?sequence=1&isAllowed=y");
		urlList.add("http://eprints.nottingham.ac.uk/1718/1/Murphy_&_Whitty,_The_Question_of_Evil_(2006)_FLS.pdf");
		urlList.add("http://paduaresearch.cab.unipd.it/5619/1/BEGHETTO_A_-_L'attività_di_revisione_legale_del_bilancio_d'esercizio.pdf");
		urlList.add("http://paduaresearch.cab.unipd.it/5619/1/BEGHETTO_A_-_L'attivit%C3%A0_di_revisione_legale_del_bilancio_d'esercizio.pdf");
		urlList.add("https://signon.utwente.nl:443/oam/server/obrareq.cgi?encquery%3DNag5hroDAYcZB73s6qFabcJrCLu93LkC%2B%2BehD6VzQDBXjyBeFwtDMuD1y8RrSDHeJy5fC5%2Fy2bJ06QJBGd1f0YAph8D4YcL49l8SbwEcjfrA7TYcvee8aiQakGx1o5pLUN4KrQC%2F3OBf5PrdrMwJb98CJjMkSBGdSMteofa1JVOMTxSQUwTdObMY04eHA51ReEiT3v3fpOlg6%2BcJgtdHSCEhYL2yCt2rgkgPSVoJ%2BqZvFzc6o3FhSmCeXtFiO1FpG5%2BzFSP5JEHVFUerdnw1GpLOtGOT6PpbDf9Fd%2BnAT6Q%3D%20agentid%3DRevProxyWebgate%20ver%3D1%20crmethod%3D2&ECID-Context=1.005YfySQ6km8LunDsnZBCX0002cY00001F%3BkXjE");
		
		
		// Add more urls to test.
		
		for ( String url : urlList )
		{
			validateRegexOnUrl(url);
		}
		
		// Check the urls provided in the input-file.
		setInputOutput();
		
		// Start loading and checking urls.
		HashMultimap<String, String> loadedIdUrlPairs;
		boolean isFirstRun = true;
		while ( true )
		{
			loadedIdUrlPairs = FileUtils.getNextIdUrlPairGroupFromJson(); // Take urls from jsonFile.
			
			if ( isFinishedLoading(loadedIdUrlPairs.isEmpty(), isFirstRun) )    // Throws RuntimeException which is automatically passed on.
				break;
			else
				isFirstRun = false;
			
			Set<String> keys = loadedIdUrlPairs.keySet();
			
			for ( String retrievedId : keys )
				for ( String retrievedUrl : loadedIdUrlPairs.get(retrievedId) )
					validateRegexOnUrl(retrievedUrl);
		}// End-while
	}
	
	
	private static void validateRegexOnUrl(String url)
	{
		logger.info("Checking \"URL_TRIPLE\"-REGEX on url: \"" + url + "\".");
		
		String urlPart = null;
		if ( (urlPart = UrlUtils.getDomainStr(url)) != null )
			logger.info("\t\tDomain: \"" + urlPart + "\"");
		
		urlPart = null;
		if ( (urlPart = UrlUtils.getPathStr(url)) != null )
			logger.info("\t\tPath: \"" + urlPart + "\"");
		
		urlPart = null;
		if ( (urlPart = UrlUtils.getDocIdStr(url)) != null )
			logger.info("\t\tDocID: \"" + urlPart + "\"");
	}
}
