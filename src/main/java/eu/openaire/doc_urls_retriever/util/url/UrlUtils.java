package eu.openaire.doc_urls_retriever.util.url;

import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.HashMultimap;
import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.url.URLCanonicalizer;
import eu.openaire.doc_urls_retriever.crawler.MachineLearning;
import eu.openaire.doc_urls_retriever.crawler.CrawlerController;

import eu.openaire.doc_urls_retriever.crawler.PageCrawler;
import eu.openaire.doc_urls_retriever.exceptions.CanonicalizationFailedException;

import eu.openaire.doc_urls_retriever.util.file.FileUtils;
import eu.openaire.doc_urls_retriever.util.http.HttpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class contains various methods and regexes to interact with URLs.
 * @author Lampros A. Smyrnaios
 */
public class UrlUtils
{
	private static final Logger logger = LoggerFactory.getLogger(UrlUtils.class);
	
	public final static Pattern URL_TRIPLE = Pattern.compile("(.+:\\/\\/(?:www(?:(?:\\w+)?\\.)?)?([\\w\\.\\-]+)(?:[\\:\\d]+)?(?:.*\\/)?(?:[\\w\\.\\,\\-\\_\\%\\:\\~]*\\?[\\w\\.\\,\\-\\_\\%\\:\\~]+\\=)?)(.+)?");
	// URL_TRIPLE regex to group domain, path and ID --> group <1> is the regular PATH, group<2> is the DOMAIN and group <3> is the regular "ID".
	
	public static final Pattern URL_DIRECTORY_FILTER =
			Pattern.compile(".*\\/(?:profile|login|auth\\.|authentication\\.|ac(?:c)?ess|join|subscr|register|submit|post\\/|send\\/|shop\\/|import|bookmark|announcement|rss|feed|about|faq|wiki|news|events|cart|support|sitemap|license|disclaimer|polic(?:y|ies)|privacy|terms|help|law"
							+ "|(?:my|your)?account|user|author|editor|citation|review|external|statistics|application|permission|ethic|contact|survey|wallet|contribute|deposit|donate|template|logo|image|photo|advertiser|people"
							+ "|error|(?:mis|ab)use|gateway|sorryserver|notfound|404\\.(?:\\w)?htm).*");
	// We check them as a directory to avoid discarding publications's urls about these subjects. There's "acesso" (single "c") in Portuguese.
	
	public static final Pattern CURRENTLY_UNSUPPORTED_DOC_EXTENSION_FILTER = Pattern.compile(".+\\.(?:doc|docx|ppt|pptx)(?:\\?.+)?$");	// Doc-extensions which are currently unsupported.
	
	public static final Pattern PAGE_FILE_EXTENSION_FILTER = Pattern.compile(".+\\.(?:ico|css|js|gif|jpg|jpeg|png|wav|mp3|mp4|webm|mkv|mov|pt|xml|rdf|bib|nt|refer|enw|ris|n3|csv|tsv|mso|dtl|svg|asc|txt|c|cc|cxx|cpp|java|py)(?:\\?.+)?$");
	
	public static final Pattern INNER_LINKS_KEYWORDS_FILTER = Pattern.compile(".*(?:doi.org|mailto:|\\?lang=).*");	// Plain key-words inside innerLinks-String. We avoid "doi.org" in inner links, as, after many redirects, they will reach the same pageUrl.
	
	public static final Pattern INNER_LINKS_FILE_EXTENSION_FILTER = Pattern.compile(".+\\.(?:ico|css|js|gif|jpg|jpeg|png|wav|mp3|mp4|webm|mkv|mov|pt|xml|rdf|bib|nt|refer|enw|ris|n3|csv|tsv|mso|dtl|svg|do|asc|txt|c|cc|cxx|cpp|java|py)(?:\\?.+)?$");
    // In the above, don't include .php and relative extensions, since even this can be a docUrl. For example: https://www.dovepress.com/getfile.php?fileID=5337
	
	// So, we make a new REGEX for these extensions, this time, without a potential argument in the end (e.g. ?id=XXX..), except for the potential "lang".
	public static final Pattern PLAIN_PAGE_EXTENSION_FILTER = Pattern.compile(".+\\.(?:php|php2|php3|php4|php5|phtml|htm|html|shtml|xht|xhtm|xhtml|xml|rdf|bib|nt|refer|enw|ris|n3|csv|tsv|aspx|asp|jsp|do|asc)$");
	
	public static final Pattern INNER_LINKS_FILE_FORMAT_FILTER = Pattern.compile(".+format=(?:xml|htm|html|shtml|xht|xhtm|xhtml).*");
    
    public static final Pattern SPECIFIC_DOMAIN_FILTER = Pattern.compile(".+:\\/\\/.*(?:google|goo.gl|gstatic|facebook|twitter|youtube|linkedin|wordpress|s.w.org|ebay|bing|amazon\\.|wikipedia|myspace|yahoo|mail|pinterest|reddit|blog|tumblr"
																					+ "|evernote|skype|microsoft|adobe|buffer|digg|stumbleupon|addthis|delicious|dailymotion|gostats|blogger|copyright|friendfeed|newsvine|telegram|getpocket"
																					+ "|flipboard|instapaper|line.me|telegram|vk|ok.rudouban|baidu|qzone|xing|renren|weibo|doubleclick|github).*\\/.*");
    
    public static final Pattern PLAIN_DOMAIN_FILTER = Pattern.compile(".+:\\/\\/[\\w.:-]+(?:\\/)?$");	// Exclude plain domains' urls.
	
    public static final Pattern JSESSIONID_FILTER = Pattern.compile("(.+:\\/\\/.+)(?:\\;(?:JSESSIONID|jsessionid)=.+)(\\?.+)");
	
    public static final Pattern DOC_URL_FILTER = Pattern.compile(".+(pdf|download|/doc|document|(?:/|[?]|&)file|/fulltext|attachment|/paper|viewfile|viewdoc|/get|cgi/viewcontent.cgi?).*");
    // "DOC_URL_FILTER" works for lowerCase Strings (we make sure they are in lowerCase before we check).
    // Note that we still need to check if it's an alive link and if it's actually a docUrl (though it's mimeType).
	
	public static final Pattern MIME_TYPE_FILTER = Pattern.compile("([\\w]+\\/[\\w\\+\\-\\.]+)(?:\\;.+)?");
	
	public static final Pattern DOI_ORG_J_FILTER = Pattern.compile(".+[doi.org]\\/[\\d]{2}\\.[\\d]{4}\\/[j]\\..+");	// doi.org urls which has this form and redirect to "sciencedirect.com".
	
	public static final Pattern DOI_ORG_PARENTHESIS_FILTER = Pattern.compile(".+[doi.org]\\/[\\d]{2}\\.[\\d]{4}\\/[\\w]*[\\d]{4}\\-[\\d]{3}(?:[\\d]|[\\w])[\\(][\\d]{2}[\\)][\\d]{5}\\-(?:[\\d]|[\\w])");	// Same reason as above.
	
	public static int sumOfDocsFound = 0;	// Change it back to simple int if finally in singleThread mode
	public static long inputDuplicatesNum = 0;
	
	public static HashSet<String> duplicateUrls = new HashSet<String>();
	public static HashSet<String> docUrls = new HashSet<String>();
	public static HashSet<String> knownDocTypes = new HashSet<String>();
	
	// Counters for certain unwanted domains. We show statistics in the end.
	public static int javascriptPageUrls = 0;
	public static int sciencedirectUrls = 0;
	public static int elsevierUnwantedUrls = 0;
	public static int crawlerSensitiveDomains = 0;
	public static int doajResultPageUrls = 0;
	public static int pagesWithHtmlDocUrls = 0;
	public static int pagesRequireLoginToAccessDocFiles = 0;
	public static int pagesWithLargerCrawlingDepth = 0;	// Pages with their docUrl behind an inner "view" page.
	public static int longToRespondUrls = 0;	// Urls belonging to domains which take too long to respond.
	public static int doiOrgToScienceDirect = 0;	// Urls from "doi.org" which redirect to "sciencedirect.com".
	public static int urlsWithUnwantedForm = 0;	// (plain domains, unwanted page-extensions ect.)
	public static int pangaeaUrls = 0;	// These urls are in false form by default, but even if they weren't or we transform them, PANGAEA. only gives datasets, not fulltext.
	public static int connProblematicUrls = 0;	// Urls known to have connectivity problems, such as long conn times etc.
	public static int pagesNotProvidingDocUrls = 0;
	
	static {
		logger.debug("Setting knownDocTypes. Currently testing only \".pdf\" type.");
		knownDocTypes.add("application/pdf");	// For the moment we care only for the pdf type.
	}
	
	
	/**
	 * This method loads the urls from the input file in memory and check their type.
	 * Then, the loaded urls will either reach the connection point, were they will be checked for a docMimeType or they will be send directly for crawling.
	 * @throws RuntimeException
	 */
	public static void loadAndCheckUrls() throws RuntimeException
	{
		Collection<String> loadedUrlGroup;
		boolean isFirstRun = true;
		
		// Start loading and checking urls.
		while ( true )
		{
			loadedUrlGroup = FileUtils.getNextUrlGroupTest();	// Take urls from single-columned (testing) csvFile.
			
			if ( isFinishedLoading(loadedUrlGroup.isEmpty(), isFirstRun) )	// Throws RuntimeException which is automatically passed on.
				break;
			else
				isFirstRun = false;
			
			for ( String retrievedUrl : loadedUrlGroup )
			{
				String lowerCaseUrl = retrievedUrl.toLowerCase();	// Only for string checking purposes, not supposed to reach any connection.
				
				if ( (retrievedUrl = handleUrlCheckAtLoading(retrievedUrl, lowerCaseUrl)) == null )
					continue;
				
				// After utl-type checks, see if this is a possibleDocUlr and if so, connect to it. Otherwise, add it to the crawler and move on.
				try {
					if ( !handleIfPossibleDocUrlAtLoading(retrievedUrl, lowerCaseUrl) )
						CrawlerController.controller.addSeed(retrievedUrl);	// Canonicalization is performed by Crawler4j itself.
				} catch (CanonicalizationFailedException cfe) {
					// This url was badly-formed, no special-handling here, move on to the next one.
				}
				
			}// end for-loop
		}// end while-loop
	}
	
	
	/**
	 * This method loads the id-url pairs from the input file, in memory and check them. TODO - Add/Optimize documentation.
	 * Then, the loaded urls will either reach the connection point, were they will be checked for a docMimeType or they will be send directly for crawling.
	 * @throws RuntimeException
	 */
	public static void loadAndCheckIdUrlPairs() throws RuntimeException
	{
		HashMultimap<String, String> loadedIdUrlPairs;
		boolean isFirstRun = true;
		
		// Start loading and checking urls.
        while ( true )
        {
			loadedIdUrlPairs = FileUtils.getNextIdUrlPairGroupFromJson(); // Take urls from jsonFile.
			
			if ( isFinishedLoading(loadedIdUrlPairs.isEmpty(), isFirstRun) )	// Throws RuntimeException which is automatically passed on.
				break;
			else
				isFirstRun = false;
			
			List<String> urlList = new ArrayList<String>();	// Theoretically, is faster to add 3 elements in a new list, than removing 3 values from a Multimap, after finding the key between 3000 other keys.
			boolean goToNextId = false;
			
			//logger.debug("CurGroup IDs-size: " + loadedIdUrlPairs.keySet().size());	// DEBUG!
			
			for ( String retrievedId : loadedIdUrlPairs.keySet() )
			{
				//logger.debug("ID: " + retrievedId);	// DEBUG!
				
				for ( String retrievedUrl : loadedIdUrlPairs.get(retrievedId) )
				{
					//logger.debug("     URL: " + retrievedUrl);	// DEBUG!
					
					String lowerCaseUrl = retrievedUrl.toLowerCase();    // Only for string checking purposes, not supposed to reach any connection.
					
					if ( (retrievedUrl = handleUrlCheckAtLoading(retrievedUrl, lowerCaseUrl)) == null )
						continue;
					
					// Check if it's a possible-DocUrl, if so, this is the only url which will be checked from this group, unless there's a canonicalization problem.
					try {
						if ( handleIfPossibleDocUrlAtLoading(retrievedUrl, lowerCaseUrl) ) {
							goToNextId = true;
							break;
						}
					} catch (CanonicalizationFailedException cfe) {
						// This url was badly-formed, but any other url in this group will reach this same badly-formed url anyway.. so just move on tho the next group.
						goToNextId = true;
						break;
					}
					
					urlList.add(retrievedUrl);	// Store the needToBeCrawled-url in a new tempList, to be handled appropriately.
				}// end-for
				
				if ( goToNextId ) {    // If either the docUrl was found, or the possible-DocUrl was broken, continue with the next group. It will get false in the next iteration by its own.
					goToNextId = false;
					urlList.clear();
					continue;
				}
				
	        	// Here, since we can't find the docUrl right-away, choose the bestUrl to be crawled (needs less time to give the docUrl and connect with it).
					// An example would be a url which exists in two faces: the pre-redirections and the post-redirections, choosing the post-one will result in faster docUrl-retrieval.
					// Custom rules for two-faces URLs, have to be added hardcoded (at least in the beginning). For example "hdl.handle.net" always redirect to a url which contains "/handle/".
				int urlsInList = urlList.size();
				if ( urlsInList > 0 ) {	// If a valid-url existed in this group..
					if ( urlsInList > 1 ) {    // If we still have a group of duplicates..
						
						String bestUrl = null;
						String nonDoiUrl = null;
						
						for ( String url : urlList ) {
							// We already checked if there is a possible docUrl within the values.. so here we decide which url from this group we will crawl.
							
							// TODO - Use custom rules (no MLA can be used at this point, since the urls added to Crawler4j will be crawled after loading is finished), to define which urls are best-cases among others (which ones take less time to connect).
							
							// Add this rule, if we accept the slow "hdl.handle.net"
							if ( url.contains("/handle/") ) {	// If this url contains "/handle/" we know that it's a bestCaseUrl among urls from the domain "handle.net", which after redirects reaches the bestCaseUrl (containing "/handle/").
								bestUrl = url;
								break;
							}
							
							if ( (nonDoiUrl == null) && !url.contains("doi.org") )	// If we find a nonDoiUrl keep it for possible later usage.
								nonDoiUrl = url;	// To be un-commented later.. if bestUrl-rules are added.
						}
						if ( bestUrl != null )
							CrawlerController.controller.addSeed(bestUrl);
						else if ( nonDoiUrl != null )	// No bestUrl was found based on our customRules. We will have to use an unknown one, but first look if we can at least avoid the redirect-expensive "doi.org" urls.
							CrawlerController.controller.addSeed(nonDoiUrl);    // Use the 1st one.
						else	// Use the 1st one (unknown case).
							CrawlerController.controller.addSeed(urlList.get(0));
					}
					else	// If there's only one url, add it in the crawler.
						CrawlerController.controller.addSeed(urlList.get(0));    // Canonicalization is performed by Crawler4j itself.
					
					urlList.clear();
				}// end-if-listNotEmpty
			}// end for-loop
        }// end while-loop
	}
	
	
	public static boolean isFinishedLoading(boolean isEmptyOfData, boolean isFirstRun) throws RuntimeException
	{
		if ( isEmptyOfData ) {
			if ( isFirstRun ) {
				logger.error("Could not retrieve any urls from the inputFile!");
				throw new RuntimeException();
			} else {
				logger.debug("Done loading " + FileUtils.getCurrentlyLoadedUrls() + " urls from the inputFile.");	// DEBUG!
				return true;
			}
		} else
			return false;
	}
	
	
	/**
	 * This method checks if the given url is either of unwantedType or if it's a duplicate in the input, while removing the potential jsessionid from the url.
	 * It returns the givenUrl without the jsessionidPart if this url is accepted for connection/crawling, otherwise, it returns "null".
	 * @param retrievedUrl
	 * @param lowerCaseUrl
	 * @return the non-jsessionid-url-string / null for unwanted-duplicate-url
	 */
	public static String handleUrlCheckAtLoading(String retrievedUrl, String lowerCaseUrl)
	{
		if ( matchesUnwantedUrlType(retrievedUrl, lowerCaseUrl) )
			return null;	// The url-logging is happening inside this method (per urlType).
		
		// Remove "jsessionid" for urls. Most of them, if not all, will already be expired.
		if ( lowerCaseUrl.contains("jsessionid") )
			retrievedUrl = UrlUtils.removeJsessionid(retrievedUrl);
		
		// Check if it's a duplicate.
		if ( UrlUtils.duplicateUrls.contains(retrievedUrl) ) {
			logger.debug("Skipping url: \"" + retrievedUrl + "\", at loading, as it has already been seen!");
			UrlUtils.inputDuplicatesNum ++;
			UrlUtils.logTriple(retrievedUrl, "duplicate", "Discarded at loading time, as it's a duplicate.", null);
			return null;
		}
		
		return retrievedUrl;	// The calling method needs the non-jsessionid-string.
	}
	
	
	/**
	 * This method checks if a url is a possibleDocUrl and if so, it first looks if it was found before and if not, it connects to it.
	 * It returns "true" if the given url is a possibleDocUrl and it was handled, otherwise, it returns "false".
	 * @param retrievedUrl
	 * @param lowerCaseUrl
	 * @return
	 * @throws CanonicalizationFailedException
	 */
	public static boolean handleIfPossibleDocUrlAtLoading(String retrievedUrl, String lowerCaseUrl) throws CanonicalizationFailedException
	{
		if ( UrlUtils.DOC_URL_FILTER.matcher(lowerCaseUrl).matches() )	// If it probably a docUrl, check it right away. (This way we avoid the expensive Crawler4j's process)
		{
			//logger.debug("Possible docUrl: " + retrievedUrl);
			
			String urlToCheck = null;
			if ( (urlToCheck = URLCanonicalizer.getCanonicalURL(retrievedUrl, null, StandardCharsets.UTF_8)) == null ) {
				logger.warn("Could not canonicalize url: " + retrievedUrl);
				UrlUtils.logTriple(retrievedUrl, "unreachable", "Discarded at loading time, due to canonicalization problems.", null);
				throw new CanonicalizationFailedException();	// The calling method can decide its move.
			}
			
			if ( UrlUtils.docUrls.contains(urlToCheck) ) {	// If we got into an already-found docUrl, log it and return.
				logger.debug("Re-crossing (before connecting to it) the already found docUrl: \"" +  urlToCheck + "\"");
				if ( FileUtils.shouldDownloadDocFiles )
					UrlUtils.logTriple(urlToCheck, urlToCheck, "This file is probably already downloaded.", null);
				else
					UrlUtils.logTriple(urlToCheck, urlToCheck, "", null);
				return true;	// Return quickly, don't let this already-found docUrl to be connected again.
			}
			
			try {
				HttpUtils.connectAndCheckMimeType(urlToCheck, urlToCheck, null, true, true);    // If it's not a docUrl, it's still added in the crawler but inside this method, in order to add the final-redirected-free url.
			} catch (Exception e) {
				UrlUtils.logTriple(urlToCheck, "unreachable", "Discarded at loading time, due to connectivity problems.", null);
				UrlUtils.connProblematicUrls ++;
			}
			return true;
		} else
			return false;
	}
	
	
	/**
	 * This method takes the "retrievedUrl" from the inputFile and the "lowerCaseUrl" that comes out the retrieved one.
	 * It then checks if the "lowerCaseUrl" matched certain criteria representing the unwanted urls' types. It uses the "retrievedUrl" for proper logging.
	 * If these criteria match, then it logs the url and returns "true", otherwise, it returns "false".
	 * @param lowerCaseUrl
	 * @return true/false
	 */
	public static boolean matchesUnwantedUrlType(String retrievedUrl, String lowerCaseUrl)
	{
		if ( lowerCaseUrl.contains("frontiersin.org") || lowerCaseUrl.contains("tandfonline.com") ) {	// Avoid JavaScript-powered domains, other than the "sciencedirect.com", which is counted separately.
			UrlUtils.javascriptPageUrls++;
			UrlUtils.logTriple(retrievedUrl, "unreachable", "Discarded after matching to a JavaScript-using domain, other than.", null);
			return true;
		}
		else if ( lowerCaseUrl.contains("sciencedirect.com") ) {	// These urls are in JavaScript, having dynamic links which we cannot currently retrieve.
			UrlUtils.sciencedirectUrls ++;
			UrlUtils.logTriple(retrievedUrl, "unreachable", "Discarded after matching to the JavaScript-using domain \"sciencedirect.com\".", null);
			return true;
		}
		else if ( lowerCaseUrl.contains("elsevier.com") ) {	// The plain "elsevier.com" and the "journals.elsevier.com" don't give docUrls.
			// The "linkinghub.elsevier.com" is redirecting to "sciencedirect.com".
			// Note that we still accept the "elsevier.es" pageUrls, which give docUrls.
			UrlUtils.elsevierUnwantedUrls ++;
			UrlUtils.logTriple(retrievedUrl, "unreachable", "Discarded after matching to the unwanted \"elsevier.com\" domain.", null);
			return true;
		}
		else if ( lowerCaseUrl.contains("europepmc.org") || lowerCaseUrl.contains("ncbi.nlm.nih.gov") ) {	// Avoid known-crawler-sensitive domains.
			UrlUtils.crawlerSensitiveDomains ++;
			UrlUtils.logTriple(retrievedUrl, "unreachable", "Discarded after matching to a crawler-sensitive domain.", null);
			return true;
		}
		else if ( lowerCaseUrl.contains("doaj.org/toc/") ) {	// Avoid resultPages.
			UrlUtils.doajResultPageUrls ++;
			UrlUtils.logTriple(retrievedUrl, "unreachable", "Discarded after matching to the Results-directory: \"doaj.org/toc/\".", null);
			return true;
		}
		else if ( lowerCaseUrl.contains("dlib.org") || lowerCaseUrl.contains("saberes.fcecon.unr.edu.ar") ) {    // Avoid HTML docUrls.
			UrlUtils.pagesWithHtmlDocUrls++;
			UrlUtils.logTriple(retrievedUrl, "unreachable", "Discarded after matching to an HTML-docUrls site.", null);
			return true;
		}
		else if ( lowerCaseUrl.contains("rivisteweb.it") || lowerCaseUrl.contains("wur.nl") || lowerCaseUrl.contains("remeri.org.mx")
				|| lowerCaseUrl.contains("cam.ac.uk") || lowerCaseUrl.contains("scindeks.ceon.rs") || lowerCaseUrl.contains("egms.de") ) {	// Avoid pages known to not provide docUrls (just metadata).
			UrlUtils.pagesNotProvidingDocUrls ++;												// Keep "remeri" subDomain of "org.mx", as the TLD is having a lot of different sites.
			UrlUtils.logTriple(retrievedUrl,"unreachable", "Discarded after matching to the non docUrls-providing site \"rivisteweb.it\".", null);
			return true;
		}
		else if ( lowerCaseUrl.contains("bibliotecadigital.uel.br") ) {	// Avoid domains requiring login to access docUrls.
			UrlUtils.pagesRequireLoginToAccessDocFiles++;
			UrlUtils.logTriple(retrievedUrl,"unreachable", "Discarded after matching to a domain which needs login to access docFiles.", null);
			return true;
		}
		else if ( lowerCaseUrl.contains("/view/") || lowerCaseUrl.contains("scielosp.org") || lowerCaseUrl.contains("dk.um.si") ) {	// Avoid crawling pages with larger depth.
			UrlUtils.pagesWithLargerCrawlingDepth ++;
			UrlUtils.logTriple(retrievedUrl,"unreachable", "Discarded after matching to an increasedCrawlingDepth-site.", null);
			return true;
		}
		else if ( lowerCaseUrl.contains("doi.org/https://doi.org/") && lowerCaseUrl.contains("pangaea.") ) {	// PANGAEA. urls with problematic form and non docUrl inner links.
			UrlUtils.pangaeaUrls ++;
			UrlUtils.logTriple(retrievedUrl,"unreachable", "Discarded after matching to \"PANGAEA.\" urls with invalid form and non-docUrls in their inner links.", null);
			return true;
		}
		else if ( lowerCaseUrl.contains("200.17.137.108") ) {	// Known domains with connectivity problems.
			UrlUtils.connProblematicUrls ++;
			UrlUtils.logTriple(retrievedUrl,"unreachable", "Discarded after matching to known urls with connectivity problems.", null);
			return true;
		}
		/*else if ( lowerCaseUrl.contains("handle.net") || lowerCaseUrl.contains("doors.doshisha.ac.jp") || lowerCaseUrl.contains("opac-ir.lib.osaka-kyoiku.ac.jp") ) {	// Slow urls (taking more than 3secs to connect).
			UrlUtils.longToRespondUrls ++;
			UrlUtils.logTriple(retrievedUrl,"unreachable", "Discarded after matching to domain, known to take long to respond.", null);
			return true;
		}*/
		else if ( UrlUtils.DOI_ORG_J_FILTER.matcher(lowerCaseUrl).matches() || UrlUtils.DOI_ORG_PARENTHESIS_FILTER.matcher(lowerCaseUrl).matches() ) {
			UrlUtils.doiOrgToScienceDirect ++;
			UrlUtils.logTriple(retrievedUrl,"unreachable", "Discarded after matching to a urlType of \"doi.org\", which redirects to \"sciencedirect.com\".", null);
			return true;
		}
		else if ( shouldNotAcceptPageUrl(retrievedUrl, lowerCaseUrl) ) {
			UrlUtils.urlsWithUnwantedForm ++;
			UrlUtils.logTriple(retrievedUrl, "unreachable", "Discarded after matching to unwantedType-regex-rules.", null);
			return true;
		}
		else
			return false;
	}
	
	
	/**
	 * This method matches the given pageUrl against general regexes.
	 * It returns "true" if the givenUrl should not be accepted, otherwise, it returns "false".
	 * @param pageUrl
	 * @param lowerCasePageUrl
	 * @return true / false
	 */
	public static boolean shouldNotAcceptPageUrl(String pageUrl, String lowerCasePageUrl)
	{
		String lowerCaseUrl = null;
		
		if ( lowerCasePageUrl == null )
			lowerCaseUrl = pageUrl.toLowerCase();
		else
			lowerCaseUrl = lowerCasePageUrl;	// We might have already done the transformation in the calling method.
		
		return	UrlUtils.PLAIN_DOMAIN_FILTER.matcher(lowerCaseUrl).matches() || UrlUtils.SPECIFIC_DOMAIN_FILTER.matcher(lowerCaseUrl).matches()
				|| UrlUtils.URL_DIRECTORY_FILTER.matcher(lowerCaseUrl).matches() || UrlUtils.PAGE_FILE_EXTENSION_FILTER.matcher(lowerCaseUrl).matches()
				|| UrlUtils.CURRENTLY_UNSUPPORTED_DOC_EXTENSION_FILTER.matcher(lowerCaseUrl).matches();	// TODO - To be removed when these docExtensions get supported.
	}
	

    /**
     * This method logs the outputEntry to be written, as well as the docUrlPath (if non-empty String) and adds entries in the blackList.
	 * @param sourceUrl
	 * @param initialDocUrl
	 * @param comment
	 * @param domain (it may be null)
	 */
    public static void logTriple(String sourceUrl, String initialDocUrl, String comment, String domain)
    {
        String finalDocUrl = initialDocUrl;
		
        if ( !finalDocUrl.equals("unreachable") && !finalDocUrl.equals("duplicate") )	// If we have reached a docUrl..
        {
            // Remove "jsessionid" for urls for "cleaner" output.
			String lowerCaseUrl = finalDocUrl.toLowerCase();
            if ( lowerCaseUrl.contains("jsessionid") )
                finalDocUrl = UrlUtils.removeJsessionid(initialDocUrl);
			
            logger.debug("docUrl found: <" + finalDocUrl + ">");
            if ( FileUtils.shouldDownloadDocFiles && !comment.contains("DocFileNotRetrievedException") )	// If we set to download docFiles, then their fileNames will be in the "comment".
				logger.debug("DocFile: \"" + comment + "\" seems to have been downloaded! Go check it out!");    // DEBUG!
			
			sumOfDocsFound ++;
			
            // Gather data for the MLA, if we decide to have it enabled.
            if ( MachineLearning.useMLA )
				MachineLearning.gatherMLData(domain, sourceUrl, finalDocUrl);
			
            docUrls.add(finalDocUrl);	// Add it here, in order to be able to recognize it and quick-log it later, but also to distinguish it from other duplicates.
        }
        else if ( !finalDocUrl.equals("duplicate") )	{// Else if this url is not a docUrl and has not been processed before..
            duplicateUrls.add(sourceUrl);	 // Add it in duplicates BlackList, in order not to be accessed for 2nd time in the future..
        }	// We don't add docUrls here, as we want them to be separate for checking purposes.
		
        FileUtils.tripleToBeLoggedOutputList.add(new TripleToBeLogged(sourceUrl, finalDocUrl, comment));	// Log it to be written later.
		
        if ( FileUtils.tripleToBeLoggedOutputList.size() == FileUtils.groupCount )	// Write to file every time we have a group of <groupCount> triples.
            FileUtils.writeToFile();
    }


    /**
     * This method takes a url and its mimeType and checks if it's a document mimeType or not.
     * @param urlStr
     * @param mimeType
     * @param contentDisposition
	 * @return boolean
     */
    public static boolean hasDocMimeType(String urlStr, String mimeType, String contentDisposition, HttpURLConnection conn, Page page)
    {
    	if ( mimeType != null )
		{
			if ( mimeType.contains("System.IO.FileInfo") ) {	// Check this out: "http://www.esocialsciences.org/Download/repecDownload.aspx?fname=Document110112009530.6423303.pdf&fcategory=Articles&AId=2279&fref=repec", ιt has: "System.IO.FileInfo".
				// In this case, we want first to try the "Content-Disposition", as it's more trustworthy. If that's not available, use the urlStr as the last resort.
				if ( conn != null )	// If we came here from the "HttpUtils".
					contentDisposition = conn.getHeaderField("Content-Disposition");
				else if ( page != null )	// If we came from the "PageCrawler".
					contentDisposition = PageCrawler.getPageContentDisposition(page);
				// else it will be "null".
				
				if ( contentDisposition != null )
					return	contentDisposition.contains("pdf");	// TODO - add more types as needed.
				else
					return	urlStr.toLowerCase().contains("pdf");
			}
			
			String plainMimeType = mimeType;	// Make sure we don't cause any NPE later on..
			if ( mimeType.contains("charset") )
			{
				plainMimeType = removeCharsetFromMimeType(mimeType);
				
				if ( plainMimeType == null ) {    // If there was any error removing the charset, still try to save any docMimeType (currently pdf-only).
					logger.warn("Url with problematic mimeType was: " + urlStr);
					return	urlStr.toLowerCase().contains("pdf");
				}
			}
			
			if ( knownDocTypes.contains(plainMimeType) )
				return true;
			else
				return	((plainMimeType.equals("application/octet-stream") || plainMimeType.contains("unknown")) && urlStr.toLowerCase().contains("pdf"));
				// This is a special case. (see: "https://kb.iu.edu/d/agtj" for "octet" info.
				// and an example for "unknown" : "http://imagebank.osa.org/getExport.xqy?img=OG0kcC5vZS0yMy0xNy0yMjE0OS1nMDAy&xtype=pdf&article=oe-23-17-22149-g002")
				// TODO - When we will accept more docTypes, match it also against other docTypes, not just "pdf".
		}
		else if ( contentDisposition != null ) {	// If the mimeType was not retrieve, then try the "Content Disposition".
			// TODO - When we will accept more docTypes, match it also against other docTypes instead of just "pdf".
			return	(contentDisposition.contains("attachment") && contentDisposition.contains("pdf"));
		}
		else {
    		logger.warn("No mimeType, nor Content-Disposition, were able to be retrieved for url: " + urlStr);
			return false;
		}
    }
    
	
	/**
	 * This method receives the mimeType and returns it without the "charset" part.
	 * If there is any error, it returns null.
	 * @param mimeType
	 * @return charset-free mimeType
	 */
	public static String removeCharsetFromMimeType(String mimeType)
	{
		String plainMimeType = null;
		Matcher mimeMatcher = null;
		
		try {
			mimeMatcher = MIME_TYPE_FILTER.matcher(mimeType);
		} catch (NullPointerException npe) {	// There should never be an NPE...
			logger.debug("NPE was thrown after calling \"Matcher\" in \"removeCharsetFromMimeType()\" with \"null\" value!");
			return null;
		}
		
		if ( mimeMatcher.matches() ) {
			plainMimeType = mimeMatcher.group(1);
			if ( plainMimeType == null || plainMimeType.isEmpty() ) {
				logger.warn("Unexpected null or empty value returned by \"mimeMatcher.group(1)\" for mimeType: \"" + mimeType + "\".");
				return null;
			}
		} else {
			logger.warn("Unexpected MIME_TYPE_FILTER's (" + mimeMatcher.toString() + ") mismatch for mimeType: \"" + mimeType + "\"");
			return null;
		}
		
		return plainMimeType;
	}
	
	
	/**
	 * This method returns the domain of the given url, in lowerCase (for better comparison).
	 * @param urlStr
	 * @return domainStr
	 */
	public static String getDomainStr(String urlStr)
	{
		String domainStr = null;
		Matcher matcher = null;
		
		try {
			matcher = URL_TRIPLE.matcher(urlStr);
		} catch (NullPointerException npe) {	// There should never be an NPE...
			logger.debug("NPE was thrown after calling \"Matcher\" in \"getDomainStr()\" with \"null\" value!");
			return null;
		}
		
		if ( matcher.matches() ) {
			domainStr = matcher.group(2);	// Group <2> is the DOMAIN.
			if ( (domainStr == null) || domainStr.isEmpty() ) {
				logger.warn("Unexpected null or empty value returned by \"matcher.group(2)\" for url: \"" + urlStr + "\".");
				return null;
			}
		} else {
			logger.warn("Unexpected URL_TRIPLE's (" + matcher.toString() + ") mismatch for url: \"" + urlStr + "\"");
			return null;
		}
		
		return domainStr.toLowerCase();	// We return it in lowerCase as we don't want to store double domains. (it doesn't play any part in connectivity, only the rest of the url is case-sensitive.)
	}


	/**
	 * This method returns the path of the given url.
	 * @param urlStr
	 * @return pathStr
	 */
	public static String getPathStr(String urlStr)
	{
		String pathStr = null;
		Matcher matcher = null;
		
		try {
			matcher = URL_TRIPLE.matcher(urlStr);
		} catch (NullPointerException npe) {	// There should never be an NPE...
			logger.debug("NPE was thrown after calling \"Matcher\" in \"getPathStr()\" with \"null\" value!");
			return null;
		}
		
		if ( matcher.matches() ) {
			pathStr = matcher.group(1);	// Group <1> is the PATH.
			if ( (pathStr == null) || pathStr.isEmpty() ) {
				logger.warn("Unexpected null or empty value returned by \"matcher.group(1)\" for url: \"" + urlStr + "\".");
				return null;
			}
		} else {
			logger.warn("Unexpected URL_TRIPLE's (" + matcher.toString() + ") mismatch for url: \"" + urlStr + "\"");
			return null;
		}
		
		return pathStr;
	}
	
	
	
	/**
	 * This method returns the path of the given url.
	 * @param urlStr
	 * @return pathStr
	 */
	public static String getDocIdStr(String urlStr)
	{
		String docIdStr = null;
		Matcher matcher = null;
		
		try {
			matcher = URL_TRIPLE.matcher(urlStr);
		} catch (NullPointerException npe) {	// There should never be an NPE...
			logger.debug("NPE was thrown after calling \"Matcher\" in \"getDocIdStr\" with \"null\" value!");
			return null;
		}
		
		if ( matcher.matches() ) {
			docIdStr = matcher.group(3);	// Group <3> is the docId.
			if ( (docIdStr == null) || docIdStr.isEmpty() ) {
				logger.warn("Unexpected null or empty value returned by \"matcher.group(3)\" for url: \"" + urlStr + "\".");
				return null;
			}
		}
		else {
			logger.warn("Unexpected URL_TRIPLE's (" + matcher.toString() + ") mismatch for url: \"" + urlStr + "\"");
			return null;
		}
		
		return docIdStr;
	}
	
	
	/**
	 * This method is responsible for removing the "jsessionid" part of a url.
	 * If no jsessionId is found, then it returns the string it recieved.
	 * @param urlStr
	 * @return urlWithoutJsessionId
	 */
	public static String removeJsessionid(String urlStr)
	{
		String finalUrl = urlStr;
		
		String preJsessionidStr = null;
		String afterJsessionidStr = null;
		
		Matcher jsessionidMatcher = JSESSIONID_FILTER.matcher(urlStr);
		if (jsessionidMatcher.matches())
		{
			preJsessionidStr = jsessionidMatcher.group(1);	// Take only the 1st part of the urlStr, without the jsessionid.
		    if ( (preJsessionidStr == null) || preJsessionidStr.isEmpty() ) {
		    	logger.warn("Unexpected null or empty value returned by \"jsessionidMatcher.group(1)\" for url: \"" + urlStr + "\"");
		    	return finalUrl;
		    }
		    finalUrl = preJsessionidStr;
		    
		    afterJsessionidStr = jsessionidMatcher.group(2);
			if ( (afterJsessionidStr == null) || afterJsessionidStr.isEmpty() )
				return finalUrl;
			else
				return finalUrl + afterJsessionidStr;
		}
		else
			return finalUrl;
	}

}
