package eu.openaire.doc_urls_retriever.util.url;

import java.util.Collection;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.uci.ics.crawler4j.url.URLCanonicalizer;
import eu.openaire.doc_urls_retriever.crawler.MachineLearning;

import eu.openaire.doc_urls_retriever.crawler.CrawlerController;
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
	
	public final static Pattern URL_TRIPLE = Pattern.compile("(.+:\\/\\/(?:www(?:(?:\\w+)?\\.)?)?([\\w\\.\\-]+)(?:[\\:\\d]+)?(?:.*\\/)?(?:[\\w\\.\\-\\_\\%\\:\\~]*\\?[\\w\\.\\-\\_\\%\\:\\~]+\\=)?)(.+)?");
	// URL_TRIPLE regex to group domain, path and ID --> group <1> is the regular PATH, group<2> is the DOMAIN and group <3> is the regular "ID".
	
	public static final Pattern URL_DIRECTORY_FILTER =
			Pattern.compile(".+\\/(?:profile|login|join|subscr|register|submit|post|import|bookmark|announcement|rss|feed|about|faq|wiki|support|sitemap|license|disclaimer|policies|policy|privacy|terms|account|help|law"
							+ "|user|author|editor|citation|external|statistics|application|permission|ethic|contact|survey|wallet|contribute|template|logo|image|photo|advertiser|people"
							+ "|error|misuse|abuse|gateway|sorryserver|notfound|404\\.(?:\\w)?htm).*");
	// We check them as a directory to avoid discarding publications's urls about these subjects.
	
	public static final Pattern PAGE_FILE_EXTENSION_FILTER = Pattern.compile(".+\\.(?:ico|css|js|gif|jpg|jpeg|png|wav|mp3|mp4|webm|mkv|mov|pt|mso|dtl|svg|txt|c|cc|cxx|cpp|java|py)(?:\\?.+)?$");
	
    public static final Pattern INNER_LINKS_FILE_EXTENSION_FILTER = Pattern.compile(".+\\.(?:ico|css|js|gif|jpg|jpeg|png|wav|mp3|mp4|webm|mkv|mov|pt|xml|mso|dtl|svg|do|txt|c|cc|cxx|cpp|java|py)(?:\\?.+)?$");
    // Here don't include .php and relative extensions, since even this can be a docUrl. For example: https://www.dovepress.com/getfile.php?fileID=5337
	// So, we make a new REGEX for these extensions, this time, without a potential argument in the end (?id=XXX..)
	public static final Pattern PLAIN_PAGE_EXTENSION_FILTER = Pattern.compile(".+\\.(?:php|php2|php3|php4|php5|phtml|htm|html|shtml|xht|xhtm|xhtml|xml|aspx|asp|jsp|do)$");
	
	public static final Pattern INNER_LINKS_FILE_FORMAT_FILTER = Pattern.compile(".+format=(?:xml|htm|html|shtml|xht|xhtm|xhtml).*");
    
    public static final Pattern SPECIFIC_DOMAIN_FILTER = Pattern.compile(".+:\\/\\/.*(?:google|goo.gl|gstatic|facebook|twitter|youtube|linkedin|wordpress|s.w.org|ebay|bing|amazon|wikipedia|myspace|yahoo|mail|pinterest|reddit|blog|tumblr"
																					+ "|evernote|skype|microsoft|adobe|buffer|digg|stumbleupon|addthis|delicious|dailymotion|gostats|blogger|copyright|friendfeed|newsvine|telegram|getpocket"
																					+ "|flipboard|instapaper|line.me|telegram|vk|ok.rudouban|baidu|qzone|xing|renren|weibo|doubleclick).*\\/.*");
    
    public static final Pattern PLAIN_DOMAIN_FILTER = Pattern.compile(".+:\\/\\/[\\w.:-]+(?:\\/)?$");	// Exclude plain domains' urls.
	
    public static final Pattern JSESSIONID_FILTER = Pattern.compile("(.+:\\/\\/.+)(?:\\;(?:JSESSIONID|jsessionid)=.+)(\\?.+)");
	
    public static final Pattern DOC_URL_FILTER = Pattern.compile(".+:\\/\\/.+(pdf|download|/doc|document|(?:/|[?]|&)file|/fulltext|attachment|/paper|viewfile|viewdoc|/get|cgi/viewcontent.cgi?).*");
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
	public static int frontiersinUrls = 0;
	public static int sciencedirectUrls = 0;
	public static int elsevierUnwantedUrls = 0;
	public static int crawlerSensitiveDomains = 0;
	public static int doajResultPageUrls = 0;
	public static int pageWithHtmlDocUrls = 0;
	public static int pagesWithLargerCrawlingDepth = 0;	// Pages with their docUrl behind an inner "view" page.
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
		
		boolean firstRun = true;
		
		// Start loading and checking urls.
        while ( true )
        {
        	loadedUrlGroup = FileUtils.getNextUrlGroupFromJson(); // Take urls from jsonFile.
			
			//loadedUrlGroup = FileUtils.getNextUrlGroupTest();	// Take urls from single-columned (testing) csvFile.
			
	        if ( loadedUrlGroup.isEmpty() ) {
	        	if ( firstRun ) {
	        		logger.error("Could not retrieve any urls from the inputFile!");
	        		throw new RuntimeException();
	        	}
	        	else {
	        		logger.debug("Done loading " + FileUtils.getCurrentlyLoadedUrls() + " urls from the inputFile.");	// DEBUG!
	        		break;	// No more urls to load and check, initialize M.L.A. (if wanted) and start Crawling.
	        	}
	        }
			
			firstRun = false;
	        
			for ( String retrievedUrl : loadedUrlGroup )
			{
				String lowerCaseUrl = retrievedUrl.toLowerCase();	// Only for string checking purposes, not supposed to reach any connection.
				
				if ( matchesUnwantedUrlType(retrievedUrl, lowerCaseUrl) )
					continue;
				
				// Remove "jsessionid" for urls. Most of them, if not all, will already be expired.
				if ( lowerCaseUrl.contains("jsessionid") )
					retrievedUrl = UrlUtils.removeJsessionid(retrievedUrl);
				
				// Check if it's a duplicate. (if already found before inside or outside the Crawler4j).
	        	if ( UrlUtils.duplicateUrls.contains(retrievedUrl) ) {
	        		logger.debug("Skipping url: \"" + retrievedUrl + "\", at loading, as it has already been seen!");
	        		UrlUtils.inputDuplicatesNum ++;
	        		UrlUtils.logTriple(retrievedUrl, "duplicate", "Discarded at loading time, as it's a duplicate.", null);
	        		continue;
	        	}
	        	
	        	if ( UrlUtils.DOC_URL_FILTER.matcher(lowerCaseUrl).matches() )	// If it probably a docUrl, check it right away. (This way we avoid the expensive Crawler4j's process)
	        	{
	        		//logger.debug("Possible docUrl at loading: " + retrievedUrl);
	        		
	        		String urlToCheck = null;
					if ( (urlToCheck = URLCanonicalizer.getCanonicalURL(retrievedUrl, null)) == null ) {
						logger.warn("Could not canonicalize url: " + retrievedUrl);
						UrlUtils.logTriple(retrievedUrl, "unreachable", "Discarded at loading time, due to canonicalization problems.", null);
						continue;
					}
					
	        		try {
						HttpUtils.connectAndCheckMimeType(urlToCheck, urlToCheck, null, true);    // If it's not a docUrl, it's still added in the crawler but inside this method, in order to add the final-redirected-free url.
					} catch (Exception e) {
						UrlUtils.logTriple(urlToCheck, "unreachable", "Discarded at loading time, due to connectivity problems.", null);
						UrlUtils.connProblematicUrls ++;
					}
				}
	   			else
					CrawlerController.controller.addSeed(retrievedUrl);	// If this is not a valid url, Crawler4j will throw it away by itself.
				
			}// end for-loop
        }// end while-loop
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
		if ( lowerCaseUrl.contains("frontiersin.org") ) {
			UrlUtils.frontiersinUrls ++;
			UrlUtils.logTriple(retrievedUrl, "unreachable", "Discarded after matching to the JavaScript-using domain \"frontiersin.org\".", null);
			return true;
		}
		else if ( lowerCaseUrl.contains("sciencedirect.com") ) {	// These urls are in JavaScript, having dynamic links which we cannot currently retrieve.
			UrlUtils.sciencedirectUrls ++;
			UrlUtils.logTriple(retrievedUrl, "unreachable", "Discarded after matching to the JavaScript-using domain \"sciencedirect.com\".", null);
			return true;
		}
		else if ( lowerCaseUrl.contains("elsevier.com") ) {	// The plain "elsevier.com" and the "journals.elsevier.com" don't give docUrls.
			// The "linkinghub.elsevier.com" is redirecting to "sciencedirect.com".
			// Note that we still accept the "elsevier.es" urls, which give docUrls.
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
			UrlUtils.pageWithHtmlDocUrls++;
			UrlUtils.logTriple(retrievedUrl, "unreachable", "Discarded after matching to an HTML-docUrls site.", null);
			return true;
		}
		else if ( lowerCaseUrl.contains("rivisteweb.it") || lowerCaseUrl.contains("library.wur.nl") || lowerCaseUrl.contains("remeri.org.mx") ) {	// Avoid pages known to not provide docUrls (just metadata).
			UrlUtils.pagesNotProvidingDocUrls ++;
			UrlUtils.logTriple(retrievedUrl,"unreachable", "Discarded after matching to the non docUrls-providing site \"rivisteweb.it\".", null);
			return true;
		}
		else if ( lowerCaseUrl.contains("/view/") )	// Avoid crawling pages with larger depth.
		{
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
		else if ( UrlUtils.DOI_ORG_J_FILTER.matcher(lowerCaseUrl).matches() || UrlUtils.DOI_ORG_PARENTHESIS_FILTER.matcher(lowerCaseUrl).matches() ) {
			UrlUtils.doiOrgToScienceDirect ++;
			UrlUtils.logTriple(retrievedUrl,"unreachable", "Discarded after matching to a urlType of \"doi.org\", which redirects to \"sciencedirect.com\".", null);
			return true;
		}
		else if ( shouldNotAcceptPageUrl(retrievedUrl, lowerCaseUrl) )
		{
			UrlUtils.urlsWithUnwantedForm ++;
			UrlUtils.logTriple(retrievedUrl, "unreachable", "Discarded after matching to unwantedType-regex-rules.", null);
			return true;
		}
		else
			return false;
	}
	
	
	/**
	 * This method matches the given pageUrl against general regexes.
	 * It returns true
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
		
		if ( UrlUtils.PLAIN_DOMAIN_FILTER.matcher(lowerCaseUrl).matches() || UrlUtils.SPECIFIC_DOMAIN_FILTER.matcher(lowerCaseUrl).matches()
				|| UrlUtils.URL_DIRECTORY_FILTER.matcher(lowerCaseUrl).matches() || UrlUtils.PAGE_FILE_EXTENSION_FILTER.matcher(lowerCaseUrl).matches() )
			return true;
		else
			return false;
	}
	

    /**
     * This method logs the outputEntry to be written, as well as the docUrlPath (if non-empty String) and adds entries in the blackList.
	 * @param sourceUrl
	 * @param initialDocUrl
	 * @param errorCause
	 * @param domain
	 */
    public static void logTriple(String sourceUrl, String initialDocUrl, String errorCause, String domain)
    {
        String finalDocUrl = initialDocUrl;
		
        if ( !finalDocUrl.equals("unreachable") && !finalDocUrl.equals("duplicate") )	// If we have reached a docUrl..
        {
            // Remove "jsessionid" for urls for "cleaner" output.
			String lowerCaseUrl = finalDocUrl.toLowerCase();
            if ( lowerCaseUrl.contains("jsessionid") )
                finalDocUrl = UrlUtils.removeJsessionid(initialDocUrl);
			
            logger.debug("docUrl found: <" + finalDocUrl + ">");
            
            sumOfDocsFound ++;
			
            // Gather data for the MLA, if we decide to have it enabled.
            if ( MachineLearning.useMLA )
				MachineLearning.gatherMLData(domain, sourceUrl, finalDocUrl);
			
            docUrls.add(finalDocUrl);	// Add it here, in order to be able to recognize it and quick-log it later, but also to distinguish it from other duplicates.
        }
        else if ( !finalDocUrl.equals("duplicate") )	{// Else if this url is not a docUrl and has not been processed before..
            duplicateUrls.add(sourceUrl);	 // Add it in duplicates BlackList, in order not to be accessed for 2nd time in the future..
        }	// We don't add docUrls here, as we want them to be separate for checking purposes.
		
        //logger.debug("docUrl received in \"UrlUtils.logUrl()\": "+  docUrl);	// DEBUG!
		
        FileUtils.tripleToBeLoggedOutputList.add(new TripleToBeLogged(sourceUrl, finalDocUrl, errorCause));	// Log it to be written later.
		
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
    public static boolean hasDocMimeType(String urlStr, String mimeType, String contentDisposition)
    {
    	if ( mimeType != null )
		{
			String plainMimeType = mimeType;	// Make sure we don't cause any NPE later on..
			if ( mimeType.contains("charset") )
			{
				plainMimeType = removeCharsetFromMimeType(mimeType);
				
				if ( plainMimeType == null ) {    // If there was any error removing the charset, still try to save any docMimeType (currently pdf-only).
					logger.warn("Url with problematic mimeType was: " + urlStr);
					if ( mimeType.contains("pdf") )
						return true;
					else
						return false;
				}
			}
			
			if ( knownDocTypes.contains(plainMimeType) )
				return true;
			else if ( plainMimeType.equals("application/octet-stream") && urlStr.toLowerCase().contains("pdf") )
				// This is a special case. (see: "https://kb.iu.edu/d/agtj")
				// TODO - When we will accept more docTypes, match it also against other docTypes instead of just "pdf".
				return true;
			else
				return false;
		}
		else if ( contentDisposition != null )	// If the mimeType was not retrieve, thn try the "Content Disposition".
		{
			if ( contentDisposition.contains("attachment") && contentDisposition.contains("pdf") )
				// TODO - When we will accept more docTypes, match it also against other docTypes instead of just "pdf".
				return true;
			else
				return false;
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
		
		Matcher mimeMatcher = UrlUtils.MIME_TYPE_FILTER.matcher(mimeType);
		if ( mimeMatcher.matches() )
		{
			plainMimeType = mimeMatcher.group(1);
			if ( plainMimeType == null || plainMimeType.isEmpty() ) {
				logger.warn("Unexpected null or empty value returned by \"mimeMatcher.group(1)\" for mimeType: \"" + mimeType + "\".");
				return null;
			}
		}
		else {
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
		
		if ( matcher.matches() )
		{
			domainStr = matcher.group(2);	// Group <2> is the DOMAIN.
			if ( (domainStr == null) || domainStr.isEmpty() ) {
				logger.warn("Unexpected null or empty value returned by \"matcher.group(2)\" for url: \"" + urlStr + "\".");
				return null;
			}
		}
		else {
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
		
		if ( matcher.matches() )
		{
			pathStr = matcher.group(1);	// Group <1> is the PATH.
			if ( (pathStr == null) || pathStr.isEmpty() ) {
				logger.warn("Unexpected null or empty value returned by \"matcher.group(1)\" for url: \"" + urlStr + "\".");
				return null;
			}
		}
		else {
			logger.warn("Unexpected URL_TRIPLE's (" + matcher.toString() + ") mismatch for url: \"" + urlStr + "\"");
			return null;
		}
		
		return pathStr;
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
		
		String prejsessionidStr = null;
		String afterJsessionidStr = null;
		
		Matcher jsessionIdMatcher = JSESSIONID_FILTER.matcher(urlStr);
		if (jsessionIdMatcher.matches())
		{
			prejsessionidStr = jsessionIdMatcher.group(1);	// Take only the 1st part of the urlStr, without the jsessionid.
		    if ( (prejsessionidStr == null) || prejsessionidStr.isEmpty() ) {
		    	logger.warn("Unexpected null or empty value returned by \"jsessionIdMatcher.group(1)\" for url: \"" + urlStr + "\"");
		    	return finalUrl;
		    }
		    finalUrl = prejsessionidStr;
		    
		    afterJsessionidStr = jsessionIdMatcher.group(2);
			if ( (afterJsessionidStr == null) || afterJsessionidStr.isEmpty() )
				return finalUrl;
			else
				return finalUrl + afterJsessionidStr;
		}
		else
			return finalUrl;
	}

}
