package eu.openaire.doc_urls_retriever.util.url;

import java.net.HttpURLConnection;
import java.util.Collection;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;

import edu.uci.ics.crawler4j.url.URLCanonicalizer;
import eu.openaire.doc_urls_retriever.crawler.CrawlerController;
import eu.openaire.doc_urls_retriever.util.file.FileUtils;
import eu.openaire.doc_urls_retriever.util.http.HttpUtils;


public class UrlUtils
{
	private static final Logger logger = LogManager.getLogger(UrlUtils.class);
	
	public final static Pattern URL_TRIPLE = Pattern.compile("(.+:\\/\\/(?:www(?:(?:\\w+)?\\.)?)?([\\w\\.\\-]+)(?:[\\:\\d]+)?\\/(?:.+\\/)?(?:[\\w.-]*[^\\.pdf]\\?[\\w.-]+[^site]=)?)(.+)?");
	// URL_TRIPLE regex to group domain, path and ID --> group <1> is the regular PATH, group<2> is the DOMAIN and group <3> is the regular "ID".
	
	public static final Pattern URL_DIRECTORY_FILTER = Pattern.compile(".+\\/(?:login|join|subscr|register|announcement|feed|about|citation|faq|wiki|support|error|notfound|contribute|subscription|advertisers|authors|license|disclaimer"
																	+ "|policies|policy|privacy|terms|sitemap|account|search|statistics|cookie|help|law|contact|survey|wallet|template|logo|image|photo).*");
	// We check them as a directory to avoid discarding publications's urls about these subjects.
	
	//public static final Pattern PAGE_EXTENSION_FILTER = Pattern.compile(".+\\.(?:php|php2|php3|php4|php5|phtml|htm|html|shtml|xht|xhtm|xhtml|xml|aspx|asp|jsp)(?:\\?.+=.+)?$");
	
	public static final Pattern PAGE_FILE_EXTENSION_FILTER = Pattern.compile(".+\\.(?:ico|css|js|gif|jpg|jpeg|png|wav|mp3|mp4|webm|mkv|pt|mso|dtl)(?:\\?.+=.+)?$");
	
    public static final Pattern INNER_LINKS_FILE_EXTENSION_FILTER = Pattern.compile(".+:\\/\\/.+\\.(?:ico|css|js|gif|jpg|jpeg|png|wav|mp3|mp4|webm|mkv|pt|"
    																							+ "php|php2|php3|php4|php5|phtml|htm|html|shtml|xht|xhtm|xhtml|xml|aspx|asp|jsp|mso|dtl)(?:\\?.+=.+)?$");
    
    public static final Pattern INNER_LINKS_FILE_FORMAT_FILTER = Pattern.compile(".+:\\/\\/.+format=(?:xml|htm|html|shtml|xht|xhtm|xhtml).*");
    
    public static final Pattern SPECIFIC_DOMAIN_FILTER = Pattern.compile(".+:\\/\\/.*(?:google|goo.gl|gstatic|facebook|twitter|youtube|linkedin|wordpress|s.w.org|ebay|bing|amazon|wikipedia|myspace|yahoo|mail|pinterest|reddit|blog|tumblr|evernote|skype|microsoft|adobe|buffer"
    																				+ "|digg|stumbleupon|addthis|delicious|dailymotion|gostats|blogger|friendfeed|newsvine|telegram|getpocket|flipboard|instapaper|line.me|telegram|vk|ok.ru"
    																				+ "|douban|baidu|qzone|xing|renren|weibo).*\\/.*");
    
    public static final Pattern PLAIN_DOMAIN_FILTER = Pattern.compile(".+:\\/\\/[\\w.:-]+(?:\\/)?$");	// Exclude plain domains' urls.
    public static final Pattern PLAIN_FIRST_LEVEL_DIRECTORY_FILTER = Pattern.compile(".+:\\/\\/[\\w\\.\\:\\-]+\\/[\\w\\-\\.]+(?:\\/)?$");	// Exclude plain first level directories.
    
    public static final Pattern JSESSIONID_FILTER = Pattern.compile(".+:\\/\\/.+(;(?:JSESSIONID|jsessionid)=.*[?\\w\\W]+$)");
    public static final Pattern ENDING_WITH_GENERAL_EXTENSION_FILTER = Pattern.compile(".+:\\/\\/.+(?:\\.[\\w]+)$");
    
    //public static final Pattern ENDING_WITH_DOC_EXTENSION_FILTER = Pattern.compile(".+(?:\\.(?:pdf))(?:\\?.+=.+)?$");	// TODO - Add more doc extensions when decided.
    
    //public static final Pattern STAT_REPORT_FILTER = Pattern.compile(".+(statistic[s]*[al]*[\\-\\_]*report[s]*).*");
    
    public static final Pattern DOC_URL_FILTER = Pattern.compile(".+:\\/\\/.+(pdf|download|doc|file|fulltext|attachment|paper|cgi/viewcontent.cgi?|viewfile|viewdoc|/get).*");
    // Works for lowerCase Strings (we make sure they are in lowerCase before we check).
    // Note that we still need to check  if it's an alive link and if it's actually a docUrl (though it's mimeType).
    
    public static int sumOfDocsFound = 0;	// Change it back to simple int if finally in singleThread mode
	public static long inputDuplicatesNum = 0;
	
	public static HashSet<String> duplicateUrls = new HashSet<String>();
	public static SetMultimap<String, String> successDomainPathsMultiMap = HashMultimap.create();	// Holds multiple values for any key, if a domain(key) has many different paths (values) for doc links.
	private static HashSet<String> loadedUrlGroup = new HashSet<String>();
	public static HashSet<String> docUrls = new HashSet<String>();
	public static HashSet<String> knownDocTypes = new HashSet<String>();
	
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
		boolean firstRun = true;
		
		// Start loading and checking urls.
        while ( true )
        {
			// Take urls from file.
        	loadedUrlGroup = FileUtils.getNextUrlGroup();
	        if ( loadedUrlGroup.isEmpty() ) {
	        	if ( firstRun ) {
	        		logger.fatal("Could not retrieve any urls from the inputFile!");
	        		throw new RuntimeException();
	        	}
	        	else {
	        		logger.debug("Done loading urls from the inputFile.");	// DEBUG!
	        		break;	// No more urls to load and check, just start Crawling.
	        	}
	        }

			firstRun = false;
	        String canonicalizedUrl = null;
	        
			for ( String retrievedUrl : loadedUrlGroup )
			{
				String lowerCaseUrl = retrievedUrl.toLowerCase();	// Only for string checking purposes, not supposed to reach any connection.
				
				// Remove "jsessionid" for urls. Most of them, if not all, will already be expired.
				if ( lowerCaseUrl.contains("jsessionid") ) {
					String noJsessinId;
					if ( (noJsessinId = UrlUtils.removeJsessionId(retrievedUrl)) != null )	// If it returns null we will NOT loose the value of "retrievedUrl".
						retrievedUrl = noJsessinId;
				}
				
				if ( docUrls.contains(retrievedUrl) ) {	// If it's already a docUrl that we have come across before, log it and continue.
					logUrl(retrievedUrl, retrievedUrl);
					logger.debug("Re-crossing the already found url: \"" + retrievedUrl + "\"");
				}
				
				// Check if it's a duplicate. (if already found before inside or outside the Crawler).
	        	if ( UrlUtils.duplicateUrls.contains(retrievedUrl) )
	        	{
	        		logger.debug("Skipping url: \"" + retrievedUrl + "\" as it has been already seen!");	// DEBUG!
	        		UrlUtils.inputDuplicatesNum ++;
	        		UrlUtils.logUrl(retrievedUrl, "duplicate");
	        		continue;
	        	}
	        	
	        	// If this url is of a certain unwanted type, blacklist itand move on.
				if ( UrlUtils.PAGE_FILE_EXTENSION_FILTER.matcher(lowerCaseUrl).matches() || UrlUtils.SPECIFIC_DOMAIN_FILTER.matcher(lowerCaseUrl).matches() || UrlUtils.URL_DIRECTORY_FILTER.matcher(lowerCaseUrl).matches() )
				{
					UrlUtils.logUrl(retrievedUrl, "unreachable");
					continue;	// If this link matches certain blackListed criteria, move on..
				}
	        	
				if ( (canonicalizedUrl = URLCanonicalizer.getCanonicalURL(retrievedUrl) ) == null ) {	// Fix potential encoding problems.
					logger.debug("Could not cannonicalize url: " + retrievedUrl);
					UrlUtils.logUrl(retrievedUrl, "unreachable");	// We log the "retrieved", so that we have the initial url that we retrieved from the inputFile.
					continue;	// Could not canonicalize this url! Move on..
				}
				
	    		// Check if it's already a docURL.
	        	try {
	        		UrlUtils.checkIfDocMimeType(canonicalizedUrl, canonicalizedUrl, null, true);
	        		// Note that if it's "true", then we have already add it in the outPut links inside "checkIfDocMimeType()".
	        		// And if it's false, the we have added it in the Crawler inside the "checkIfDocMimeType()", to make sure we don't repeat the redirects.
	        	} catch (RuntimeException e) {
	        		UrlUtils.logUrl(retrievedUrl, "unreachable");	// If there was a connection problem, log it as "unreachable".
	        	}
			}// end for-loop
        }// end while-loop
	}
	
	
	/**
	 * This method uses previous success cases to predict the docUrl of a page, if this page gives us the ID of the document.
	 * The idea is that we might get a url which shows info about the publication and as the same ID with the wanted docUrl, but ut just happens to be in a different directory (path).
	 * So, before going and checking each and every one of the inner links, we should check if by using known paths that gave docUrls before (for the current spesific domain), we are able to take the docUrl immediately.
	 * Disclaimer: This is still in experimental stage.
	 * @param pageUrl
	 * @return true / false
	 * @throws RuntimeException
	 */
	public static boolean guessInnerDocUrl(String pageUrl)
	{
		Collection<String> paths;
		StringBuilder strB = new StringBuilder(150);
		String guessedDocUrl = null;
		String domainStr = null;
		String docIdStr = null;
		
		Matcher matcher = URL_TRIPLE.matcher(pageUrl);
		if ( matcher.matches() ) {
		    domainStr = matcher.group(2);	// Group <2> is the DOMAIN.
		    if ( (domainStr == null) || domainStr.isEmpty() ) {
		    	logger.error("Unexpected null or empty value returned by \"URL_TRIPLE.group(2)\"");
		    	return false;	// We don't add the empty string here, as we are used to handle this in the calling classes.
		    }
		}
		else {
			logger.warn("Unexpected matcher's (" + matcher.toString() + ") mismatch for url: \"" + pageUrl + "\"");
			return false;	// We don't add the empty string here, as we are used to handle this in the calling classes.
		}
		
		if ( successDomainPathsMultiMap.containsKey(domainStr.toLowerCase()) )	// If this domain is already logged go check for previous succesfull paths, if not logged, then return..
		{
			docIdStr = matcher.group(3);	// group <3> is the "ID".
			if ( (docIdStr == null) || docIdStr.isEmpty() ) {
		    	logger.debug("No available ID information in url: \"" + pageUrl + "\"");
		    	return false;	// The docUrl can't be guessed in this case.
		    }
			
			paths = successDomainPathsMultiMap.get(domainStr);	// Get all available paths for this domain.
			
			for ( String path : paths )
			{
				// For every available docPath for this domain construct the expected docLink..
				strB.append(path);

				strB.append(docIdStr);
				strB.append(".pdf");

				guessedDocUrl = strB.toString();
				
		    	if ( UrlUtils.docUrls.contains(guessedDocUrl) ) {	// If we got into an already-found docUrl, log it and return true.
		    		UrlUtils.logUrl(pageUrl, guessedDocUrl);
		    		logger.debug("MachineLearningAlgorithm got a hit for: \""+ pageUrl + "\". Resulted docUrl was: \"" + guessedDocUrl + "\"" );	// DEBUG!
		    		return true;
		    	}
				
				// Check if it's a truly-alive docUrl.
				try {
					logger.debug("Going to check guessedDocUrl: " + guessedDocUrl +"\", made out from initialUrl: \"" + pageUrl + "\"");
					
					if ( checkIfDocMimeType(pageUrl, guessedDocUrl, domainStr, false) ) {
						logger.debug("MachineLearningAlgorithm got a hit for: \""+ pageUrl + "\". Resulted docUrl was: \"" + guessedDocUrl + "\"" );	// DEBUG!
						// TODO - Maybe it will be interesting also to count (with an AtomicInteger, if in multithread) the handled urls with this algorithm.
						return true;	// Note that we have already add it in the outPut links inside "checkIfDocMimeType()".
					}
				} catch (Exception e) {
					// No special handling here, neither logging.. since it's expected that some checks will fail.
				} finally {
					strB.setLength(0);	// Clear the buffer before going to check the next path.
				}
			}// end for-loop
		}// end if
		
		return false;	// We can't find its docUrl.. so we return false and continue with crawling this page.
	}
	
	
	/**
	 * This method logs the ouputEntry to be written, as well as the docUrlPath (if non-empty String) and adds entries in the blackList.
	 * @param sourceUrl
	 * @param initialDocUrl
	 */
	public static void logUrl(String sourceUrl, String initialDocUrl)
	{
		String finalDocUrl = initialDocUrl;
		
		if ( !finalDocUrl.equals("unreachable") && !finalDocUrl.equals("duplicate") )	// If we have reached a docUrl..
		{
			// Remove "jsessionid" for urls for "cleanner" output.
			if ( finalDocUrl.contains("jsessionid") || finalDocUrl.contains("JSESSIONID") )
				if ( (finalDocUrl = UrlUtils.removeJsessionId(initialDocUrl)) == null )	// If there is problem removing the "jsessionid" and it return "null", reassign the initial value.
					finalDocUrl = initialDocUrl;
			
        	logger.debug("docUrl found: <" + finalDocUrl + ">");
        	sumOfDocsFound ++;
			
			// Get its domain and path and put it inside "successDomainPathsMultiMap".
			String domainStr = null;
			String pathStr = null;
			
			Matcher matcher = URL_TRIPLE.matcher(finalDocUrl);
			
			if ( matcher.matches() )
			{
			    domainStr = matcher.group(2);	// Group <2> is the DOMAIN.
			    if ( (domainStr == null) || domainStr.isEmpty() ) {
			    	logger.warn("Unexpected null or empty value returned by \"URL_TRIPLE.group(2)\"");
			    	return;
			    }
			    
				pathStr = matcher.group(1);	// group <1> is the PATH.
			    if ( (pathStr == null) || pathStr.isEmpty() ) {
			    	logger.warn("Unexpected null or empty value returned by \"URL_TRIPLE.group(1)\"");
			    	return;
			    }
				
			    successDomainPathsMultiMap.put(domainStr.toLowerCase(), pathStr);	// Add this pair in "successDomainPathsMultiMap", if the key already exists then it will just add one more value to that key.
			}
			else {
				logger.warn("Unexpected matcher's (" + matcher.toString() + ") mismatch for url: \"" + finalDocUrl + "\"");
				return;
			}
			
			docUrls.add(finalDocUrl);	// Add it here, in order to be able to recognize it and quick-log it later, but also to distinguish it from other duplicates.
			
		}
		else if ( !finalDocUrl.equals("duplicate") )	{// Else if this url is not a docUrl and has not been processed before..
			duplicateUrls.add(sourceUrl);	 // Add it in duplicates BlackList, in order not to be accessed for 2nd time in the future..
		}	// We don't add docUrls here, as we want them to be separate for checking purposes/
		
		//logger.debug("docUrl received in logUrl() : "+  docUrl);	// DEBUG!
		
		FileUtils.outputEntries.put(sourceUrl, initialDocUrl);	// Log it to be written later.
		
		if ( FileUtils.outputEntries.size() == FileUtils.groupCount )	// Write to file every time we have a group of <groupCount> urls' sets.
			FileUtils.writeToFile();
	}
	
	
	/**
	 * This method checks if a certain url can give us its mimeType, as well as if this mimeType is a docMimeType.
	 * It automatically calls the "logUrl()" method for the valid docUrls, while it doesn't call it for non-success cases, thus allowing calling method to handle the case.
	 * @param currentPage
	 * @param resourceURL
	 * @return True, if it's a pdfMimeType. False, if it has a different mimeType.
	 * @throws RuntimeException (when there was a network error).
	 */
	public static boolean checkIfDocMimeType(String currentPage, String resourceURL, String domainStr, boolean calledFromLoading) throws RuntimeException
	{	
		HttpURLConnection conn = null;
		try {
			
			if ( domainStr == null )	// No info about dominStr from the calling method.. we have to find it here.
				if ( (domainStr = UrlUtils.getDomainStr(resourceURL)) == null )
					throw new RuntimeException();	// The cause it's already logged inside "getDomainStr()".
			
			conn = HttpUtils.openHttpConnection(resourceURL, domainStr);
			
			int responceCode = conn.getResponseCode();	// It's already checked for -1 case (Invalid HTTP), inside openHttpConnection().
			if ( responceCode < 200 || responceCode > 299)	// If not an "HTTP SUCCESS"..
			{
				conn = HttpUtils.handleRedirects(conn, responceCode, domainStr);	// Take care of redirects, as well as some connectivity problems.
			}
			
	        // Check if we are able to find the mime type.
	        String mimeType = null;
	        if ( (mimeType = conn.getContentType()) == null ) {
	        	if ( currentPage.equals(resourceURL) )
	        		logger.warn("Could not find mimeType for " + conn.getURL().toString());
	        	throw new RuntimeException();
	        }
	        else if ( knownDocTypes.contains(mimeType) ) {
	        	logUrl(currentPage, conn.getURL().toString());	// we send the urls, before and after potential redirections.
	        	return true;
	        }
	        else if ( calledFromLoading ) {
	        	// If it's called at loading time, then add it here in the Crawler..
	        	// because the final url produced here is the one that has already passed any potential redirects.
	        	// If it's not called from loading.. then don't add it in the crawler.. as it will be 
	        	
	        	String finalUrl = conn.getURL().toString();
	        	
	        	CrawlerController.controller.addSeed(finalUrl);
	        	
	        	//logger.debug("Mime type for \"" + finalUrl + "\" is: " + mimeType);
	        }// Else, it returns "false".
		} catch (Exception e) {
			if ( currentPage.equals(resourceURL) )	// Log this error only for urls checked at loading time.
        		logger.warn("Could not handle connection for \"" + resourceURL + "\". MimeType not retrieved!");
			throw new RuntimeException(e);
		} finally {
			if ( conn != null )
				conn.disconnect();
		}
        
        return false;
	}
	
	
	/**
	 * This method returns the domain of the given url.
	 * @param urlStr
	 * @return domainStr
	 */
	public static String getDomainStr(String urlStr)
	{
		 if ( (urlStr == null) || urlStr.isEmpty() ) {
			logger.error("A null or an empty String was given when called \"getDomainStr()\" method!");
			return null;
		}
		
		String domainStr = null;
		Matcher matcher = URL_TRIPLE.matcher(urlStr);
		
		if ( matcher.matches() )
		{
		    domainStr = matcher.group(2);	// Group <2> is the DOMAIN.
		    if ( (domainStr == null) || domainStr.isEmpty() ) {
		    	logger.warn("Unexpected null or empty value returned by \"matcher.group(2)\"");
		    	return null;
		    }
		}
		else {
			logger.warn("Unexpected matcher's (" + matcher.toString() + ") mismatch for url: \"" + urlStr + "\"");
			return null;
		}
		
		return domainStr.toLowerCase();	// We return it in lowerCase as we don't want to store double domains. (it doesn't play any part in connectivity, only the rest of the url in case Sensitive.)
	}
	
	
	public static String removeJsessionId(String urlStr)
	{
		String finalUrl = urlStr;
		
		String jsessionID = null;
		
		Matcher matcher = JSESSIONID_FILTER.matcher(urlStr);
		if (matcher.matches())
		{
			jsessionID = matcher.group(1);	// Take only the 1st part of the urlStr, without the jsessionid.
		    if ( (jsessionID == null) || jsessionID.isEmpty() ) {
		    	logger.warn("Unexpected null or empty value returned by \"matcher.group(1)\"");
		    	return null;
		    }
		    finalUrl = StringUtils.replace(finalUrl, jsessionID, "");
		}
		else
			logger.warn("Unexpected \"JSESSIONID_FILTER\" mismatch for url: \"" + urlStr + "\" !");
		
		return finalUrl;
	}
	
	
	/**
	 * This method adds a slash character "/" in the end of the input urlStr, if the given urlStr does not already end with a slash or does not end with a file extension.
	 * This tactic aims to avoid pointless redirections from non-slash-ending urls to slash-ending ones. But, it may cause connection problems for others.
	 * If the input url is not ending with an extension and is not already ending with a shlash, it adds a slash and return it, otherwise it returns the input itself.
	 * It returns null in the input is either null or an empty String.
	 * @param urlStr
	 * @return a slash-ending String or null if wrong input
	 */
	public static String checkAndAddEndingSlash(String urlStr)
	{
		if ( !urlStr.endsWith("/") )
		{
			Matcher matcherExtention = ENDING_WITH_GENERAL_EXTENSION_FILTER.matcher(urlStr);
			
			if ( !matcherExtention.matches() ) {	// If the input url is not ending with an extension.
				return (urlStr + "/");
			}
		}
		return urlStr;
	}
	
}
