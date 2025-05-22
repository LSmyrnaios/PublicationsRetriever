package eu.openaire.publications_retriever.util.http;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import eu.openaire.publications_retriever.PublicationsRetriever;
import eu.openaire.publications_retriever.crawler.MachineLearning;
import eu.openaire.publications_retriever.crawler.PageCrawler;
import eu.openaire.publications_retriever.exceptions.DocLinkFoundException;
import eu.openaire.publications_retriever.exceptions.DomainBlockedException;
import eu.openaire.publications_retriever.exceptions.FileNotRetrievedException;
import eu.openaire.publications_retriever.models.IdUrlMimeTypeTriple;
import eu.openaire.publications_retriever.models.MimeTypeResult;
import eu.openaire.publications_retriever.util.args.ArgsUtils;
import eu.openaire.publications_retriever.util.file.FileData;
import eu.openaire.publications_retriever.util.file.FileUtils;
import eu.openaire.publications_retriever.util.file.HtmlFileUtils;
import eu.openaire.publications_retriever.util.file.HtmlResult;
import eu.openaire.publications_retriever.util.url.LoaderAndChecker;
import eu.openaire.publications_retriever.util.url.UrlUtils;
import org.apache.commons.compress.compressors.brotli.BrotliCompressorInputStream;
import org.apache.commons.compress.compressors.deflate.DeflateCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorInputStream;
import org.apache.commons.io.FileDeleteStrategy;
import org.apache.commons.lang3.StringUtils;
import org.jsoup.Jsoup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * @author Lampros Smyrnaios
 */
public class ConnSupportUtils
{
	private static final Logger logger = LoggerFactory.getLogger(ConnSupportUtils.class);

	public static final Pattern MIME_TYPE_FILTER = Pattern.compile("(?:\\([']?)?([\\w]+/[\\w+.-]+).*");

	public static final Pattern POSSIBLE_DOC_OR_DATASET_MIME_TYPE = Pattern.compile("(?:(?:application|binary)/(?:(?:x-)?octet(?:-stream)?|save|force-download))|unknown");	// We don't take it for granted.. if a match is found, then we check for the "pdf" keyword in the "contentDisposition" (if it exists) or in the url.
	// There are special cases. (see: "https://kb.iu.edu/d/agtj" for "octet" info.
	// and an example for "unknown" : "http://imagebank.osa.org/getExport.xqy?img=OG0kcC5vZS0yMy0xNy0yMjE0OS1nMDAy&xtype=pdf&article=oe-23-17-22149-g002")

	public static final Pattern DATASET_MIME_TYPE = Pattern.compile("(?:application|binary)/" + LoaderAndChecker.dataset_formats);

	public static final Pattern HTML_STRING_MATCH = Pattern.compile("^(?:[\\s]*<(?:!doctype\\s)?html).*");
	public static final Pattern RESPONSE_BODY_UNWANTED_MATCH = Pattern.compile("^(?:[\\s]+|[\\s]*<(?:\\?xml|!--).*)");	// TODO - Avoid matching to "  <?xml>sddfs<html[...]" (as some times the whole page-code is a single line)

	public static final Pattern SPACE_ONLY_LINE = Pattern.compile("^[\\s]+$");	// For full-HTML-extraction.

	private static final Pattern NON_PROTOCOL_URL = Pattern.compile("^(?:[^:/]+://)(.*)");

	// Note: We cannot remove all the spaces from the HTML, as the JSOUP fails to extract the internal links. If a custom-approach will be followed, then we can take the space-removal into account.
	//public static final Pattern REMOVE_SPACES = Pattern.compile("([\\s]+)");

	public static final int minPolitenessDelay = 3000;	// 3 sec
	public static final int maxPolitenessDelay = 7000;	// 7 sec

	public static final ConcurrentHashMap<String, Integer> timesDomainsReturned5XX = new ConcurrentHashMap<String, Integer>();	// Domains that have returned HTTP 5XX Error Code, and the amount of times they did.
	public static final ConcurrentHashMap<String, Integer> timesDomainsHadTimeoutEx = new ConcurrentHashMap<String, Integer>();
	public static final ConcurrentHashMap<String, Integer> timesPathsReturned403 = new ConcurrentHashMap<String, Integer>();
	
	public static final SetMultimap<String, String> domainsMultimapWithPaths403BlackListed = Multimaps.synchronizedSetMultimap(HashMultimap.create());	// Holds multiple values for any key, if a domain(key) has many different paths (values) for which there was a 403 errorCode.
	
	private static final int timesToHave403errorCodeBeforePathBlocked = 10;	// If a path leads to 403 with different urls, more than 5 times, then this path gets blocked.
	private static final int numberOf403BlockedPathsBeforeDomainBlocked = 50;	// If a domain has more than 5 different 403-blocked paths, then the whole domain gets blocked.

	public static boolean shouldBlockMost5XXDomains = true;	// In General, if we decide to block, then the 503 will be excluded. If we decide to NOT block, then only the 511 will be blocked.
	// Keep the above as "public" and "non-final", in order to be set by external services.
	private static final int timesToHave5XXerrorCodeBeforeDomainBlocked = 10;
	private static final int timesToHaveTimeoutExBeforeDomainBlocked = 25;

	private static final int timesToReturnNoTypeBeforeDomainBlocked = 10;
	public static AtomicInteger reCrossedDocUrls = new AtomicInteger(0);

	public static final String alreadyDownloadedFromIDMessage = "This file is probably already downloaded by ID=";
	public static final String alreadyDownloadedFromSourceUrlContinuedMessage = " and SourceUrl=";

	public static final String alreadyDetectedFromIDMessage = "This url was already detected by ID=";
	public static final String alreadyDetectedFromSourceUrlContinuedMessage = " and SourceUrl=";


	public static final Set<String> knownDocMimeTypes = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
	public static final Set<String> knownDatasetMimeTypes = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

	public static final ConcurrentHashMap<String, DomainConnectionData> domainsWithConnectionData = new ConcurrentHashMap<>();

	public static String userAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0";	// This should not be "final", another program, using this software as a library, should be able to set its own "UserAgent".
	public static String acceptLanguage = "en-US,en;q=0.5";


	public static void setHttpHeaders(HttpURLConnection conn, String domainStr)
	{
		conn.setRequestProperty("User-Agent", userAgent);
		conn.setRequestProperty("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8");
		conn.setRequestProperty("Accept-Encoding", "gzip, deflate, br, zstd");	// TODO - In case we use other user-agents than "Firefox" (in a rotating way), then make sure they support "zstd" encoding as well, if not, then it should not be used for them.
		//conn.setRequestProperty("TE", "trailers");	// TODO - Investigate the "transfer-encoding" header.

		if ( !HttpConnUtils.domainsWithUnsupportedAcceptLanguageParameter.contains(domainStr) )
			conn.setRequestProperty("Accept-Language", acceptLanguage);

		conn.setRequestProperty("DNT", "1");
		conn.setRequestProperty("Connection", "keep-alive");
		conn.setRequestProperty("Sec-Fetch-Dest", "document");
		conn.setRequestProperty("Sec-Fetch-Mode", "navigate");
		conn.setRequestProperty("Sec-Fetch-Site", "cross-site");
		conn.setRequestProperty("Upgrade-Insecure-Requests", "1");
		conn.setRequestProperty("Pragma", "no-cache");
		conn.setRequestProperty("Cache-Control", "no-cache");
		conn.setRequestProperty("Host", domainStr);
	}


	public static void setKnownMimeTypes()
	{
		if ( ArgsUtils.retrieveDocuments ) {
			setKnownDocMimeTypes();
			if ( ArgsUtils.retrieveDatasets )
				setKnownDatasetMimeTypes();
		} else
			setKnownDatasetMimeTypes();
	}


	public static void setKnownDocMimeTypes()
	{
		logger.debug("Setting up the official document mime types. Currently there is support only for pdf documents.");
		knownDocMimeTypes.add("application/pdf");
		knownDocMimeTypes.add("application/x-pdf");
		knownDocMimeTypes.add("image/pdf");
		knownDocMimeTypes.add("image/x-pdf");
		knownDocMimeTypes.add("text/pdf");
		knownDocMimeTypes.add("text/x-pdf");
		knownDocMimeTypes.add("application/acrobat");
		knownDocMimeTypes.add("application/vnd.adobe.pdf");
		knownDocMimeTypes.add("application/vnd.adobe.portable-document-format");
		knownDocMimeTypes.add("application/vnd.pdf");
		knownDocMimeTypes.add("application/vnd.ms-pdf");
		knownDocMimeTypes.add("application/pdf-stream");
		knownDocMimeTypes.add("application/x-pdf-stream");
		// TODO - Add support for other document formats, like "ps", "doc", "docx", ...
			// Then create a file to keep all mimetypes and load them in memory, just like we do for the datasets below.

		// For now, allow the detection of more docTypes, only when they are not downloaded.
		if ( !ArgsUtils.shouldDownloadDocFiles ) {
			knownDocMimeTypes.add("application/postscript");
			knownDocMimeTypes.add("application/x-postscript");
			knownDocMimeTypes.add("application/vnd.cups-postscript");
			knownDocMimeTypes.add("application/eps");
			knownDocMimeTypes.add("application/ps");
			knownDocMimeTypes.add("application/x-ps");
			knownDocMimeTypes.add("application/x-postscript-not-eps");
			knownDocMimeTypes.add("text/postscript");
			knownDocMimeTypes.add("image/eps");
			knownDocMimeTypes.add("image/ps");
			knownDocMimeTypes.add("application/msword");
			knownDocMimeTypes.add("application/vnd.ms-word");
			knownDocMimeTypes.add("application/vnd.openxmlformats-officedocument.wordprocessingml.document");
			knownDocMimeTypes.add("application/vnd.openxmlformats-officedocument.presentationml.presentation");
			knownDocMimeTypes.add("application/vnd.openxmlformats-officedocument.spreadsheetml.template");
			knownDocMimeTypes.add("application/vnd.ms-powerpoint");
			knownDocMimeTypes.add("application/vnd.oasis.opendocument.presentation");
			knownDocMimeTypes.add("application/x-tex");
			knownDocMimeTypes.add("application/vnd.oasis.opendocument.text");
			knownDocMimeTypes.add("application/vnd.ms-xpsdocument");
			knownDocMimeTypes.add("application/epub+zip");
			knownDocMimeTypes.add("application/oxps");
			knownDocMimeTypes.add("application/rtf");
			knownDocMimeTypes.add("application/x-impress");
			knownDocMimeTypes.add("application/vnd.oasis.opendocument.formula");
			knownDocMimeTypes.add("application/vnd.oasis.opendocument.graphics");
			knownDocMimeTypes.add("application/vnd.oasis.opendocument.chart");
			knownDocMimeTypes.add("application/vnd.oasis.opendocument.image");
			knownDocMimeTypes.add("application/vnd.apple.pages");
			knownDocMimeTypes.add("application/vnd.apple.keynote");
			knownDocMimeTypes.add("application/vnd.wordperfect");
		}
	}


	private static final Pattern FILTER_COMMENT_FROM_MIMETYPE = Pattern.compile("([^/]+/[^/]+)(?:[\\s]*//.*)?");


	public static void setKnownDatasetMimeTypes()
	{
		logger.debug("Setting up the official dataset mime-types.");
		String resourcePath = "dataset-mimetypes.txt";
		try (InputStream inputStream = ConnSupportUtils.class.getClassLoader().getResourceAsStream(resourcePath))
		{
			if ( inputStream == null ) {
				String errorMsg = "File not found in resources: " + resourcePath;
				logger.error(errorMsg);
				System.err.println(errorMsg);
				System.exit(77);
			}

			try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8), FileUtils.mb)) {
				String line;
				while ( (line = reader.readLine()) != null ) {
					Matcher matcher = FILTER_COMMENT_FROM_MIMETYPE.matcher(line);
					if ( matcher.matches() ) {
						String mimeType = matcher.group(1);
						if ( (mimeType != null) && !mimeType.isEmpty() ) {
							knownDatasetMimeTypes.add(mimeType.trim());
						} else
							logger.error("Failed to extract the mimetype from line: " + line);
					} else
						logger.error("Failed to match the line using the \"FILTER_COMMENT_FROM_MIMETYPE\"-regex: " + line);
				}

				if ( logger.isTraceEnabled() )
					logger.trace(knownDatasetMimeTypes.toString());
			}
		} catch (IOException ioe) {
			String errorMsg = "Could not read file:" + resourcePath;
			logger.error(errorMsg, ioe);
			System.err.println(errorMsg);
			System.exit(78);
		}
	}

	
	/**
	 * This method takes a url and its mimeType and checks if it's a document mimeType or not.
	 *
	 * @param urlStr
	 * @param mimeType                          in lowercase
	 * @param contentDisposition
	 * @param calledForPageUrl
	 * @param calledForPossibleDocOrDatasetUrl)
	 * @return boolean
	 */
	public static MimeTypeResult hasDocOrDatasetMimeType(String urlStr, String mimeType, String contentDisposition, HttpURLConnection conn, boolean calledForPageUrl, boolean calledForPossibleDocOrDatasetUrl)
	{
		MimeTypeResult mimeTypeResult = null;
		String lowerCaseUrl = null;

		if ( mimeType != null )
		{	// The "mimeType" here is in lowercase
			if ( mimeType.contains("system.io.fileinfo") ) {	// Check this out: "http://www.esocialsciences.org/Download/repecDownload.aspx?fname=Document110112009530.6423303.pdf&fcategory=Articles&AId=2279&fref=repec", ιt has: "System.IO.FileInfo".
				// In this case, we want first to try the "Content-Disposition", as it's more trustworthy. If that's not available, use the urlStr as the last resort.
				if ( conn != null ) {    // Just to be sure we avoid an NPE.
					contentDisposition = conn.getHeaderField("Content-Disposition");
					// The "contentDisposition" will be definitely "null", since "mimeType != null" and so, the "contentDisposition" will not have been retrieved by the caller method.
					if ( (contentDisposition != null) ) {
						contentDisposition = contentDisposition.toLowerCase();
						if ( !contentDisposition.equals("attachment") )
							mimeTypeResult = contentDisposition.contains(".pdf") ? new MimeTypeResult("application/pdf", "document") : null;    // TODO - add more types as needed. Check: "http://www.esocialsciences.org/Download/repecDownload.aspx?qs=Uqn/rN48N8UOPcbSXUd2VFI+dpOD3MDPRfIL8B3DH+6L18eo/yEvpYEkgi9upp2t8kGzrjsWQHUl44vSn/l7Uc1SILR5pVtxv8VYECXSc8pKLF6QJn6MioA5dafPj/8GshHBvLyCex2df4aviMvImCZpwMHvKoPiO+4B7yHRb97u1IHg45E+Z6ai0Z/0vacWHoCsNT9O4FNZKMsSzen2Cw=="
					} else
						mimeTypeResult = urlStr.toLowerCase().contains(".pdf") ? new MimeTypeResult("application/pdf", "document") : null;
				}
				return mimeTypeResult;	// It may be null.
			}

			String plainMimeType = mimeType;	// Make sure we don't cause any NPE later on..
			if ( mimeType.contains("charset") || mimeType.contains("name")
					|| mimeType.startsWith("(", 0) )	// See: "https://www.mamsie.bbk.ac.uk/articles/10.16995/sim.138/galley/134/download/" -> "Content-Type: ('application/pdf', none)"
			{
				plainMimeType = getPlainMimeType(mimeType);
				if ( plainMimeType == null ) {    // If there was any error removing the charset, still try to determine the data-type.
					logger.warn("Url with problematic mimeType (" + mimeType + ") was: " + urlStr);
					lowerCaseUrl = urlStr.toLowerCase();
					if ( lowerCaseUrl.contains("pdf") )
						mimeTypeResult = new MimeTypeResult("application/pdf", "document");
					else if ( LoaderAndChecker.DATASET_URL_FILTER.matcher(lowerCaseUrl).matches() )
						mimeTypeResult = new MimeTypeResult("unspecified", "dataset");

					return mimeTypeResult;	// Default is "null".
				}
			}

			// Cleanup the mimeType further, e.g.: < application/pdf' > (with the < ' > in the end): http://www.ccsenet.org/journal/index.php/ijb/article/download/48805/26704
			plainMimeType = StringUtils.replace(plainMimeType, "'", "", -1);
			plainMimeType = StringUtils.replace(plainMimeType, "\"", "", -1);

			if ( ArgsUtils.retrieveDocuments && knownDocMimeTypes.contains(plainMimeType) )
				mimeTypeResult = new MimeTypeResult(plainMimeType, "document");
			else if ( ArgsUtils.retrieveDatasets
					&& (knownDatasetMimeTypes.contains(plainMimeType) || DATASET_MIME_TYPE.matcher(plainMimeType).matches()) )
				mimeTypeResult = new MimeTypeResult(plainMimeType, "dataset");
			else if ( POSSIBLE_DOC_OR_DATASET_MIME_TYPE.matcher(plainMimeType).matches() )
			{
				contentDisposition = conn.getHeaderField("Content-Disposition");
				if ( (contentDisposition != null) ) {
					contentDisposition = contentDisposition.toLowerCase();
					if ( !contentDisposition.equals("attachment") )    // It may be "attachment" but also be a pdf.. but we have to check if the "pdf" exists inside the url-string.
					{
						if ( ArgsUtils.retrieveDocuments && contentDisposition.contains(".pdf") )
							mimeTypeResult = new MimeTypeResult("application/pdf", "document");
						else {
							String clearContentDisposition = StringUtils.replace(contentDisposition, "\"", "", -1);
							clearContentDisposition = StringUtils.replace(clearContentDisposition, "'", "", -1);
							if ( ArgsUtils.retrieveDatasets && LoaderAndChecker.DATASET_URL_FILTER.matcher(clearContentDisposition).matches() )
								mimeTypeResult = new MimeTypeResult(plainMimeType, "dataset");
						}
						return mimeTypeResult;
					}
				}
				// In case the content-disposition is null or "attachment", check the url.
				lowerCaseUrl = urlStr.toLowerCase();
				if ( ArgsUtils.retrieveDocuments && lowerCaseUrl.contains("pdf") )
					mimeTypeResult = new MimeTypeResult("application/pdf", "document");
				else if ( ArgsUtils.retrieveDatasets && LoaderAndChecker.DATASET_URL_FILTER.matcher(lowerCaseUrl).matches() )
					mimeTypeResult = new MimeTypeResult(plainMimeType, "dataset");
			}	// TODO - When we will accept more docTypes, match it also against other docTypes, not just "pdf".
			else {	// This url is going to be classified as a "page", so do one last check, if the content-disposition refers to a doc-file.
				// The domain "bib.irb.hr" and possibly others as well, classify their full-texts as "html-pages" in the "content-type", but their "Content-Disposition" says there's a "filename.pdf", which is true.
				if ( conn != null ) {    // Just to be sure we avoid an NPE.
					contentDisposition = conn.getHeaderField("Content-Disposition");    // The "contentDisposition" will be definitely "null", since "mimeType != null" and so, the "contentDisposition" will not have been retrieved by the caller method.
					if ( contentDisposition != null )
					{
						if ( ArgsUtils.retrieveDocuments && contentDisposition.toLowerCase().contains(".pdf") )
							mimeTypeResult = new MimeTypeResult("application/pdf", "document");

						String clearContentDisposition = StringUtils.replace(contentDisposition, "\"", "", -1);
						clearContentDisposition = StringUtils.replace(clearContentDisposition, "'", "", -1);
						if ( ArgsUtils.retrieveDatasets && LoaderAndChecker.DATASET_URL_FILTER.matcher(clearContentDisposition).matches() )
							mimeTypeResult = new MimeTypeResult(plainMimeType, "dataset");
					}
				}
			}
			return mimeTypeResult;	// Default is "null".
		}
		else if ( (contentDisposition != null) && !contentDisposition.equals("attachment") ) {	// If the mimeType was not retrieved, then try the "Content Disposition", which is already in "lowerCase".
			// TODO - When we will accept more docTypes, match it also against other docTypes instead of just "pdf".
			if ( ArgsUtils.retrieveDocuments && contentDisposition.contains(".pdf") )
				mimeTypeResult = new MimeTypeResult("application/pdf", "document");
			else {
				String clearContentDisposition = StringUtils.replace(contentDisposition, "\"", "", -1);
				clearContentDisposition = StringUtils.replace(clearContentDisposition, "'", "", -1);
				if ( ArgsUtils.retrieveDatasets && LoaderAndChecker.DATASET_URL_FILTER.matcher(clearContentDisposition).matches() )
					mimeTypeResult = new MimeTypeResult("unspecified", "dataset");
			}
			return mimeTypeResult;	// Default is "null".
		}
		else {	// This is not expected to be reached. Keep it for method-reusability.
			if ( calledForPageUrl || calledForPossibleDocOrDatasetUrl )
				logger.warn("No mimeType, nor Content-Disposition, were able to be retrieved for url: " + urlStr);
			return null;
		}
	}


	public static void handleReCrossedTargetUrl(String urlId, String sourceUrl, String pageUrl, String docUrl, IdUrlMimeTypeTriple originalIdUrlMimeTypeTriple, boolean calledForPageUrl) {
		logger.info("re-crossed targetUrl found: < " + docUrl + " >");
		reCrossedDocUrls.incrementAndGet();
		String wasDirectLink = ConnSupportUtils.getWasDirectLink(sourceUrl, pageUrl, calledForPageUrl, docUrl);
		String filePath = (((ArgsUtils.shouldDownloadDocFiles || ArgsUtils.shouldJustDownloadHtmlFiles) ? (alreadyDownloadedFromIDMessage + originalIdUrlMimeTypeTriple.id + alreadyDownloadedFromSourceUrlContinuedMessage) : (alreadyDetectedFromIDMessage + originalIdUrlMimeTypeTriple.id + alreadyDetectedFromSourceUrlContinuedMessage)) + originalIdUrlMimeTypeTriple.url);
		UrlUtils.addOutputData(urlId, sourceUrl, pageUrl, docUrl, "N/A", filePath, null, false, "true", "true", "true", wasDirectLink, "true", null, "null", originalIdUrlMimeTypeTriple.mimeType);
	}

	
	/**
	 * This method receives the mimeType and returns it without the "parentheses" ot the "charset" part.
	 * If there is any error, it returns null.
	 * @param mimeType
	 * @return charset-free mimeType
	 */
	public static String getPlainMimeType(String mimeType)
	{
		if ( mimeType == null ) {	// Null-check to avoid NPE in "matcher()".
			logger.warn("A null mimeType was given to \"getPlainMimeType()\".");
			return null;
		} else if ( mimeType.length() > 255 ) {
			logger.warn("A suspiciously large mimeType was given to \"getPlainMimeType()\", having length: " + mimeType.length());
			return null;	// If it contains garbage, it may cause a "ReDoS"-attack, when being processed by "MIME_TYPE_FILTER"-regex.
		}
		
		String plainMimeType = null;
		Matcher mimeMatcher = MIME_TYPE_FILTER.matcher(mimeType);
		if ( mimeMatcher.matches() ) {
			try {
				plainMimeType = mimeMatcher.group(1);
			} catch (Exception e) { logger.error("", e); return null; }
			if ( (plainMimeType == null) || plainMimeType.isEmpty() ) {
				logger.warn("Unexpected null or empty value returned by \"mimeMatcher.group(1)\" for mimeType: \"" + mimeType + "\".");
				return null;
			}
		} else {
			logger.warn("Unexpected MIME_TYPE_FILTER's (" + mimeMatcher + ") mismatch for mimeType: \"" + mimeType + "\"");
			return null;
		}
		return plainMimeType;
	}


	public static final ConcurrentHashMap<String, String> fileHashesWithLocations = new ConcurrentHashMap<>();

	public static FileData checkAndHandleDuplicateHash(FileData fileData, String url)
	{
		// Check whether the file-hash has been found before.
		// That would mean that the same file was detected from a DIFFERENT url (otherwise we would have caught the duplicate url and not re-download the file..)
		String fileHash = fileData.getHash();
		String alreadyDownloadedFileLocation = fileHashesWithLocations.get(fileHash);
		if ( alreadyDownloadedFileLocation != null ) {
			// Delete the new duplicate file and keep the first downloaded one, which was downloaded by a different "sourceUrl".
			logger.debug("The file of url \"" + url + "\" has been already downloaded in location: " + alreadyDownloadedFileLocation);
			File file = fileData.getFile();	// The current file to be deleted.
			try {
				if ( file.exists() ) {
					try {
						FileDeleteStrategy.FORCE.delete(file);
					} catch (Exception e) {
						logger.error("Error when deleting the duplicate file from url: " + url, e);
					}
				}
			} catch (Exception e1) {
				logger.error("Error when checking if the duplicate file exists, from url: " + url, e1);
			}
			if ( ArgsUtils.shouldDownloadDocFiles ) {	// Instead of HTML-files.
				if ( ArgsUtils.fileNameType.equals(ArgsUtils.fileNameTypeEnum.numberName) )
						FileUtils.numOfDocFile--;
				else
					FileUtils.numOfDocFiles.decrementAndGet();
			}	// In case of HTML-files, no "decrementation" is needed, as the incrementation happens after calling this method, not before, like with Doc-files.
			// Return the file-data which will now point to the initial and identical file. No recalculation of hash and size is needed.
			fileData.setLocation(alreadyDownloadedFileLocation);
			fileData.setFile(new File(alreadyDownloadedFileLocation));
			return fileData;
		} else {
			fileHashesWithLocations.put(fileData.getHash(), fileData.getLocation());
			return null;	// The file is new.
		}
	}


	/**
	 * This method first checks which "HTTP METHOD" was used to connect to the docUrl.
	 * If this docUrl was connected using "GET" (i.e. when this docURL was fast-found as a possibleDocUrl), just write the data to the disk.
	 * If it was connected using "HEAD", then, before we can store the data to the disk, we connect again, this time with "GET" in order to download the data.
	 * It returns the docFileName which was produced for this docUrl.
	 * @param conn
	 * @param id
	 * @param domainStr
	 * @param docUrl
	 * @param calledForPageUrl
	 * @return
	 * @throws FileNotRetrievedException
	 */
	public static FileData downloadAndStoreDocFile(HttpURLConnection conn, String id, String domainStr, String docUrl, boolean calledForPageUrl)
			throws FileNotRetrievedException
	{
		boolean reconnected = false;
		try {
			if ( conn.getRequestMethod().equals("HEAD") ) {    // If the connection happened with "HEAD" we have to re-connect with "GET" to download the docFile.
				// No call of "conn.disconnect()" here, as we will connect to the same server.
				conn = HttpConnUtils.openHttpConnection(docUrl, domainStr, false, true);
				reconnected = true;
				int responseCode = conn.getResponseCode();    // It's already checked for -1 case (Invalid HTTP response), inside openHttpConnection().
				// Only a final-url will reach here, so no redirect should occur (thus, we don't check for it).
				if ( (responseCode < 200) || (responseCode >= 400) ) {    // If we have unwanted/error codes.
					String errorMessage = onErrorStatusCode(conn.getURL().toString(), domainStr, responseCode, calledForPageUrl, conn);
					throw new FileNotRetrievedException(errorMessage);
				}	// No redirection should exist in this re-connection with another HTTP-Method.
			}

			// Check if we should abort the download based on its content-size.
			int contentSize = 0;
			if ( (contentSize = getContentSize(conn, true, false)) == -1 )	// "Unacceptable size"-code..
				throw new FileNotRetrievedException("The HTTP-reported size of this file was unacceptable!");
			// It may be "-2", in case it was not retrieved..

			// Write the downloaded bytes to the docFile and return the docFileName.
			FileData fileData =  null;
			if ( ArgsUtils.fileNameType.equals(ArgsUtils.fileNameTypeEnum.numberName) )
				fileData = FileUtils.storeDocFileWithNumberName(conn, docUrl, contentSize);
			else
				fileData = FileUtils.storeDocFileWithIdOrOriginalFileName(conn, docUrl, id, contentSize);

			if ( fileData == null ) {
				String errMsg = "The file could not be " + (ArgsUtils.shouldUploadFilesToS3 ? "uploaded to S3" : "downloaded") + " from the docUrl " + docUrl;
				logger.warn(errMsg);
				throw new FileNotRetrievedException(errMsg);
			}

			FileData newFileData = checkAndHandleDuplicateHash(fileData, docUrl);
			if ( newFileData != null )
				return newFileData;
			// Else, it's not a duplicate.

			File docFile = fileData.getFile();
			if ( ArgsUtils.shouldUploadFilesToS3 ) {
				try {	// In the "S3"-mode, we don't keep the files locally, after they get transferred.
					FileDeleteStrategy.FORCE.delete(docFile);    // We don't need the local file anymore..
				} catch (Exception e) {
					logger.warn("The file \"" + docFile.getName() + "\" could not be deleted after being uploaded to S3 ObjectStore!");
				}
			}

			return fileData;

		} catch (FileNotRetrievedException dfnre ) {	// Catch it here, otherwise it will be caught as a general exception.
			throw dfnre;	// Avoid creating a new "DocFileNotRetrievedException" if it's already created. By doing this we have a better stack-trace if we decide to log it in the caller-method.
		} catch (Exception e) {
			logger.error("", e);
			throw new FileNotRetrievedException(e.getMessage());
		} finally {
			if ( reconnected )	// Otherwise the given-previous connection will be closed by the calling method.
				conn.disconnect();
		}
	}


	/**
	 * This method receives the domain and manages the sleep-time, if needed.
	 * It first extracts the last 3 parts of the domain. Then it checks if the domain is faced for the first time.
	 * If it is the first time, then the domain is added in the ConcurrentHashMap along with a new DomainConnectionData.
	 * Else the thread will lock that domain and check if it was connected before at most "minPolitenessDelay" secs, if so, then the thread will sleep for a random number of milliseconds.
	 * Different threads lock on different domains, so each thread is not dependent on another thread which works on a different domain.
	 * @param domainStr
	 */
	public static void applyPolitenessDelay(String domainStr)
	{
		// Consider only the last three parts of a domain, not all, otherwise, a sub-sub-domain might connect simultaneously with another sub-sub-domain.
		domainStr = UrlUtils.getTopThreeLevelDomain(domainStr);

		DomainConnectionData domainConnectionData = domainsWithConnectionData.get(domainStr);
		if ( domainConnectionData == null ) {	// If it is the 1st time connecting.
			domainsWithConnectionData.put(domainStr, new DomainConnectionData());
			return;
		}

		long elapsedTimeSinceLastConnection;
		domainConnectionData.lock.lock();// Threads trying to connect with the same domain, should sleep one AFTER the other, to avoid coming back after sleep at the same time, in the end..
		Instant currentTime = Instant.now();
		try {
			elapsedTimeSinceLastConnection = Duration.between(domainConnectionData.lastTimeConnected, currentTime).toMillis();
		} catch (Exception e) {
			logger.warn("An exception was thrown when tried to obtain the time elapsed from the last time the domain connected: " + e.getMessage());
			domainConnectionData.updateAndUnlock(currentTime);
			return;
		}

		if ( elapsedTimeSinceLastConnection < minPolitenessDelay ) {
			long finalPolitenessDelay = getRandomNumber(minPolitenessDelay, maxPolitenessDelay) - elapsedTimeSinceLastConnection;

			// Apply the following for testing. Otherwise, it's more efficient to use the above method.
			//long randomPolitenessDelay = getRandomNumber(minPolitenessDelay, maxPolitenessDelay);
			//long finalPolitenessDelay = randomPolitenessDelay - elapsedTimeSinceLastConnection;	// This way we avoid reaching the upper limit while preventing underflow (in case we applied the difference in the parameters of "getRandomNumber()").
			//logger.debug("WILL SLEEP for " + finalPolitenessDelay + " | randomNumber was " + randomPolitenessDelay + ", elapsedTime was: " + elapsedTimeSinceLastConnection + " | domain: " + domainStr);	// DEBUG!

			try {
				Thread.sleep(finalPolitenessDelay);    // Avoid server-overloading for the same domain.
			} catch (InterruptedException ie) {
				Instant newCurrentTime = Instant.now();
				try {
					elapsedTimeSinceLastConnection = Duration.between(currentTime, newCurrentTime).toMillis();
				} catch (Exception e) {
					logger.warn("An exception was thrown when tried to obtain the time elapsed from the last time the \"currentTime\" was updated: " + e.getMessage());
					domainConnectionData.updateAndUnlock(newCurrentTime);	// Update the time and connect.
					return;
				}
				if ( elapsedTimeSinceLastConnection < minPolitenessDelay ) {
					finalPolitenessDelay -= elapsedTimeSinceLastConnection;
					try {
						Thread.sleep(finalPolitenessDelay);
					} catch (InterruptedException ignored) {}
				}
			}	// At this point, if both sleeps were interrupted, some time has already passed, so it's ok to connect to the same domain.
			currentTime = Instant.now();	// Update, after the sleep.
		} //else
			//logger.debug("NO SLEEP NEEDED, elapsedTime: " + elapsedTimeSinceLastConnection + " > " + minPolitenessDelay + " | domain: " + domainStr);	// DEBUG!

		domainConnectionData.updateAndUnlock(currentTime);
	}

	
	/**
	 * This method receives a pageUrl which gave an HTTP-300-code and extracts an internalLink out of the multiple choices provided.
	 * @param conn
	 * @return
	 */
	public static String getInternalLinkFromHTTP300Page(String url, HttpURLConnection conn)
	{
		try {
			String html = null;
			HtmlResult htmlResult = null;
			if ( (htmlResult = ConnSupportUtils.getHtml(conn, null, url, null, false, null, null)) == null ) {
				logger.warn("Could not retrieve the HTML-code for HTTP300PageUrl: " + url);
				return null;
			}
			html = htmlResult.getHtmlString();
			HashSet<String> extractedLinksHashSet = PageCrawler.extractInternalLinksFromHtml(html, url);
			if ( extractedLinksHashSet == null || extractedLinksHashSet.size() == 0 )
				return null;	// Logging is handled inside..

			return new ArrayList<>(extractedLinksHashSet).get(0);	// There will be only a couple of urls, so it's not a big deal to gather them all.
		} catch ( DocLinkFoundException dlfe) {
			return dlfe.getMessage();	// Return the DocLink to connect with.
		} catch (Exception e) {
			logger.error("", e);
			return null;
		}
	}
	
	
	/**
	 * This method is called on errorStatusCode only. Meaning any status code not belonging in 2XX or 3XX.
	 * @param urlStr
	 * @param domainStr
	 * @param errorStatusCode
	 * @param calledForPageUrl
	 * @param conn
	 * @return
	 * @throws DomainBlockedException
	 */
	public static String onErrorStatusCode(String urlStr, String domainStr, int errorStatusCode, boolean calledForPageUrl, HttpURLConnection conn) throws DomainBlockedException
	{
		if ( (errorStatusCode == 500) && domainStr.contains("handle.net") ) {    // Don't take the 500 of "handle.net", into consideration, it returns many times 500, where it should return 404.. so treat it like a 404.
			//logger.warn("\"handle.com\" returned 500 where it should return 404.. so we will treat it like a 404.");    // See an example: "https://hdl.handle.net/10655/10123".
			errorStatusCode = 404;	// Set it to 404 to be handled as such, if any rule for 404s is to be added later.
		}

		String errorLogMessage;

		if ( (errorStatusCode >= 400) && (errorStatusCode <= 499) )	// Client Error.
		{
			errorLogMessage = "Url: \"" + urlStr + "\" seems to be unreachable. Received: HTTP " + errorStatusCode + " Client Error.";
			// Get the error-response-body:
			if ( calledForPageUrl && (errorStatusCode != 404) && (errorStatusCode != 410) ) {
				String errorText = getErrorMessageFromResponseBody(conn, urlStr);
				if ( errorText != null ) {

					if ( domainStr.contains("doi.org") && errorText.contains("Not a DOI") ) {
						logger.warn("Found a \"doi.org\" url with an invalid DOI: " + urlStr);
						// In this case it is highly likely that the "DOI" in the url is a DOI-LINK.
					}

					errorLogMessage += " Error-text: " + errorText;
					/*if ( errorStatusCode == 403 && errorText.toLowerCase().contains("javascript") ) {
						// Use selenium to execute the JS.
						driver.get(urlStr);
						driver.findElement(By.id("download")).click();
					}*/
				}
			}
			if ( errorStatusCode == 403 )
				on403ErrorCode(urlStr, domainStr, calledForPageUrl);	// The "DomainBlockedException" will go up-method by its own, if thrown inside this one.
			else if ( errorStatusCode == 429 ) {
				String retryAfterTime = conn.getHeaderField("Retry-After");	// Get the "Retry-After" header, if it exists.
				if ( retryAfterTime != null ) {
					errorLogMessage += " | Retry-After:" + retryAfterTime;
					// TODO - Add this domain in a special hashMap, having the retry-after time as a value.
					// TODO - Upon deciding the delay between requests of the same domain, lookup each domain and take into account this "retry-after" time.
					// TODO - One possible problem with this: we may get our threads starved, waiting even a day, for a couple of domains.
					//  Ideally, we should put those urls aside somehow and retry them in the end, if the waiting time is above some threshold.
					//  (Just put them in a list and go to the next url. In the end, take care all the urls in that list. If the retry-time is huge (e.g. > 1 day, set a could-retry status, with a nice error-msg, and finish the current-batch.))
					// TODO - Check the syntax of this header here: https://www.geeksforgeeks.org/http-headers-retry-after/
				}
			}
		}
		else {	// Other errorCodes. Retrieve the domain and make the required actions.
			if ( (domainStr == null) || !urlStr.contains(domainStr) )	// The domain might have changed after redirections.
				domainStr = UrlUtils.getDomainStr(urlStr, null);

			if ( (errorStatusCode >= 500) && (errorStatusCode <= 599) ) {	// Server Error.
				errorLogMessage = "Url: \"" + urlStr + "\" seems to be unreachable. Received: HTTP " + errorStatusCode + " Server Error.";
				on5XXerrorCode(errorStatusCode, domainStr);
			} else {	// Unknown Error (including non-handled: 1XX and the weird one: 999 (used for example on Twitter), responseCodes).
				errorLogMessage = "Url: \"" + urlStr + "\" seems to be unreachable. Received unexpected responseCode: " + errorStatusCode;
				if ( calledForPageUrl ) {
					String errorText = getErrorMessageFromResponseBody(conn, urlStr);
					if ( errorText != null )
						errorLogMessage += " Error-text: " + errorText;
				}
				logger.warn(errorLogMessage);
				if ( domainStr != null ) {
					HttpConnUtils.blacklistedDomains.add(domainStr);
					logger.warn("Domain: \"" + domainStr + "\" was blocked, after giving a " + errorStatusCode + " HTTP-status-code.");
					throw new DomainBlockedException(domainStr);	// Throw this even if there was an error preventing the domain from getting blocked.
				}
			}
		}
		return errorLogMessage;
	}
	

	public static InputStream checkEncodingAndGetInputStream(HttpURLConnection conn, boolean isForError)
	{
		InputStream inputStream = null;
		try {
			inputStream = (isForError ? conn.getErrorStream() : conn.getInputStream());
			if ( isForError && (inputStream == null) )	// Only the "getErrorStream" may return null.
				return null;
		} catch (Exception e) {
			logger.error("", e);
			return null;
		}
		// Determine the potential encoding
		String encoding = conn.getHeaderField("content-encoding");
		if ( encoding != null ) {
			String url = conn.getURL().toString();
			/*if ( logger.isTraceEnabled() )
				logger.trace("Url \"" + url + "\" has content-encoding: " + encoding);*/
			InputStream compressedInputStream = getCompressedInputStream(inputStream, encoding, url, isForError);
			if ( compressedInputStream == null ) {
				try {
					inputStream.close();
				} catch (IOException ioe) {}
				return null;    // The error is logged inside the called method.
			}
			inputStream = compressedInputStream;
		}

		return inputStream;
	}


	public static InputStream getCompressedInputStream(InputStream inputStream, String encoding, String url, boolean isForError)
	{
		InputStream compressedInputStream;
		String lowercaseEncoding = encoding.toLowerCase();
		try {
			if ( lowercaseEncoding.equals("gzip") )
				compressedInputStream = new GzipCompressorInputStream(inputStream);
			else if ( lowercaseEncoding.equals("deflate") )
				compressedInputStream = new DeflateCompressorInputStream(inputStream);
			else if ( lowercaseEncoding.equals("br") )
				compressedInputStream = new BrotliCompressorInputStream(inputStream);
			else if ( lowercaseEncoding.equals("zstd") )
				compressedInputStream = new ZstdCompressorInputStream(inputStream);
			else {
				logger.warn("An unsupported \"content-encoding\" (" + encoding + ") was received from url: " + url);
				return null;
			}
		} catch (IOException ioe) {
			String exMsg = ioe.getMessage();
			if ( exMsg.startsWith("Input is not in the") )
				logger.warn(exMsg + " | http-published-encoding: " + encoding + " | url: " + url);
				// Some urls do not return valid html-either way.
			else
				logger.error("Could not acquire the compressorInputStream for encoding: " + encoding + " | url: " + url, ioe);
			return null;
		}
		return compressedInputStream;
	}


	public static String getErrorMessageFromResponseBody(HttpURLConnection conn, String url)
	{
		HtmlResult htmlResult = getHtml(conn, null, url, null, true, null, null);
		if ( htmlResult == null )
			return null;

		String html = htmlResult.getHtmlString();
		int htmlLength = html.length();	// It won't be 0;
		if ( htmlLength > 10000 )
			return null;

		String errorText = Jsoup.parse(html).text();	// The result is already "trimmed".
		return ((errorText.length() > 0) ? errorText : null);
	}


	/**
	 * This method handles the HTTP 403 Error Code.
	 * When a connection returns 403, we take the path of the url, and we block it, as the directory which we are trying to connect to, is forbidden to be accessed.
	 * If a domain ends up having more paths blocked than a certain number, we block the whole domain itself.
	 * @param urlStr
	 * @param domainStr
	 * @param calledForPageUrl
	 * @throws DomainBlockedException
	 */
	public static void on403ErrorCode(String urlStr, String domainStr, boolean calledForPageUrl) throws DomainBlockedException
	{
		Matcher matcher = null;
		if ( (domainStr == null) || !urlStr.contains(domainStr) ) {    // The domain might have changed after redirections.
			if ( (matcher = UrlUtils.getUrlMatcher(urlStr)) == null )
				return;
			if ( (domainStr = UrlUtils.getDomainStr(urlStr, matcher)) == null )
				return;
		}

		String pathStr = UrlUtils.getPathStr(urlStr, matcher);
		if ( pathStr == null )
			return;
		
		if ( countAndBlockPathAfterTimes(domainsMultimapWithPaths403BlackListed, timesPathsReturned403, pathStr, domainStr, timesToHave403errorCodeBeforePathBlocked, calledForPageUrl) )
		{
			logger.warn("Path: \"" + pathStr + "\" of domain: \"" + domainStr + "\" was blocked after returning 403 Error Code more than " + timesToHave403errorCodeBeforePathBlocked + " times.");
			// Block the whole domain if it has more than a certain number of blocked paths.
			if ( domainsMultimapWithPaths403BlackListed.get(domainStr).size() > numberOf403BlockedPathsBeforeDomainBlocked )	// It will not throw an NPE, as the domain is inserted by the previous method.
			{
				// Note that the result of "domainsMultimapWithPaths403BlackListed.get(domainStr)" cannot be null! It may only be empty.
				if ( ! ConnSupportUtils.domainsNotBlockableAfterTimes.contains(domainStr) ) {
					HttpConnUtils.blacklistedDomains.add(domainStr);	// Block the whole domain itself.
					logger.warn("Domain: \"" + domainStr + "\" was blocked, after having more than " + numberOf403BlockedPathsBeforeDomainBlocked + " of its paths 403blackListed.");
					domainsMultimapWithPaths403BlackListed.removeAll(domainStr);	// No need to keep this anymore.
					throw new DomainBlockedException(domainStr);
				}
			}
		}
	}
	
	
	public static boolean countAndBlockPathAfterTimes(SetMultimap<String, String> domainsWithPaths, ConcurrentHashMap<String, Integer> pathsWithTimes, String pathStr, String domainStr, int timesBeforeBlocked, boolean calledForPageUrl)
	{
		if ( countInsertAndGetTimes(pathsWithTimes, pathStr) > timesBeforeBlocked )
		{
			// If we use MLA, we are storing the docPage-successful-paths, so check if this is one of them, if it is then don't block it.
			// If it's an internal-link, then.. we can't iterate over every docUrl-successful-path of every docPage-successful-path.. it's too expensive O(5*n), not O(1)..
			if ( MachineLearning.useMLA && calledForPageUrl && MachineLearning.successPathsHashMultiMap.containsKey(pathStr) )
				return false;

			domainsWithPaths.put(domainStr, pathStr);	// Add this path in the list of blocked paths of this domain.
			pathsWithTimes.remove(pathStr);	// No need to keep the count for a blocked path.
			return true;
		}
		return false;
	}
	
	
	/**
	 * This method check if there was ever a url from the given/current domain, which returned an HTTP 403 Error Code.
	 * If there was, it retrieves the directory path of the given/current url and checks if it caused an 403 Error Code before.
	 * It returns "true" if the given/current path is already blocked,
	 * otherwise, if it's not blocked, or if there was a problem retrieving this path from this url, it returns "false".
	 * @param urlStr
	 * @param domainStr
	 * @return boolean
	 */
	public static boolean checkIfPathIs403BlackListed(String urlStr, String domainStr)
	{
		if ( domainsMultimapWithPaths403BlackListed.containsKey(domainStr) )	// If this domain has returned 403 before, then go and check if the current path is blacklisted.
		{
			String pathStr = UrlUtils.getPathStr(urlStr, null);
			if ( pathStr == null )	// If there is a problem retrieving this athStr, return false;
				return false;
			
			return domainsMultimapWithPaths403BlackListed.get(domainStr).contains(pathStr);
		}
		return false;
	}


	/**
	 * This method is called for an HTTP-5XX case. Depending on the value of "shouldBlockMost5XXDomains", it blocks the domain if it has given a 5XX more than Y times.
	 * @param domainStr
	 * @throws DomainBlockedException
	 */
	public static void on5XXerrorCode(int http5xxErrorCode, String domainStr) throws DomainBlockedException
	{
		if ( !shouldBlockMost5XXDomains )
			if ( http5xxErrorCode != 511 )
				return;

		if ( (http5xxErrorCode == 503) || (domainStr == null) )
			return;

		if ( countAndBlockDomainAfterTimes(HttpConnUtils.blacklistedDomains, timesDomainsReturned5XX, domainStr, timesToHave5XXerrorCodeBeforeDomainBlocked, true) ) {
			logger.warn("Domain: \"" + domainStr + "\" was blocked after returning 5XX Error Code " + timesToHave5XXerrorCodeBeforeDomainBlocked + " times.");
			throw new DomainBlockedException(domainStr);
		}
	}
	
	
	public static void onTimeoutException(String domainStr) throws DomainBlockedException
	{
		if ( countAndBlockDomainAfterTimes(HttpConnUtils.blacklistedDomains, timesDomainsHadTimeoutEx, domainStr, timesToHaveTimeoutExBeforeDomainBlocked, true) ) {
			logger.warn("Domain: \"" + domainStr + "\" was blocked after causing TimeoutException " + timesToHaveTimeoutExBeforeDomainBlocked + " times.");
			throw new DomainBlockedException(domainStr);
		}
	}


	public static final Set<String> domainsNotBlockableAfterTimes = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
	// These domains have some non-giving pageUrls, some dataset-giving pages and some document-giving ones. So we avoid blocking them, in case 500 consecutive pages do not give full--texts.
	static {
		// All of these domains are manually checked for their quality and consistency.
		domainsNotBlockableAfterTimes.add("zenodo.org");
		domainsNotBlockableAfterTimes.add("doi.org");	// This domain is the starting point for many publications. It redirects to other domains.
		domainsNotBlockableAfterTimes.add("dx.doi.org");	// Same as above. We have to add full-domains in this set.
	}

	
	/**
	 * This method handles domains which are reaching cases were they can be blocked.
	 * It calculates the times they did something and if they reached a red line, it adds them in the blackList provided by the caller.
	 * After adding it in the blackList, it removes its counters to free-up memory.
	 * It returns "true", if this domain was blocked, otherwise, "false".
	 * @param blackList
	 * @param domainsWithTimes
	 * @param domainStr
	 * @param timesBeforeBlock
	 * @param checkAgainstDocUrlsHits
	 * @return boolean
	 */
	public static boolean countAndBlockDomainAfterTimes(Set<String> blackList, ConcurrentHashMap<String, Integer> domainsWithTimes, String domainStr, int timesBeforeBlock, boolean checkAgainstDocUrlsHits)
	{
		if ( domainsNotBlockableAfterTimes.contains(domainStr) )
			return false;

		int badTimes = countInsertAndGetTimes(domainsWithTimes, domainStr);
		if ( badTimes > timesBeforeBlock )
		{
			if ( checkAgainstDocUrlsHits ) {	// This will not be the case for MLA-blocked-domains. We cannot take into account ALL retrieved docUrls when only a part was retrieved by the MLA.
				Integer goodTimes = UrlUtils.domainsAndNumHits.get(domainStr);
				if ( (goodTimes != null)
						&& (badTimes <= (goodTimes + timesBeforeBlock)) )	// If the badTimes are less/equal to the goodTimes PLUS the predefined "bufferZone", do not block this domain.
					return false;
			}

			blackList.add(domainStr);    // Block this domain.
			domainsWithTimes.remove(domainStr);	// Remove counting-data.
			return true;	// This domain was blocked.
		}
		return false;	// It wasn't blocked.
	}
	
	
	public static int countInsertAndGetTimes(ConcurrentHashMap<String, Integer> itemsWithTimes, String itemToCount)
	{
		int curTimes = 1;
		Integer prevTimes = itemsWithTimes.get(itemToCount);
		if ( prevTimes != null )
			curTimes += prevTimes;
		
		itemsWithTimes.put(itemToCount, curTimes);
		
		return curTimes;
	}


	/**
	 * This method blocks the domain of the targetLink which tried to cause the "sharedSiteSession-redirectionPack".
	 * It also blocks the domain of the url which led to this redirection (if applicable and only for the right-previous url)
	 * @param targetUrl
	 * @param previousFromTargetUrl
	 * @return
	 */
	public static List<String> blockSharedSiteSessionDomains(String targetUrl, String previousFromTargetUrl)
	{
		final List<String> blockedDomainsToReturn = new ArrayList<>(2);
		String targetUrlDomain, beforeTargetUrlDomain;

		if ( (targetUrlDomain = UrlUtils.getDomainStr(targetUrl, null)) == null )
			return null;	// The problem is logged, but nothing more needs to bo done.

		blockedDomainsToReturn.add(targetUrlDomain);
		if ( HttpConnUtils.blacklistedDomains.add(targetUrlDomain) )	// If it was added for the first time.
			logger.warn("Domain: \"" + targetUrlDomain + "\" was blocked after trying to cause a \"sharedSiteSession-redirectionPack\" with url: \"" + targetUrl + "\"!");

		if ( (previousFromTargetUrl != null) && !previousFromTargetUrl.equals(targetUrl) ) {
			if ( (beforeTargetUrlDomain = UrlUtils.getDomainStr(previousFromTargetUrl, null)) != null ) {
				blockedDomainsToReturn.add(beforeTargetUrlDomain);
				if ( HttpConnUtils.blacklistedDomains.add(beforeTargetUrlDomain) )    // If it was added for the first time.
					logger.warn("Domain: \"" + beforeTargetUrlDomain + "\" was blocked after its url : \"" + previousFromTargetUrl + "\" tried to redirect to targetUrl: \"" + targetUrl + "\" and cause a \"sharedSiteSession-redirectionPack\"!");
			}
		}

		return blockedDomainsToReturn;
	}


	public static ThreadLocal<StringBuilder> htmlStrBuilder = new ThreadLocal<StringBuilder>();	// Every Thread has its own variable.

	public static HtmlResult getHtml(HttpURLConnection conn, String urlId, String pageUrl, BufferedReader bufferedReader, boolean isForError, Matcher urlMatcher, String firstHTMLlineFromDetectedContentType)
	{
		int contentSize = 0;
		if ( (contentSize = getContentSize(conn, false, isForError)) == -1 ) {	// "Unacceptable size"-code..
			if ( !isForError )	// It's expected to have ZERO-length most times, and thus the extraction cannot continue. Do not show a message. It's rare that we get an error-message anyway.
				logger.warn("Aborting HTML-extraction for pageUrl: " + pageUrl);
			ConnSupportUtils.closeBufferedReader(bufferedReader);	// This page's content-type was auto-detected, and the process fails before re-requesting the conn-inputStream, then make sure we close the last one.
			return null;
		}
		// It may be "-2" in case the "contentSize" was not available.

		boolean shouldWriteHtmlFile = (ArgsUtils.shouldJustDownloadHtmlFiles && !isForError);

		StringBuilder htmlStrB = htmlStrBuilder.get();
		if ( (htmlStrB == null) && !shouldWriteHtmlFile ) {
			htmlStrB = new StringBuilder(100_000);	// Initialize and pre-allocate the StringBuilder.
			htmlStrBuilder.set(htmlStrB);	// Save it for future use by this thread.
		}

		int bufferSize = 0;
		InputStream inputStream = null;
		if ( bufferedReader == null ) {
			inputStream = checkEncodingAndGetInputStream(conn, isForError);
			if ( inputStream == null )	// The error is already logged inside.
				return null;
			bufferSize = (((contentSize != -2) && (contentSize < FileUtils.mb)) ? contentSize : FileUtils.mb);
		}

		FileData htmlFileData = null;
		String fullPathFileName = null;
		if ( shouldWriteHtmlFile ) {
			try {
				htmlFileData = HtmlFileUtils.getFinalHtmlFilePath(urlId, pageUrl, urlMatcher, contentSize);	// This will not be null.
				fullPathFileName = htmlFileData.getLocation();
				//} catch (FileNotRetrievedException fnre) {
			} catch (Exception e) {
				logger.error("Failed to acquire the \"fullPathFileName\": " + e.getMessage());
				if ( ArgsUtils.fileNameType.equals(ArgsUtils.fileNameTypeEnum.numberName) )
					HtmlFileUtils.htmlFilesNum.decrementAndGet();
				return null;
			}
		}

		try (BufferedReader br = ((bufferedReader != null) ? bufferedReader : new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8), bufferSize));
			 BufferedWriter bw = (shouldWriteHtmlFile ? new BufferedWriter(new FileWriter(fullPathFileName, StandardCharsets.UTF_8),  bufferSize) : null) )	// Try-with-resources
		{
			String inputLine;
			String htmlSpaceChar = ((bw != null) ? FileUtils.endOfLine : " ");

			// We may have extracted the first response-line in order to determine the content-type, in case no other method succeeded.
			if ( firstHTMLlineFromDetectedContentType != null ) {
				if ( !shouldWriteHtmlFile )
					htmlStrB.append(firstHTMLlineFromDetectedContentType).append(htmlSpaceChar);
				if ( bw != null ) {
					bw.write(firstHTMLlineFromDetectedContentType);
					bw.newLine();
				}
			}

			while ( (inputLine = br.readLine()) != null )
			{
				if ( !inputLine.isEmpty() && (inputLine.length() != 1) && !SPACE_ONLY_LINE.matcher(inputLine).matches() ) {	// We check for (inputLine.length() != 1), as some lines contain just an unrecognized byte.
					if ( !shouldWriteHtmlFile )
						htmlStrB.append(inputLine).append(htmlSpaceChar);	// Add the "spaceChar" to avoid joining words from different lines.
					if ( bw != null ) {
						bw.write(inputLine);
						bw.newLine();
					}
					//logger.debug(inputLine);	// DEBUG!
				}
			}
			//logger.debug("Chars in html: " + String.valueOf(htmlStrB.length()));	// DEBUG!

			if ( bw != null ) {
				bw.flush();	// Otherwise the "bw" will be flushed only upon closing and the calculation of "hash" and "size" will not work.
				logger.info("HtmlFile '" + fullPathFileName + "' was downloaded.");
				if ( htmlFileData.calculateAndSetHashAndSize() ) {
					FileData newFileData = checkAndHandleDuplicateHash(htmlFileData, pageUrl);
					if ( newFileData != null )
						htmlFileData = newFileData;
					else	// It's not a duplicate.
						HtmlFileUtils.htmlFilesNum.incrementAndGet();
				} else {
					try {
						FileDeleteStrategy.FORCE.delete(htmlFileData.getFile());
					} catch (Exception e) {
						logger.error("Error when deleting the duplicate file from pageUrl: " + pageUrl, e);
					}
					return null;
				}
			}

			if ( !shouldWriteHtmlFile ) {
				String htmlString = ((htmlStrB != null) && htmlStrB.length() != 0) ? htmlStrB.toString() : null;
				return ((htmlString != null) ? new HtmlResult(htmlString, null) : null);
			} else
				return new HtmlResult(null, htmlFileData);	// Make sure we return a "null" on empty string, to better handle the case in the caller-method.

		} catch ( IOException ioe ) {
			logger.error("IOException when retrieving the HTML-code for pageUrl \"" + pageUrl + "\": " + ioe.getMessage());
			return null;
		} catch ( Exception e ) {
			logger.error("", e);
			return null;
		} finally {
			if ( htmlStrB != null )
				htmlStrB.setLength(0);	// Reset "StringBuilder" WITHOUT re-allocating.
			try {
				if ( inputStream != null )
					inputStream.close();
			} catch (IOException ioe) {
				// Ignore.
			}
		}
	}


	/**
	 *
	 * @param finalUrlStr
	 * @param domainStr
	 * @param conn
	 * @param calledForPageUrl
	 * @return
	 * @throws DomainBlockedException
	 * @throws RuntimeException
	 */
	public static ArrayList<Object> detectContentTypeFromResponseBody(String finalUrlStr, String domainStr, HttpURLConnection conn, boolean calledForPageUrl)
			throws DomainBlockedException, RuntimeException
	{
		String warnMsg = "No ContentType nor ContentDisposition, were able to be retrieved from url: " + finalUrlStr;
		String mimeType = null;
		boolean foundDetectedContentType = false;
		String firstHtmlLine = null;
		BufferedReader bufferedReader = null;
		boolean calledForPossibleDocUrl = false;
		boolean wasConnectedWithHTTPGET = conn.getRequestMethod().equals("GET");

		// Try to detect the content type.
		if ( wasConnectedWithHTTPGET ) {
			DetectedContentType detectedContentType = ConnSupportUtils.extractContentTypeFromResponseBody(conn);
			if ( detectedContentType != null ) {
				if ( detectedContentType.detectedContentType.equals("html") ) {
					if ( calledForPageUrl )	// Do not show logs for dozens of internal-links.. Normally this check is not needed, as the non-possible-docUrl-internal-links are connected only with GET, but maybe a possible-docUrls is connected with GET and it is just a page. Also, later things may change even for internal-links.
						logger.debug("The url with the undeclared content type < " + finalUrlStr + " >, was examined and found to have HTML contentType! Going to visit the page.");
					mimeType = "text/html";
					foundDetectedContentType = true;
					firstHtmlLine = detectedContentType.firstHtmlLine;
					bufferedReader = detectedContentType.bufferedReader;
				} else if ( detectedContentType.detectedContentType.equals("pdf") ) {
					logger.debug("The url with the undeclared content type < " + finalUrlStr + " >, was examined and found to have PDF contentType!");
					mimeType = "application/pdf";
					calledForPossibleDocUrl = true;	// Important for the re-connection.
					foundDetectedContentType = true;
					// The "bufferedReader" has already been closed.
				} else if ( detectedContentType.detectedContentType.equals("undefined") )
					logger.debug("The url with the undeclared content type < " + finalUrlStr + " >, was examined and found to have UNDEFINED contentType.");
					// The "bufferedReader" has already been closed.
				else
					warnMsg += "\nUnspecified \"detectedContentType\": " + detectedContentType.detectedContentType;
					// Normally, we should never reach here. The BufferedReader should be null.
			}
			else	//  ( detectedContentType == null )
				warnMsg += "\nCould not retrieve the response-body for url: " + finalUrlStr;
		}
		else	// ( connection-method == "HEAD" )
			warnMsg += "\nThe initial connection was made with the \"HTTP-HEAD\" method, so there is no response-body to use to detect the content-type.";

		if ( !foundDetectedContentType && wasConnectedWithHTTPGET ) {	// If it could be detected (by using the "GET"  method to take the response-body), but it was not, only then go and check if it should be blocked.
			// The BufferedReader should be null here.
			if ( ConnSupportUtils.countAndBlockDomainAfterTimes(HttpConnUtils.blacklistedDomains, HttpConnUtils.timesDomainsReturnedNoType, domainStr, timesToReturnNoTypeBeforeDomainBlocked, true) ) {
				logger.warn(warnMsg);
				logger.warn("Domain: \"" + domainStr + "\" was blocked after returning no Type-info more than " + timesToReturnNoTypeBeforeDomainBlocked + " times.");
				throw new DomainBlockedException(domainStr);
			} else
				throw new RuntimeException(warnMsg);	// We can't retrieve any clue. This is not desired. The "warnMsg" will be printed by the caller method.
		}

		ArrayList<Object> detectionList = new ArrayList<>(5);
		detectionList.add(0, mimeType);
		detectionList.add(1, foundDetectedContentType);
		detectionList.add(2, firstHtmlLine);
		detectionList.add(3, bufferedReader);
		detectionList.add(4, calledForPossibleDocUrl);
		return detectionList;
	}


	/**
	 * This method examines the first line of the Response-body and returns the content-type.
	 * TODO - The only "problem" is that after the "inputStream" closes, it cannot be opened again. So, we cannot parse the HTML afterwards nor download the pdf.
	 * TODO - I guess it's fine to just re-connect but we should search for a way to reset the stream without the overhead of re-connecting.. (keeping the first line and using it later is the solution I use, but only for the html-type, since the pdf-download reads bytes and not lines)
	 * The "br.reset()" is not supported, since the given input-stream does not support the "mark" operation.
	 * @param conn
	 * @return "html", "pdf", "undefined", null
	 */
	public static DetectedContentType extractContentTypeFromResponseBody(HttpURLConnection conn)
	{
		int contentSize = 0;
		if ( (contentSize = getContentSize(conn, false, false)) == -1) {	// "Unacceptable size"-code..
			logger.warn("Aborting content-extraction for pageUrl: " + conn.getURL().toString());
			return null;
		}
		// It may be "-2" in case the "contentSize" was not available.

		InputStream inputStream = checkEncodingAndGetInputStream(conn, false);
		if ( inputStream == null )
			return null;

		int bufferSize = (((contentSize != -2) && contentSize < FileUtils.fiveMb) ? contentSize : FileUtils.fiveMb);

		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8), bufferSize);
			String inputLine;

			// Skip empty lines in the beginning of the HTML-code
			while ( ((inputLine = br.readLine()) != null) && (inputLine.isEmpty() || (inputLine.length() == 1) || RESPONSE_BODY_UNWANTED_MATCH.matcher(inputLine).matches()) )	// https://repositorio.uam.es/handle/10486/687988
			{	/* No action inside */	}	// https://bv.fapesp.br/pt/publicacao/96198/homogeneous-gaussian-profile-p-type-emitters-updated-param/		http://naosite.lb.nagasaki-u.ac.jp/dspace/handle/10069/29792

			// For DEBUGing..
			/*while ( ((inputLine = br.readLine()) != null) && !(inputLine.isEmpty() || inputLine.length() == 1 || RESPONSE_BODY_UNWANTED_MATCH.matcher(inputLine).matches()) )
			{ logger.debug(inputLine + "\nLength of line: " + inputLine.length());
				logger.debug(Arrays.toString(inputLine.chars().toArray()));
			}*/

			// Check if the stream ended before we could find an "accepted" line.
			if ( inputLine == null )
				return null;

			//logger.debug("First actual line of RequestBody: " + inputLine);	// DEBUG!

			String lowerCaseInputLine = inputLine.toLowerCase();
			//logger.debug(lowerCaseInputLine + "\nLength of line: "  + lowerCaseInputLine.length());	// DEBUG!
			if ( HTML_STRING_MATCH.matcher(lowerCaseInputLine).matches() )
				return new DetectedContentType("html", inputLine, br);
			else {
				try {
					br.close();	// We close the stream here, since if we got a pdf we should reconnect in order to get the very first bytes (we don't read "lines" when downloading PDFs).
				} catch (IOException ignored) {}
				if ( lowerCaseInputLine.startsWith("%pdf-", 0) )	// After the "-", the pdf-specification version follows (e.g. "%pdf-1.6").
					return new DetectedContentType("pdf", null, null);	// For PDFs we just going to re-connect in order to download the, since we read plain bytes for them and not String-lines, so we re-connect just to be sure we don't corrupt them.
				else
					return new DetectedContentType("undefined", inputLine, null);
			}
		} catch (Exception e) {
			if ( e instanceof IOException )
				logger.error("IOException when retrieving the HTML-code: " + e.getMessage());
			else
				logger.error("", e);

			if ( br != null ) {
				try {
					br.close();
				} catch (IOException ignored) {}
			}
			return null;
		}
	}


	private static final int maxAllowedContentSizeMB = (HttpConnUtils.maxAllowedContentSize / FileUtils.mb);

	
	/**
	 * This method returns the ContentSize of the content of an HttpURLConnection.
	 * @param conn
	 * @param calledForFullTextDownload
	 * @param isForError
	 * @return contentSize
	 * @throws NumberFormatException
	 */
	public static int getContentSize(HttpURLConnection conn, boolean calledForFullTextDownload, boolean isForError)
	{
		int contentSize = 0;
		try {
			contentSize = Integer.parseInt(conn.getHeaderField("Content-Length"));
			if ( (contentSize <= 0) || (contentSize > HttpConnUtils.maxAllowedContentSize) ) {
				if ( !isForError )	// In case of an error, we expect it to be < 0 > most of the time. Do not show a message, but return that it's not acceptable to continue acquiring the content.
					logger.warn((calledForFullTextDownload ? "DocUrl: \"" : "Url: \"") + conn.getURL().toString() + "\" had a non-acceptable contentSize: " + contentSize + ". The maxAllowed one is: " + maxAllowedContentSizeMB + " MB.");
				return -1;
			}
			//logger.debug("Content-length of \"" + conn.getURL().toString() + "\" is: " + contentSize);	// DEBUG!
			return contentSize;
		} catch (NumberFormatException nfe) {	// This is also thrown if the "contentLength"-field does not exist inside the headers-list.
			if ( calledForFullTextDownload && logger.isTraceEnabled() )	// It's not useful to show a logging-message otherwise.
				logger.trace("No \"Content-Length\" was retrieved from docUrl: \"" + conn.getURL().toString() + "\"! We will store the docFile anyway..");	// No action is needed.
			return -2;	// The content size could not be retrieved.
		} catch ( Exception e ) {
			logger.error("", e);
			return -2;	// The content size could not be retrieved.
		}
	}


	public static void closeBufferedReader(BufferedReader bufferedReader)
	{
		try {
			if ( bufferedReader != null )
				bufferedReader.close();
		} catch ( IOException ioe ) {
			logger.warn("Problem when closing \"BufferedReader\": " + ioe.getMessage());
		}
	}

	
	/**
	 * This method constructs fully-formed urls which are connection-ready, as the retrieved links may be relative-links.
	 * @param pageUrl
	 * @param currentLink
	 * @param urlBase
	 * @return
	 */
	public static String getFullyFormedUrl(String pageUrl, String currentLink, URL urlBase)
	{
		try {
			if ( urlBase == null ) {
				if ( pageUrl != null )
					urlBase = new URL(pageUrl);
				else {
					logger.error("No urlBase to produce a fully-formedUrl for internal-link: " + currentLink);
					return null;
				}
			}

			if ( currentLink.startsWith("?") )	// This case is mishandled by the automatic generation, unless we use this custom logic.
				return (urlBase + currentLink);
			else
				return new URL(urlBase, currentLink).toString();	// Return the TargetUrl.
		} catch (Exception e) {
			logger.error("Error when producing fully-formedUrl for internal-link: " + currentLink, e.getMessage());
			return null;
		}
	}


	public static boolean isJustAnHTTPSredirect(String currentUrl, String targetUrl)
	{
		return ( currentUrl.startsWith("http://", 0) && targetUrl.startsWith("https://", 0)
				&& haveOnlyProtocolDifference(currentUrl, targetUrl) );
	}


	public static boolean isJustASlashRedirect(String currentUrl, String targetUrl)
	{
		return ( !currentUrl.endsWith("/") && targetUrl.endsWith("/")
				&& currentUrl.equals(targetUrl.substring(0, targetUrl.length() -1)) );
	}


	/**
	 * This method returns "true" if the two urls have only a protocol-difference.
	 * @param url1
	 * @param url2
	 * @return
	 */
	public static boolean haveOnlyProtocolDifference(String url1, String url2)
	{
		Matcher url1NonProtocolMatcher = NON_PROTOCOL_URL.matcher(url1);
		if ( !url1NonProtocolMatcher.matches() ) {
			logger.warn("URL < " + url1 + " > failed to match with \"NON_PROTOCOL_URL\"-regex: " + NON_PROTOCOL_URL);
			return false;
		}

		String non_protocol_url1;
		try {
			non_protocol_url1 = url1NonProtocolMatcher.group(1);
		} catch (Exception e) { logger.error("No match for url1: " + url1, e); return false; }
		if ( (non_protocol_url1 == null) || non_protocol_url1.isEmpty() ) {
			logger.warn("Unexpected null or empty value returned by \"url1NonProtocolMatcher.group(1)\" for url: \"" + url1 + "\"");
			return false;
		}

		Matcher url2UrlNonProtocolMatcher = NON_PROTOCOL_URL.matcher(url2);
		if ( !url2UrlNonProtocolMatcher.matches() ) {
			logger.warn("URL < " + url2 + " > failed to match with \"NON_PROTOCOL_URL\"-regex: " + NON_PROTOCOL_URL);
			return false;
		}

		String non_protocol_url2Url;
		try {
			non_protocol_url2Url = url2UrlNonProtocolMatcher.group(1);
		} catch (Exception e) { logger.error("No match for url2: " + url2, e); return false; }
		if ( (non_protocol_url2Url == null) || non_protocol_url2Url.isEmpty() ) {
			logger.warn("Unexpected null or empty value returned by \"url2UrlNonProtocolMatcher.group(1)\" for url: \"" + url2 + "\"");
			return false;
		}

		return ( non_protocol_url1.equals(non_protocol_url2Url) );
	}


	public static InputStream getInputStreamFromInputDataUrl()
	{
		if ( (ArgsUtils.inputDataUrl == null) || ArgsUtils.inputDataUrl.isEmpty() ) {
			String errorMessage = "The \"inputDataUrl\" was not given, even though";
			logger.error(errorMessage);
			System.err.println(errorMessage);
			PublicationsRetriever.executor.shutdownNow();
			System.exit(55);
		}

		InputStream inputStream = null;
		try {
			HttpURLConnection conn = HttpConnUtils.handleConnection(null, ArgsUtils.inputDataUrl, ArgsUtils.inputDataUrl, ArgsUtils.inputDataUrl, null, true, true);
			String mimeType = conn.getHeaderField("Content-Type");
			if ( (mimeType == null) || !mimeType.toLowerCase().contains("json") ) {
				String errorMessage = "The mimeType of the url was either null or a non-json: " + mimeType;
				logger.error(errorMessage);
				System.err.println(errorMessage);
				PublicationsRetriever.executor.shutdownNow();
				System.exit(56);
			}

			inputStream = ConnSupportUtils.checkEncodingAndGetInputStream(conn, false);
			if ( inputStream == null )
				throw new RuntimeException("Could not acquire the InputStream!");

			// Check if we should abort the download based on its content-size.
			int contentSize = 0;
			if ( (contentSize = getContentSize(conn, true, false)) == -1 )	// "Unacceptable size"-code..
				throw new FileNotRetrievedException("The HTTP-reported size of this file was unacceptable!");
			// It may be "-2", in case it was not retrieved..
			int bufferSize = (((contentSize != -2) && contentSize < FileUtils.fiveMb) ? contentSize : FileUtils.fiveMb);

			// Wrap it with a buffer, for increased efficiency.
			inputStream = new BufferedInputStream(inputStream, bufferSize);

		} catch (Exception e) {
			String errorMessage = "Unexpected error when retrieving the input-stream from the inputDataUrl:\n" + e.getMessage();
			logger.error(errorMessage);
			System.err.println(errorMessage);
			PublicationsRetriever.executor.shutdownNow();
			System.exit(57);
		}

		// If the user gave both the inputUrl and the inputFile, then make sure we close the SYS-IN stream.
		try {
			System.in.close();
		} catch (Exception ignored) { }

		return inputStream;
	}


	private static final ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();

	public static long getRandomNumber(int min, int max) {
		return threadLocalRandom.nextLong(min, max+1);	// Since the max upper bound is exclusive, we have to set (max+1) in order to take the max.
	}


	// TODO - Find a way to detect when a sourceUrl is automatically redirected through HTTP to the finalDocUrl, in order to return "true". Use Thread-local public variables.
	public static String getWasDirectLink(String sourceUrl, String pageUrl, boolean calledForPageUrl, String finalUrlStr) {
		String wasDirectLink;
		if ( calledForPageUrl ) {
			boolean isSpecialUrl = (!ArgsUtils.shouldJustDownloadHtmlFiles && HttpConnUtils.isSpecialUrl.get());
			if ( (!isSpecialUrl && ( pageUrl.equals(finalUrlStr) || ConnSupportUtils.haveOnlyProtocolDifference(pageUrl, finalUrlStr) ))
					|| sourceUrl.equals(finalUrlStr) || ConnSupportUtils.haveOnlyProtocolDifference(sourceUrl, finalUrlStr))	// Or if it was not a "specialUrl" and the pageUrl is the same as the docUrl.
				wasDirectLink = "true";
			else if ( isSpecialUrl )
				wasDirectLink = "false";
			else
				wasDirectLink = "N/A";
		} else {
			// This docOrDatasetUrl came from crawling the pageUrl, so we know that it surely did not come directly from the sourceUrl.
			wasDirectLink = "false";
		}
		return wasDirectLink;
	}


	public static void printEmbeddedExceptionMessage(Exception e, String resourceURL)
	{
		String exMsg = e.getMessage();
		if (exMsg != null) {
			StackTraceElement firstLineOfStackTrace = e.getStackTrace()[0];
			logger.warn("[" + firstLineOfStackTrace.getFileName() + "->" + firstLineOfStackTrace.getMethodName() + "(@" + firstLineOfStackTrace.getLineNumber() + ")] - " + exMsg);
		} else
			logger.warn("Could not handle connection for \"" + resourceURL + "\"!");
	}


	public static void printConnectionDebugInfo(HttpURLConnection conn, boolean shouldShowFullHeaders)
	{
		if ( conn == null ) {
			logger.warn("The given connection instance was null..");
			return;
		}
		logger.debug("Connection debug info:\nURL: < {} >,\nContentType: \"{}\". ContentDisposition: \"{}\", HTTP-method: \"{}\"",
				conn.getURL().toString(), conn.getContentType(), conn.getHeaderField("Content-Disposition"), conn.getRequestMethod());
		if ( shouldShowFullHeaders ) {
			StringBuilder sb = new StringBuilder(1000).append("Headers:\n");	// This StringBuilder is thread-safe as a local-variable.
			Map<String, List<String>> headers = conn.getHeaderFields();
			for ( String headerKey : headers.keySet() )
				for ( String headerValue : headers.get(headerKey) )
					sb.append(headerKey).append(" : ").append(headerValue).append("\n");
			logger.debug(sb.toString());
		}
	}
	
	
	public static void printRedirectDebugInfo(String currentUrl, String location, String targetUrl, int responseCode, int curRedirectsNum)
	{
		// FOR DEBUG -> Check to see what's happening with the redirect urls (location field types, as well as potential error redirects).
		// Some domains use only the target-ending-path in their location field, while others use full target url.
		
		if ( currentUrl.contains("doi.org") ) {	// Debug a certain domain or url-path.
			logger.debug("\n");
			logger.debug("Redirect(s) num: " + curRedirectsNum);
			logger.debug("Redirect code: " + responseCode);
			logger.debug("Base: " + currentUrl);
			logger.debug("Location: " + location);
			logger.debug("Target: " + targetUrl + "\n");
		}
	}


	/**
	 * This method print redirectStatistics if the initial url matched to the given wantedUrlType.
	 * It's intended to be used for debugging-only.
	 * @param initialUrl
	 * @param finalUrl
	 * @param wantedUrlType
	 * @param redirectsNum
	 */
	public static void printFinalRedirectDataForWantedUrlType(String initialUrl, String finalUrl, String wantedUrlType, int redirectsNum)
	{
		if ( (wantedUrlType != null) && initialUrl.contains(wantedUrlType) ) {
			logger.debug("\"" + initialUrl + "\" DID: " + redirectsNum + " redirect(s)!");
			logger.debug("Final link is: \"" + finalUrl + "\"");
		}
	}
	
}
