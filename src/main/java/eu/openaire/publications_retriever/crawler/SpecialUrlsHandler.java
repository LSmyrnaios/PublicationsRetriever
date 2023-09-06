package eu.openaire.publications_retriever.crawler;


import eu.openaire.publications_retriever.exceptions.DocLinkFoundException;
import eu.openaire.publications_retriever.util.http.ConnSupportUtils;
import eu.openaire.publications_retriever.util.http.HttpConnUtils;
import eu.openaire.publications_retriever.util.url.LoaderAndChecker;
import eu.openaire.publications_retriever.util.url.UrlUtils;
import org.apache.commons.lang3.StringUtils;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * @author Lampros Smyrnaios
 */
public class SpecialUrlsHandler
{
	private static final Logger logger = LoggerFactory.getLogger(SpecialUrlsHandler.class);

	private static final String europepmcPageUrlBasePath = "https://europepmc.org/backend/ptpmcrender.fcgi?accid=";

	private static final String nasaBaseDomainPath = "https://ntrs.nasa.gov/";

	private static final String ieeexploreBasePath = "https://ieeexplore.ieee.org/stampPDF/getPDF.jsp?tp=&arnumber=";


	public static String checkAndHandleSpecialUrls(String resourceUrl) throws RuntimeException
	{
		String updatedUrl = null;

		if ( (updatedUrl = checkAndGetEuropepmcDocUrl(resourceUrl)) != null ) {
			//logger.debug("Europepmc-PageURL: " + resourceUrl + " to possible-docUrl: " + updatedUrl);	// DEBUG!
			resourceUrl = updatedUrl;
		} else if ( (updatedUrl = checkAndDowngradeManuscriptElsevierUrl(resourceUrl)) != null ) {
			//logger.debug("ManuscriptElsevier-URL: " + resourceUrl + " to acceptable-Url: " + updatedUrl);	// DEBUG!
			resourceUrl = updatedUrl;
		} else if ( (updatedUrl = checkAndGetNasaDocUrl(resourceUrl)) != null ) {
			//logger.debug("Nasa-PageURL: " + resourceUrl + " to possible-docUrl: " + updatedUrl);	// DEBUG!
			resourceUrl = updatedUrl;
		} else if ( (updatedUrl = checkAndGetFrontiersinDocUrl(resourceUrl)) != null ) {
			//logger.debug("Frontiersin-PageURL: " + resourceUrl + " to possible-docUrl: " + updatedUrl);	// DEBUG!
			resourceUrl = updatedUrl;
		} else if ( (updatedUrl = checkAndHandlePsyarxiv(resourceUrl)) != null ) {
			//logger.debug("Psyarxiv-PageURL: " + resourceUrl + " to possible-docUrl: " + updatedUrl);	// DEBUG!
			resourceUrl = updatedUrl;
		} else if ( (updatedUrl = checkAndHandleIjcseonlinePage(resourceUrl)) != null ) {
			//logger.debug("Ijcseonline-PageURL: " + resourceUrl + " to possible-docUrl: " + updatedUrl);	// DEBUG!
			resourceUrl = updatedUrl;
		} else if ( (updatedUrl = checkAndHandleIeeeExplorer(resourceUrl)) != null ) {
			//logger.debug("IeeeExplorer-PageURL: " + resourceURL + " to possible-docUrl: " + updatedUrl);	// DEBUG!
			resourceUrl = updatedUrl;
		} else if ( (updatedUrl = checkAndHandleOSFurls(resourceUrl)) != null ) {
			//logger.debug("OSF-PageURL: " + resourceURL + " to possible-docUrl: " + updatedUrl);	// DEBUG!
			resourceUrl = updatedUrl;
		} else
			resourceUrl = checkAndHandleDergipark(resourceUrl);	// It returns the same url if nothing was handled.

		return resourceUrl;
	}


	/////////// europepmc.org ////////////////////
	public static String checkAndGetEuropepmcDocUrl(String europepmcUrl)
	{
		if ( europepmcUrl.contains("europepmc.org") && !europepmcUrl.contains("ptpmcrender.fcgi") )	// The "ptpmcrender.fcgi" indicates that this is already a "europepmc"-docUrl.
		{
			// Offline-redirect to the docUrl.
			String idStr = UrlUtils.getDocIdStr(europepmcUrl, null);
			if ( idStr != null )
				return (europepmcPageUrlBasePath + (!idStr.startsWith("PMC", 0) ? "PMC"+idStr : idStr) + "&blobtype=pdf");    // TODO - Investigate some 404-failures (THE DOC-URLS belong to diff domain)
			else
				return europepmcUrl;
		}
		return null;	// It's from another domain, keep looking..
	}


	/////////// manuscript.elsevier ////////////////////
	// These urls, try to connect with HTTPS, but their certificate is due from 2018. So, we downgrade them to plain HTTP.
	public static String checkAndDowngradeManuscriptElsevierUrl(String manuscriptElsevierUrl)
	{
		if ( manuscriptElsevierUrl.contains("manuscript.elsevier.com") ) {
			manuscriptElsevierUrl = StringUtils.replace(manuscriptElsevierUrl, "https", "http", 1);
			return manuscriptElsevierUrl;
		} else
			return null;
	}


	/////////// ntrs.nasa.gov ////////////////////
	public static String checkAndGetNasaDocUrl(String nasaPageUrl)
	{
		if ( nasaPageUrl.contains("ntrs.nasa.gov/citations") && ! nasaPageUrl.contains("api/") )
		{
			// Offline-redirect to the docUrl.
			String idStr = UrlUtils.getDocIdStr(nasaPageUrl, null);
			if ( idStr == null )
				return nasaPageUrl;

			String citationPath = StringUtils.replace(nasaPageUrl, nasaBaseDomainPath, "", 1);
			citationPath = (citationPath.endsWith("/") ? citationPath : citationPath+"/");	// Make sure the "citationPath" has an ending slash.

			return (nasaBaseDomainPath + "api/" + citationPath + "downloads/" + idStr + ".pdf");
		}
		return null;	// It's from another domain, keep looking..
	}


	/////////// www.frontiersin.org ////////////////////
	public static String checkAndGetFrontiersinDocUrl(String frontiersinPageUrl)
	{
		//https://www.frontiersin.org/article/10.3389/feart.2017.00079
		//https://www.frontiersin.org/articles/10.3389/fphys.2018.00414/full

		if ( frontiersinPageUrl.contains("www.frontiersin.org") )
		{
			if ( frontiersinPageUrl.endsWith("/pdf") )
				return frontiersinPageUrl;	// It's already a docUrl, go connect.
			else if ( !frontiersinPageUrl.contains("/article") )
				throw new RuntimeException("This \"frontiersin\"-url is known to not lead to a docUrl: " + frontiersinPageUrl);	// Avoid the connection.

			// Offline-redirect to the docUrl.
			String idStr = UrlUtils.getDocIdStr(frontiersinPageUrl, null);
			if ( idStr == null )
				return frontiersinPageUrl;

			if ( frontiersinPageUrl.endsWith("/full") )
				return StringUtils.replace(frontiersinPageUrl, "/full", "/pdf");
			else
				return frontiersinPageUrl + "/pdf";
		}
		return null;	// It's url from another domain.
	}


	///////// psyarxiv.com ///////////////////
	// https://psyarxiv.com/e9uk7
	/**
	 * Thia is a dynamic javascript domain.
	 * @return
	 */
	public static String checkAndHandlePsyarxiv(String pageUrl) {
		if ( pageUrl.contains("psyarxiv.com") ) {
			if ( !pageUrl.contains("/download") )
				return (pageUrl + (pageUrl.endsWith("/") ? "download" : "/download"));	// Add a "/download" in the end. That will indicate we are asking for the docUrl.
			else
				return pageUrl;
		}
		return null;	// It's from another domain, keep looking..
	}


	///////// dergipark.gov.tr ///////////////////////
	// http://dergipark.gov.tr/beuscitech/issue/40162/477737
	/**
	 * This domain has been transferred to dergipark.org.tr. In 2021 the old domain will be inaccessible at some poit.
	 * @param pageUrl
	 * @return
	 */
	public static String checkAndHandleDergipark(String pageUrl)
	{
		return StringUtils.replace(pageUrl, "dergipark.gov.tr", "dergipark.org.tr");
	}

	public static Pattern Turkjgastroenterol_docUrl_pattern = Pattern.compile("<div[\\s]*>[\\s]*(/content/files/[^<>]+.pdf)[\\s]*</div>");


	/////// www.turkjgastroenterol.org //////
	// This is used when the url has already arrived in the "PageCrawler.visit()" method.
	public static boolean extractAndCheckTurkjgastroenterolDocUrl(String pageHtml, String urlId, String sourceUrl, String pageUrl, String pageDomain)
	{
		Matcher matcher = Turkjgastroenterol_docUrl_pattern.matcher(pageHtml);
		if ( !matcher.find() ) {
			UrlUtils.logOutputData(urlId, sourceUrl, pageUrl, UrlUtils.unreachableDocOrDatasetUrlIndicator, "Discarded in 'PageCrawler.visit()' method, as there was a problem retrieving the \"turkjgastroenterol\"-pdf-url from its html.", pageDomain, true, "true", "true", "false", "false", "false", null, "null");
			return false;
		}

		String pdfUrl = null;
		try {
			pdfUrl = matcher.group(1);
		} catch (Exception e) {
			logger.warn("No pdf-url was found inside the html of page: " + pageUrl, e);
			UrlUtils.logOutputData(urlId, sourceUrl, pageUrl, UrlUtils.unreachableDocOrDatasetUrlIndicator, "Discarded in 'PageCrawler.visit()' method, as there was a problem retrieving the \"turkjgastroenterol\"-pdf-url from its html.", pageDomain, true, "true", "true", "false", "false", "false", null, "null");
			PageCrawler.contentProblematicUrls.incrementAndGet();
			return false;
		}
		if ( (pdfUrl == null) || pdfUrl.isEmpty() ) {
			logger.warn("No pdf-url was found inside the html of page: " + pageUrl);
			UrlUtils.logOutputData(urlId, sourceUrl, pageUrl, UrlUtils.unreachableDocOrDatasetUrlIndicator, "Discarded in 'PageCrawler.visit()' method, as there was a problem retrieving the \"turkjgastroenterol\"-pdf-url from its html.", pageDomain, true, "true", "true", "false", "false", "false", null, "null");
			PageCrawler.contentProblematicUrls.incrementAndGet();
			return false;
		}

		String urlToCheck = pdfUrl;
		if ( ((urlToCheck = ConnSupportUtils.getFullyFormedUrl(pageUrl, pdfUrl, null)) == null)	// Make it a full-URL.
				|| ((urlToCheck = LoaderAndChecker.basicURLNormalizer.filter(urlToCheck)) == null) ) {	// Normalize it.
			logger.warn("Could not normalize url: " + pdfUrl);
			UrlUtils.logOutputData(urlId, sourceUrl, pageUrl, UrlUtils.unreachableDocOrDatasetUrlIndicator, "Discarded in 'PageCrawler.visit()' method, as the retrievied \"turkjgastroenterol\"-pdf-url had normalization's problems.", pageDomain, true, "true", "true", "false", "false", "false", null, "null");
			LoaderAndChecker.connProblematicUrls.incrementAndGet();
			return false;
		}

		if ( (urlToCheck = LoaderAndChecker.handleUrlChecks(urlId, urlToCheck)) == null )
			return false;	// The output-data was logged inside.

		if ( UrlUtils.docOrDatasetUrlsWithIDs.containsKey(urlToCheck) ) {	// If we got into an already-found docUrl, log it and return.
			ConnSupportUtils.handleReCrossedDocUrl(urlId, urlToCheck, urlToCheck, urlToCheck, true);	// The output-data was logged inside.
			return false;
		}

		boolean isPossibleDocOrDatasetUrl = true;

		try {	// We sent the < null > into quotes to avoid causing NPEs in the thread-safe datastructures that do not support null input.
			HttpConnUtils.connectAndCheckMimeType(urlId, urlToCheck, urlToCheck, urlToCheck, null, true, isPossibleDocOrDatasetUrl);
		} catch (Exception e) {
			List<String> list = LoaderAndChecker.getWasValidAndCouldRetry(e, urlToCheck);
			// The pageUrl is a VALID-URL, but whether we couldRetry or not, it depends on the error of the docUrl.. So if it is a 404, then we can never get the fulltext. On the contrary, if it is a 503, then in the future wy might get it.
			String wasValid = list.get(0);
			String couldRetry = list.get(1);
			UrlUtils.logOutputData(urlId, sourceUrl, pageUrl, UrlUtils.unreachableDocOrDatasetUrlIndicator, "Discarded in 'PageCrawler.visit()' method, as there was a problem in checking the retrieved \"turkjgastroenterol\"-pdf-url.", pageDomain, true, "true", wasValid, "false", "false", couldRetry, null, "null");
		}

		return true;
	}


	/////////// aup-online.com ////////////////////
	public static void handleAupOnlinePage(String pageUrl, Elements elementLinksOnPage) throws DocLinkFoundException, RuntimeException
	{
		// This domain gives the fulltext-urls inside the "action" attribute of a "form"-element.
		// We handle these elements for every domain, but, this one gives a false-positive ".../download" url, which causes this method to exit early and not reach the desired form-element.
		for ( Element el : elementLinksOnPage ) {
			if ( el.attr("data-title").contains("ownload") ) {    // This includes both "Download" and "download". If no such attribute exists, then the check will return "false".
				String possibleDocUrl = el.attr("action").trim();
				if ( !possibleDocUrl.isEmpty() ) {
					//logger.debug(possibleDocUrl);    // DEBUG!
					throw new DocLinkFoundException(possibleDocUrl);
				}
			}
		}
		// If we reach here, then no docUrl can be retrieved for this page. We should return immediately.
		throw new RuntimeException("No docUrl was found inside a form-element, for \"aup-online.com\" pageUrl: " + pageUrl);
	}


	private static final String ijcseonlineBaseUrl = "https://www.ijcseonline.org/pub_paper/";
	private static final Pattern IJCSEONLINE_PDF_FILENAME = Pattern.compile(".+/[^/]+&(.+)$");

	//////////////////// www.ijcseonline.org /////////////////////
	/**
	 * Transform the internal-link pageUrl into a pdfUrl.
	 *
	 * An initial pageUrl like this is examined: https://www.ijcseonline.org/full_paper_view.php?paper_id=4547
	 * Then, an internal pageUrl is extracted: https://www.ijcseonline.org/pdf_paper_view.php?paper_id=4547&48-IJCSE-07375.pdf
	 * Then, in this method, the final pdfUrl is created: https://www.ijcseonline.org/pub_paper/48-IJCSE-07375.pdf
	 *
	 * In case the given url is not a pdf-page-url or if we have an error, this method returns the pageUrl it received.
	 * */
	public static String checkAndHandleIjcseonlinePage(String pageUrl)
	{
		if ( ! pageUrl.contains("www.ijcseonline.org") )
			return null;    // It's from another domain, keep looking..

		if ( ! pageUrl.contains("pdf_paper_view.php") )
			return pageUrl;

		String pdfFileName = null;
		try {
			Matcher matcher = IJCSEONLINE_PDF_FILENAME.matcher(pageUrl);
			if ( !matcher.matches() )
				return pageUrl;

			pdfFileName = matcher.group(1);
			if ( (pdfFileName == null) || pdfFileName.isEmpty() ) {
				logger.error("No pdf-file-name was extracted from pageUrl: " + pageUrl);
				return pageUrl;
			}
		} catch (Exception e) {
			logger.error("", e);
			return pageUrl;
		}

		return ijcseonlineBaseUrl + pdfFileName;
	}


	//////////  ieeexplore.ieee.org   /////////////////
	// https://ieeexplore.ieee.org/document/8924293 --> https://ieeexplore.ieee.org/stampPDF/getPDF.jsp?tp=&arnumber=8924293
	public static String checkAndHandleIeeeExplorer(String pageUrl) {
		if ( pageUrl.contains("ieeexplore.ieee.org") ) {
			if ( pageUrl.contains("/stampPDF/") ) {	// It's probably already a docUrl
				return pageUrl;
			}
			String idStr = UrlUtils.getDocIdStr(pageUrl, null);
			if ( idStr == null )
				return pageUrl;

			return (ieeexploreBasePath + idStr);
		}
		return null;    // It's from another domain, keep looking..
	}


	//////////  osf.io   /////////////////
	// https://osf.io/2xpq7 --> https://osf.io/2xpq7/download
	public static String checkAndHandleOSFurls(String pageUrl)
	{
		if ( !pageUrl.contains("://osf.io") )	// We want to transform only urls belonging to the top-level-domain.
			return null;    // It's from another domain, keep looking..

		if ( pageUrl.contains("/download") )	// It's probably already a docUrl
			return pageUrl;
		else if ( !pageUrl.endsWith("/") )
			pageUrl += "/";

		return (pageUrl + "download");
	}

}
