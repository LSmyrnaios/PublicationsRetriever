package eu.openaire.doc_urls_retriever.crawler;


import eu.openaire.doc_urls_retriever.exceptions.FailedToProcessScienceDirectException;
import eu.openaire.doc_urls_retriever.util.file.FileUtils;
import eu.openaire.doc_urls_retriever.util.http.ConnSupportUtils;
import eu.openaire.doc_urls_retriever.util.http.HttpConnUtils;
import eu.openaire.doc_urls_retriever.util.url.LoaderAndChecker;
import eu.openaire.doc_urls_retriever.util.url.UrlUtils;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * @author Lampros Smyrnaios
 */
public class SpecialUrlsHandler
{
	private static final Logger logger = LoggerFactory.getLogger(SpecialUrlsHandler.class);
	
	// The following regex-pattern was used in ScienceDirect-handling in early days. Keep it here, as it may be of need in any domain in the future..
	// public static final Pattern JAVASCRIPT_REDIRECT_URL = Pattern.compile("(?:window.location[\\s]+\\=[\\s]+\\')(.*)(?:\\'\\;)");

	private static final String scienceDirectBasePath = "https://www.sciencedirect.com/science/article/pii/";

	private static final String europepmcPageUrlBasePath = "https://europepmc.org/backend/ptpmcrender.fcgi?accid=";

	private static final String academicMicrosoftFinalPageUrlBasePath = "https://academic.microsoft.com/api/entity/";

	private static final Pattern ACADEMIC_MICROSOFT_ID = Pattern.compile(".+/([\\d]+).*");

	private static final String nasaBaseDomainPath = "https://ntrs.nasa.gov/";


	/////////// sciencedirect.com //// "linkinghub.elsevier.com //// api.elsevier.com ////////////////////
	/**
	 * This method checks if the given url belongs to the "scienceDirect-family"-urls and if so it handles it.
	 * If the given url is not a kindOf-scienceDirect-url, then it
	 * @param pageUrl
	 * @return
	 */
	public static String checkAndGetScienceDirectUrl(String pageUrl) throws FailedToProcessScienceDirectException
	{
		boolean wasLinkinghubElsevier = false;
		if ( pageUrl.contains("linkinghub.elsevier.com") || pageUrl.contains("api.elsevier.com") )	// Avoid plain "elsevier.com"-rule as there are: "www.elsevier.com" and "journals.elsevier.com" which don't give docUrls and are handled differently.
		{
			if ( (pageUrl = offlineRedirectElsevierToScienceDirect(pageUrl)) == null ) {	// Logging is handled inside "offlineRedirectElsevierToScienceDirect()".
				throw new FailedToProcessScienceDirectException();	// Throw the exception to avoid the connection.
				//logger.debug("Produced ScienceDirect-url: " + pageUrl);	// DEBUG!
			}
			wasLinkinghubElsevier = true;
		}

		if ( wasLinkinghubElsevier || (pageUrl.contains("sciencedirect.com") && !pageUrl.endsWith("/pdf")) )
			return (pageUrl + (pageUrl.endsWith("/") ? "pdf" : "/pdf"));	// Add a "/pdf" in the end. That will indicate we are asking for the docUrl.
		else
			return null;	// This indicates that the calling method will not replace the url.
	}


	/**
	 * This method receives a url from "linkinghub.elsevier.com" or "api.elsevier.com" and returns it's matched url in "sciencedirect.com".
	 * We do this because the "linkinghub.elsevier.com" urls have a javaScript redirect inside which we are not able to handle without doing html scraping.
	 * If there is any error this method returns the URL it first received.
	 * @param elsevierUrl
	 * @return
	 */
	public static String offlineRedirectElsevierToScienceDirect(String elsevierUrl)
	{
		String idStr = UrlUtils.getDocIdStr(elsevierUrl, null);
		if ( idStr != null ) {
			idStr = StringUtils.replace(idStr, "PII:", "", 1);	// In case of an "api.elsevier.com" pageUrl.
			return (scienceDirectBasePath + idStr);
		}
		return null;
	}


	/////////// europepmc.org ////////////////////
	public static String checkAndGetEuropepmcDocUrl(String pageUrl)
	{
		if ( pageUrl.contains("europepmc.org") && !pageUrl.contains("ptpmcrender.fcgi") )	// The "ptpmcrender.fcgi" indicates that this is already a "europepmc"-docUrl.
			return offlineRedirectToEuropepmcDocUrl(pageUrl);
		else
			return null;
	}


	public static String offlineRedirectToEuropepmcDocUrl(String europepmcPageUrl)
	{
		String idStr = UrlUtils.getDocIdStr(europepmcPageUrl, null);
		if ( idStr != null )
			return (europepmcPageUrlBasePath + (!idStr.startsWith("PMC", 0) ? "PMC"+idStr : idStr) + "&blobtype=pdf");    // TODO - Investigate some 404-failures (THE DOCURLS belong to diff domain)
		else
			return null;
	}


	/////////// academic.microsoft.com ////////////////////
	public static String checkAndGetAcademicMicrosoftPageUrl(String initialAcademicMicrosoftUrl)
	{
		if ( initialAcademicMicrosoftUrl.contains("academic.microsoft") )
			return offlineRedirectToAcademicMicrosoftFinalPageUrl(initialAcademicMicrosoftUrl);
		else
			return null;
	}


	// https://academic.microsoft.com/#/detail/2084896083
	// https://academic.microsoft.com/paper/1585286892/related
	public static String offlineRedirectToAcademicMicrosoftFinalPageUrl(String initialAcademicMicrosoftUrl)
	{
		String idStr = null;
		Matcher academicMicrosoftIdMatcher = ACADEMIC_MICROSOFT_ID.matcher(initialAcademicMicrosoftUrl);
		if ( !academicMicrosoftIdMatcher.matches() )
			return null;

		try {
			idStr = academicMicrosoftIdMatcher.group(1);
		} catch (Exception e) { logger.error("", e); return null; }
		if ( (idStr != null) && !idStr.isEmpty() )
			return (academicMicrosoftFinalPageUrlBasePath + idStr + "?entityType=2");
		else
			return null;
	}


	public static void extractDocUrlFromAcademicMicrosoftJson(String urlId, String sourceUrl, String pageUrl, HttpURLConnection conn)
	{
		// There is a json with data containing the docUrl..
		// https://academic.microsoft.com/api/entity/1585286892?entityType=2

		String jsonData = null;
		if ( (jsonData = ConnSupportUtils.getHtmlString(conn, null)) == null ) {
			logger.warn("Could not retrieve the responseBody for pageUrl: " + pageUrl);
			UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Discarded in 'SpecialUrlsHandler.extractDocUrlFromAcademicMicrosoftJson' method, as there was a problem retrieving its HTML-code. Its contentType is: '" + conn.getContentType() + "'.", null, true);
			LoaderAndChecker.connProblematicUrls ++;
			return;
		}

		try {
			// Parse the jsonData
			JSONObject jObj = new JSONObject(jsonData); // Construct a JSONObject from the retrieved jsonData.

			JSONObject entityObject = jObj.getJSONObject("entity");
			//logger.debug("EntityObject: " + entityObject.toString());	// DEBUG!

			JSONArray sourceLinks = entityObject.getJSONArray("s");
			//logger.debug("SourceLinks: " + sourceLinks.toString());	// DEBUG!

			List<String> type999or0possibleDocUrlList = new ArrayList<>(sourceLinks.length());	// 1-4 links usually.

			for ( Object linkObject : sourceLinks ) {
				JSONObject linkJsonObject = (JSONObject) linkObject;
				//logger.debug("LinkJsonObject: " + linkJsonObject.toString());	// DEBUG!

				int sourceTypeNum = linkJsonObject.getInt("sourceType");
				//logger.debug("SourceTypeNum: " + sourceTypeNum);	// DEBUG!

				if ( sourceTypeNum == 3 ) {
					if ( verifyMicrosoftAcademicPossibleDocLink(urlId, sourceUrl, pageUrl, linkJsonObject.getString("link")) )
						return;
				}
				else if ( (sourceTypeNum == 999) || (sourceTypeNum == 0) ) {	// Keep the "999" and the "0" aside for now.
					// This ensures that we don't connect with a "999" or "0" type if we don't check the docUrl-guaranteed-type "3" first.
					String link = linkJsonObject.getString("link");
					if ( !link.contains("doi.org") )	// This gives only pageUrls for sure, do not connect with it.
						type999or0possibleDocUrlList.add(link);
				}
			}

			// If no type-3 link found or if it wasn't a docUrl or had conn-problems, then check if we got any 999 or 0 to connect with. (Java checks if the list is empty automatically.)
			for ( String type999or0possibleDocUrl : type999or0possibleDocUrlList )
				if ( verifyMicrosoftAcademicPossibleDocLink(urlId, sourceUrl, pageUrl, type999or0possibleDocUrl) )
					return;

			// If it has not "returned" already, then no DocUrl was found.
			logger.warn("No docUrl was extracted from the academic.microsoft jsonData for pageUrl: " + pageUrl);
			UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Discarded in 'SpecialUrlsHandler.extractDocUrlFromAcademicMicrosoftJson()' method, as no docUrl was extracted from the academic.microsoft jsonData.", null, true);
			PageCrawler.contentProblematicUrls ++;

		} catch ( JSONException je ) {	// In case any of the above "json-keys" was not found.
			logger.warn("JSON Exception was thrown while trying to retrieve the microsoft.academic docUrl: " + je.getMessage());
			UrlUtils.logQuadruple(urlId, sourceUrl, null, "unreachable", "Discarded in 'SpecialUrlsHandler.extractDocUrlFromAcademicMicrosoftJson()' method, as there was a JSON-problem while retrieving the microsoft.academic docUrl.", null, true);
			PageCrawler.contentProblematicUrls ++;
		}
	}


	private static boolean verifyMicrosoftAcademicPossibleDocLink(String urlId, String sourceUrl, String pageUrl, String possibleDocUrl)
	{
		//logger.debug("AcademicMicrosoft PossibleDocUrl: " + possibleDocUrl);	// DEBUG!

		if ( UrlUtils.docUrlsWithIDs.containsKey(possibleDocUrl) ) {    // If we got into an already-found docUrl, log it and return.
			logger.info("re-crossed docUrl found: < " + possibleDocUrl + " >");
			LoaderAndChecker.reCrossedDocUrls ++;
			if ( FileUtils.shouldDownloadDocFiles )
				UrlUtils.logQuadruple(urlId, sourceUrl, pageUrl, possibleDocUrl, UrlUtils.alreadyDownloadedByIDMessage + UrlUtils.docUrlsWithIDs.get(possibleDocUrl), null, false);
			else
				UrlUtils.logQuadruple(urlId, sourceUrl, pageUrl, possibleDocUrl, "", null, false);
			return true;
		}

		try {    // Check if it's a docUrl, if not, check for other possible ones.
			if ( HttpConnUtils.connectAndCheckMimeType(urlId, sourceUrl, possibleDocUrl, possibleDocUrl, null, false, true) )
				return true;
			// Else continue as there might be more than one possible docUrls.
		} catch ( Exception e ) {
			// No handling here, go check for other docUrls.
		}
		return false;
	}


	/////////// ntrs.nasa.gov ////////////////////
	public static String checkAndGetNasaDocUrl(String pageUrl)
	{
		if ( pageUrl.contains("ntrs.nasa.gov") && ! pageUrl.contains("api/") )
			return offlineRedirectToNasaDocUrl(pageUrl);
		else
			return null;
	}


	public static String offlineRedirectToNasaDocUrl(String nasaPageUrl)
	{
		String idStr = UrlUtils.getDocIdStr(nasaPageUrl, null);
		if ( idStr == null )
			return null;

		String citationPath = StringUtils.replace(nasaPageUrl, nasaBaseDomainPath, "", 1);
		citationPath = (citationPath.endsWith("/") ? citationPath : citationPath+"/");	// Make sure the "citationPath" has an ending slash.

		return (nasaBaseDomainPath + "api/" + citationPath + "downloads/" + idStr + ".pdf");
	}
}
