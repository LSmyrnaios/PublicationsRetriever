package eu.openaire.publications_retriever.crawler;


import eu.openaire.publications_retriever.util.http.ConnSupportUtils;
import eu.openaire.publications_retriever.util.http.HttpConnUtils;
import eu.openaire.publications_retriever.util.url.LoaderAndChecker;
import eu.openaire.publications_retriever.util.url.UrlUtils;
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

	private static final String nasaBaseDomainPath = "https://ntrs.nasa.gov/";



	public static String checkAndHandleSpecialUrls(String resourceUrl) throws RuntimeException
	{
		String updatedUrl = null;

		if ( (updatedUrl = checkAndGetScienceDirectUrl(resourceUrl)) != null ) {
			//logger.debug("ScienceDirect-PageURL to try: " + updatedUrl);	// DEBUG!
			resourceUrl = updatedUrl;
		}
		else if ( (updatedUrl = checkAndGetEuropepmcDocUrl(resourceUrl)) != null ) {
			//logger.debug("Europepmc-PageURL: " + resourceURL + " to possible-docUrl: " + updatedUrl);	// DEBUG!
			resourceUrl = updatedUrl;
		}
		else if ( (updatedUrl = checkAndGetNasaDocUrl(resourceUrl)) != null ) {
			//logger.debug("Nasa-PageURL: " + resourceURL + " to possible-docUrl: " + updatedUrl);	// DEBUG!
			resourceUrl = updatedUrl;
		}
		else if ( (updatedUrl = checkAndGetFrontiersinDocUrl(resourceUrl)) != null ) {
			//logger.debug("Frontiersin-PageURL: " + resourceURL + " to possible-docUrl: " + updatedUrl);	// DEBUG!
			resourceUrl = updatedUrl;
		}
		else if ( (updatedUrl = checkAndHandlePsyarxiv(resourceUrl)) != null )
			//logger.debug("Psyarxiv-PageURL: " + resourceURL + " to possible-docUrl: " + updatedUrl);	// DEBUG!
			resourceUrl = updatedUrl;
		else
			resourceUrl = checkAndHandleDergipark(resourceUrl);	// It returns the same url if nothing was handled.

		return resourceUrl;
	}


	/////////// sciencedirect.com //// "linkinghub.elsevier.com //// api.elsevier.com ////////////////////
	/**
	 * This method checks if the given url belongs to the "scienceDirect-family"-urls and if so it handles it.
	 * If the given url is not a kindOf-scienceDirect-url, then it
	 * @param pageUrl
	 * @return
	 */
	public static String checkAndGetScienceDirectUrl(String pageUrl)
	{
		boolean wasLinkinghubElsevier = false;
		if ( pageUrl.contains("linkinghub.elsevier.com") || pageUrl.contains("api.elsevier.com") )	// Avoid plain "elsevier.com"-rule as there are: "www.elsevier.com" and "journals.elsevier.com" which don't give docUrls and are handled differently.
		{
			// Offline-redirect Elsevier to ScienceDirect.
			// The "linkinghub.elsevier.com" urls have a javaScript redirect inside which we are not able to handle without doing html scraping.
			// The "api.elsevier.com" contains xml which contains the docUrl, but we can go there faster by "offlineRedirect".
			String idStr = UrlUtils.getDocIdStr(pageUrl, null);
			if ( idStr != null ) {
				idStr = StringUtils.replace(idStr, "PII:", "", 1);	// In case of an "api.elsevier.com" pageUrl.
				pageUrl = scienceDirectBasePath + idStr;
			}
			else
				throw new RuntimeException("Problem when handling the \"elsevier\"-family-url: " + pageUrl);

			//logger.debug("Produced ScienceDirect-url: " + pageUrl);	// DEBUG!
			wasLinkinghubElsevier = true;
		}

		if ( wasLinkinghubElsevier || pageUrl.contains("sciencedirect.com") ) {
			if ( !pageUrl.endsWith("/pdf") )
				return (pageUrl + (pageUrl.endsWith("/") ? "pdf" : "/pdf"));    // Add a "/pdf" in the end. That will indicate we are asking for the docUrl.
			else
				return pageUrl;	// It's already a docUrl..
		} else
			return null;	// It's from another domain..
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


	private static boolean verifyMicrosoftAcademicPossibleDocLink(String urlId, String sourceUrl, String pageUrl, String possibleDocUrl)
	{
		//logger.debug("AcademicMicrosoft PossibleDocUrl: " + possibleDocUrl);	// DEBUG!

		if ( UrlUtils.docOrDatasetUrlsWithIDs.containsKey(possibleDocUrl) ) {    // If we got into an already-found docUrl, log it and return.
			ConnSupportUtils.handleReCrossedDocUrl(urlId, sourceUrl, pageUrl, possibleDocUrl, false);
			return true;
		}

		try {    // Check if it's a docUrl, if not, check for other possible ones.
			if ( HttpConnUtils.connectAndCheckMimeType(urlId, sourceUrl, pageUrl, possibleDocUrl, null, false, true) )
				return true;
			// Else continue as there might be more than one possible docUrls.
		} catch ( Exception e ) {
			// No handling here, go check for other docUrls.
		}
		return false;
	}


	/////////// ntrs.nasa.gov ////////////////////
	public static String checkAndGetNasaDocUrl(String nasaPageUrl)
	{
		if ( nasaPageUrl.contains("ntrs.nasa.gov") && ! nasaPageUrl.contains("api/") )
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

}
