package eu.openaire.publications_retriever.crawler;


import eu.openaire.publications_retriever.util.http.ConnSupportUtils;
import eu.openaire.publications_retriever.util.http.HttpConnUtils;
import eu.openaire.publications_retriever.util.url.LoaderAndChecker;
import eu.openaire.publications_retriever.util.url.UrlUtils;
import org.apache.commons.lang3.StringUtils;
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

	private static final String europepmcPageUrlBasePath = "https://europepmc.org/backend/ptpmcrender.fcgi?accid=";

	private static final String nasaBaseDomainPath = "https://ntrs.nasa.gov/";



	public static String checkAndHandleSpecialUrls(String resourceUrl) throws RuntimeException
	{
		String updatedUrl = null;

		if ( (updatedUrl = checkAndGetEuropepmcDocUrl(resourceUrl)) != null ) {
			//logger.debug("Europepmc-PageURL: " + resourceURL + " to possible-docUrl: " + updatedUrl);	// DEBUG!
			resourceUrl = updatedUrl;
		}
		else if ( (updatedUrl = checkAndDowngradeManuscriptElsevierUrl(resourceUrl)) != null ) {
			//logger.debug("ManuscriptElsevier-URL: " + resourceURL + " to acceptable-Url: " + updatedUrl);	// DEBUG!
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
