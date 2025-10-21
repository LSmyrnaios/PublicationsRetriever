package eu.openaire.publications_retriever.machine_learning;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import eu.openaire.publications_retriever.crawler.PageCrawler;
import eu.openaire.publications_retriever.exceptions.DocLinkFoundException;
import eu.openaire.publications_retriever.exceptions.DocLinkInvalidException;
import eu.openaire.publications_retriever.util.args.ArgsUtils;
import eu.openaire.publications_retriever.util.file.FileUtils;
import eu.openaire.publications_retriever.util.url.UrlUtils;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Lampros Smyrnaios
 */
public class PageStructureMLA {

	private static final Logger logger = LoggerFactory.getLogger(PageStructureMLA.class);


	public static AtomicInteger structurePredictedDocLinks = new AtomicInteger(0);
	public static AtomicInteger structureValidatedDocLinks = new AtomicInteger(0);
	public static final SetMultimap<String, String> pagePathWithDocOrDatasetUrlStructure = Multimaps.synchronizedSetMultimap(HashMultimap.create());	// Holds multiple values for any key, if a docPagePath(key) has many different docUrlPaths(values) for doc links.

	private static final int NUM_ELEMENTS_IN_STRUCTURE = 50;


	public static void addStructureOfDocUrlInMap(String pageUrl, String docLinkStructure)
	{
		String pagePath = UrlUtils.getPathStr(pageUrl, null);
		if ( pagePath != null )
			pagePathWithDocOrDatasetUrlStructure.put(pagePath, docLinkStructure);
	}


	/**
	 * This method returns a list of Strings, containing the tag + class of each "step" of the page structure.
	 * We need the class, along the tag, because the pdf-link may be inside a list and having the same "tag"-structure as another non-pdf link.
	 * */
	public static String getPageTagAndClassStructureForElement(Element element)
	{
		final StringBuilder stringBuilder = new StringBuilder(1000);
		int elementsCount = 0;
		do {
			// Make sure the "tag" and "class" are "trimmed", in order to avoid mismatch by future addition or removal of fault spaces.
			stringBuilder.append(element.tagName().trim());
			String className = element.className().trim();
			if ( !className.isEmpty() )	// The "class" may not exist for some elements.
				stringBuilder.append("_").append(className);
			stringBuilder.append(FileUtils.endOfLine);
			elementsCount ++;
		} while ( ((elementsCount <= NUM_ELEMENTS_IN_STRUCTURE) && (element = element.parent()) != null) );	// Climb up to the ancestor up to 50 elements.

		return stringBuilder.toString();
	}


	public static void predictDocOrDatasetLink(String pageUrl, Elements elementLinksOnPage) throws DocLinkFoundException, DocLinkInvalidException
	{
		// Before trying to extract the internal-links and evaluate them, let's check whether we have any docUrl-structure-info for this page-path.
		// If we do, then let's try to locate the docUrl in this page, based on previous location-data.
		String pagePath = UrlUtils.getPathStr(pageUrl, null);
		if ( pagePath == null )
			return;

		Set<String> storedStructuresForPagePath = pagePathWithDocOrDatasetUrlStructure.get(pagePath);
		if ( storedStructuresForPagePath.isEmpty() )	// No structures have been stored for previously found docOrDatasetUrls of this page-path.
			return;

		String docLink;
		for ( Element el : elementLinksOnPage ) {	// These elements are only link-related, as selected by Jsoup.
			String structure = getPageTagAndClassStructureForElement(el);
			if ( storedStructuresForPagePath.contains(structure) )
			{
				if ( logger.isTraceEnabled() )
					logger.trace("Got a hit for pagePath \"" + pagePath + "\"!\n" + el);
				// Take this url, and check it by using the "verifyDocLink()" method, after throwing the related exception below.
				if ( (docLink = PageCrawler.getInternalLink(pageUrl, el)) != null ) {
					// The structure of this link is already stored in the map, so just verify the link and move to the next page.
					throw new DocLinkFoundException(docLink, structure, true);
				} else
					logger.warn("No internal-" + ArgsUtils.targetUrlType + " could be extracted from the element!");

				// If the right doc-structure was found, but the embedded link was not found, then return immediately and check the internal-links one by one.
				return;
			}
		}

		logger.warn("No " + ArgsUtils.targetUrlType + " was found, by comparing the html-structure of each element with previously stored ones, for pagePath: " + pagePath);
		// It is possible that this record is either closed-access or non-provided by the page (even if the page returns 200-OK).
		// However, the page-path may be a very high-level path and the "docId" part may be showing different type of record, having different structure.
		// Continue checking the links.
	}

}
