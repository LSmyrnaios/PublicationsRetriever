package eu.openaire.publications_retriever.exceptions;


/**
 * This class implements the new custom exception: "DocLinkFoundException".
 * This exception is thrown when we find a DocLink (javaScriptDocLink or not), upon links-retrieval.
 * It is used in order to avoid checking any other links inside the webPage.
 * @author Lampros Smyrnaios
 */
public class DocLinkFoundException extends Exception
{
	private String docLink = null;

	private String pageTagAndClassStructureForElement = null;

	private boolean predictedByStructureMLA = false;


	public DocLinkFoundException(String docLink, String pageTagAndClassStructureForElement, boolean predictedByStructureMLA)
	{
		this.docLink = docLink;
		this.pageTagAndClassStructureForElement = pageTagAndClassStructureForElement;
		this.predictedByStructureMLA = predictedByStructureMLA;
	}

	@Override
	public String getMessage()
	{
		return docLink;
	}

	public String getPageTagAndClassStructureForElement() {
		return pageTagAndClassStructureForElement;
	}

	public boolean isPredictedByStructureMLA() {
		return predictedByStructureMLA;
	}

}
