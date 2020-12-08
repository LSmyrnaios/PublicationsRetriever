package eu.openaire.doc_urls_retriever.exceptions;


/**
 * This class implements the new custom exception: "DocLinkFoundException".
 * This exception is thrown when we find a DocLink (javaScriptDocLink or not), upon links-retrieval.
 * It is used in order to avoid checking any other links inside the webPage.
 * @author Lampros Smyrnaios
 */
public class DocLinkFoundException extends Exception
{
	private String docLink = null;
	
	public DocLinkFoundException(String docLink)
	{
		this.docLink = docLink;
	}
	
	@Override
	public String getMessage()
	{
		return docLink;
	}
}
