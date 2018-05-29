package eu.openaire.doc_urls_retriever.exceptions;


/**
 * This class implements the new custom exception: "JavaScriptDocLinkFoundException".
 * This exception is thrown when we find a javaScriptDocLink upon links-retrieval.
 * It is used in order to avoid checking any other links inside the javaScript-webPage.
 * @author Lampros A. Smyrnaios
 */
public class JavaScriptDocLinkFoundException extends Exception
{
	private String javaScriptDocLink = null;
	
	public JavaScriptDocLinkFoundException(String javaScriptDocLink)
	{
		this.javaScriptDocLink = javaScriptDocLink;
	}
	
	@Override
	public String getMessage()
	{
		return javaScriptDocLink;
	}
}
