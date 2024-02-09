package eu.openaire.publications_retriever.exceptions;


/**
 * @author Lampros Smyrnaios
 */
public class DocLinkUnavailableException extends Exception {

	private String errorMsg = null;

	public DocLinkUnavailableException(String errorMsg)
	{
		this.errorMsg = errorMsg;
	}

	@Override
	public String getMessage()
	{
		return errorMsg;
	}
}
