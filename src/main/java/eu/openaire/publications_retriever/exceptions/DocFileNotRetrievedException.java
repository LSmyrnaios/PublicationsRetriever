package eu.openaire.publications_retriever.exceptions;


/**
 * This class implements the new custom exception: "DocFileNotRetrievedException".
 * This exception is used to signal a failure in retrieving a docFile.
 * @author Lampros Smyrnaios
 */
public class DocFileNotRetrievedException extends Exception
{
	public DocFileNotRetrievedException()	{}

	private String errorMessage = null;

	public DocFileNotRetrievedException(String errorMessage) { this.errorMessage = errorMessage; }

	@Override
	public String getMessage() { return errorMessage; }
}
