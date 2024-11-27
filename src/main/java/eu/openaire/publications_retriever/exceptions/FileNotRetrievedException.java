package eu.openaire.publications_retriever.exceptions;


/**
 * This class implements the new custom exception: "FileNotRetrievedException".
 * This exception is used to signal a failure in retrieving a docFile.
 * @author Lampros Smyrnaios
 */
public class FileNotRetrievedException extends Exception
{
	public FileNotRetrievedException()	{}

	private String errorMessage = null;

	public FileNotRetrievedException(String errorMessage) { this.errorMessage = errorMessage; }

	@Override
	public String getMessage() { return errorMessage; }
}
