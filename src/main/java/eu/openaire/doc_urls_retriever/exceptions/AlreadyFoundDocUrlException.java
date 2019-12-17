package eu.openaire.doc_urls_retriever.exceptions;


/**
 * This class implements the new custom exception: "AlreadyFoundDocUrlException".
 * This exception is thrown when there an already-found DocUrl is seen.
 * Normally this case can by handled without an exception,
 * but in the case which the docUrl is discovered during redirection-packs, we need a fast way out.
 * @author Lampros Smyrnaios
 */
public class AlreadyFoundDocUrlException extends Exception
{
	public AlreadyFoundDocUrlException() {}
}
