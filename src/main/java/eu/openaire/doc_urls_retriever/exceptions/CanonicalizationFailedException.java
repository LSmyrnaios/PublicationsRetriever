package eu.openaire.doc_urls_retriever.exceptions;


/**
 * This class implements the new custom exception: "CanonicalizationFailedException".
 * This exception is thrown when there is a canonicalization problem with a url.
 * The calling method which receives this exception can decide if there's sth more to be done with this url..
 * or if the program should continue with the next url or with the next id-batch.
 * ~~~~~~~ It's not used at the moment. ~~~~~~
 * @author Lampros Smyrnaios
 */
public class CanonicalizationFailedException extends Exception
{
	public CanonicalizationFailedException() {}
}
