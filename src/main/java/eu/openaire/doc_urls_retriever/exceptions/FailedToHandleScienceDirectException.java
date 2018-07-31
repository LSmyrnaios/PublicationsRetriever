package eu.openaire.doc_urls_retriever.exceptions;


/**
 * This class implements the new custom exception: "FailedToHandleScienceDirectException".
 * This exception is thrown when there is any kind of problem when handling scienceDirect-urls.
 * The calling method can decide what action to take, for example: log the url.
 * @author Lampros A. Smyrnaios
 */
public class FailedToHandleScienceDirectException extends Exception {
	
	public FailedToHandleScienceDirectException() {}
}
