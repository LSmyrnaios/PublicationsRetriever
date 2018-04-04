package eu.openaire.doc_urls_retriever.exceptions;


/**
 * This class implements the new custom exception: "DomainWithUnsupportedHEADmethodException".
 * This exception is designed to be thrown when the domain is caught to not support HTTP HEAD method and we don't want to .
 * This way, the crawling of that page can stop immediately.
 * @author Lampros A. Smyrnaios
 */
public class DomainWithUnsupportedHEADmethodException extends Exception
{
	public DomainWithUnsupportedHEADmethodException() { }
}
