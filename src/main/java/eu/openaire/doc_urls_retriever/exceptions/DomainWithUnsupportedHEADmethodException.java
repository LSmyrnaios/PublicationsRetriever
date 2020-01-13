package eu.openaire.doc_urls_retriever.exceptions;


/**
 * This class implements the new custom exception: "DomainWithUnsupportedHEADmethodException".
 * This exception is designed to be thrown when the domain is caught to not support HTTP HEAD method and we don't want to continue connecting to it's internal links.
 * The possible-docInternalLinks will be connected with "GET" method by default, so the exclusion promoted by this exception is of low risk.
 * Note that when the exception is thrown, we just return from the "visitPage()"-method , we do not block the domain, since an inputUrl from this domain might be a docUrl which we don't want to miss.
 * ...
 * @author Lampros Smyrnaios
 */
public class DomainWithUnsupportedHEADmethodException extends Exception
{
	public DomainWithUnsupportedHEADmethodException() { }
}
