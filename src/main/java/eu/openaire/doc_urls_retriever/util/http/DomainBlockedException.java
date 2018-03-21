package eu.openaire.doc_urls_retriever.util.http;


/**
 * This class implements the new custom exception: "DomainBlockedException".
 * This exception is designed to be thrown when a domain is getting blocked while its page is crawled.
 * This way, the crawling of that page can stop immediately.
 * @author Lampros A. Smyrnaios
 */
public class DomainBlockedException extends Exception
{
	public DomainBlockedException () { }
}
