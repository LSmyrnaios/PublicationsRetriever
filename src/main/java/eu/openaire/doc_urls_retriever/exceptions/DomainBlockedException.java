package eu.openaire.doc_urls_retriever.exceptions;


import java.util.List;

/**
 * This class implements the new custom exception: "DomainBlockedException".
 * This exception is designed to be thrown when a domain is getting blocked while its page is crawled.
 * This way, the crawling of that page can stop immediately.
 * @author Lampros Smyrnaios
 */
public class DomainBlockedException extends Exception
{
	private String blockedDomain = null;

	private List<String> blockedDomains = null;

	public DomainBlockedException(String blockedDomain)
	{
		this.blockedDomain = blockedDomain;
	}

	public DomainBlockedException(List<String> blockedDomains)
	{
		this.blockedDomains = blockedDomains;
	}


	@Override
	public String getMessage()
	{
		if ( blockedDomain != null)
			return blockedDomain;
		else if ( blockedDomains != null )
			return blockedDomains.toString();
		else
			return null;
	}
}
