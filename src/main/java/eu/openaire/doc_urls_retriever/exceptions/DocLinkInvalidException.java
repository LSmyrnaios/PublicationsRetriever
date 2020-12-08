package eu.openaire.doc_urls_retriever.exceptions;


/**
 * @author Lampros Smyrnaios
 */
public class DocLinkInvalidException extends Exception
{
    private String invalidDocLink = null;

    public DocLinkInvalidException(String invalidDocLink)
    {
        this.invalidDocLink = invalidDocLink;
    }

    @Override
    public String getMessage()
    {
        return invalidDocLink;
    }
}
