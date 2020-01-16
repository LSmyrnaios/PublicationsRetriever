package eu.openaire.doc_urls_retriever.exceptions;


/**
 * This class implements the new custom exception: "DynamicInternalLinksFoundException".
 * This exception is designed to be thrown when an internal link (meta-link or a simple one) is a dynamic link.
 * Then, the domain gets blocked, since it's not possible to connect with dynamic links. Their values are taken from the server, in which we have no access.
 * @author Lampros Smyrnaios
 */
public class DynamicInternalLinksFoundException extends Exception {

    public DynamicInternalLinksFoundException() {}
}
