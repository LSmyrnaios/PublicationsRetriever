package eu.openaire.publications_retriever.util.file;


/**
 * @author Lampros Smyrnaios
 */
public class IdUrlTuple {

    // Use "public" to avoid the overhead of calling successors, this is a temporal class anyway.
    public String id;
    public String url;

    public IdUrlTuple(String id, String url) {
        this.id = id;
        this.url = url;
    }
}
