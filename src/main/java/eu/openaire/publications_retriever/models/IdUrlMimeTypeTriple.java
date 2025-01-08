package eu.openaire.publications_retriever.models;

public class IdUrlMimeTypeTriple {

	// Use "public" to avoid the overhead of calling successors, this is a temporal class anyway.
	public String id;
	public String url;
	public String mimeType;

	public IdUrlMimeTypeTriple(String id, String url, String mimeType) {
		this.id = id;
		this.url = url;
		this.mimeType = mimeType;
	}

}
