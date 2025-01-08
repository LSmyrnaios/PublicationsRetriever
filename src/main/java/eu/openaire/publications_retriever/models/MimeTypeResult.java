package eu.openaire.publications_retriever.models;


/**
 * @author Lampros Smyrnaios
 */
public class MimeTypeResult {

	String mimeType;
	String category;

	public MimeTypeResult(String mimeType, String category) {
		this.mimeType = mimeType;
		this.category = category;
	}

	public String getMimeType() {
		return mimeType;
	}

	public void setMimeType(String mimeType) {
		this.mimeType = mimeType;
	}

	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}

	@Override
	public String toString() {
		return "MimeTypeResult{" + "mimeType='" + mimeType + '\'' + ", category='" + category + '\'' + '}';
	}
}
