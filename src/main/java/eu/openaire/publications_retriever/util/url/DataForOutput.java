package eu.openaire.publications_retriever.util.url;

import eu.openaire.publications_retriever.util.args.ArgsUtils;
import org.apache.commons.lang3.Strings;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.LinkedHashMap;


/**
 * This class is responsible to store the quadruple <urlId, sourceUrl, docOrDatasetUrl, wasUrlChecked, wasUrlValid, wasDocumentOrDatasetAccessible, wasDirectLink, errorCause / comment> for it to be written in the outputFile.
 * @author Lampros Smyrnaios
 */
public class DataForOutput
{
    private String urlId;
    private String sourceUrl, pageUrl, docOrDatasetUrl;
	private String wasUrlChecked, wasUrlValid, wasDocumentOrDatasetAccessible, wasDirectLink, couldRetry;
	private String hash;
	private Long size;
	private String mimeType;
	private String filePath;
	private String error;

	private static final Logger logger = LoggerFactory.getLogger(DataForOutput.class);

	public DataForOutput(String urlId, String sourceUrl, String pageUrl, String docOrDatasetUrl, String wasUrlChecked, String wasUrlValid, String wasDocumentOrDatasetAccessible, String wasDirectLink, String couldRetry, String hash, Long size, String mimeType, String filePath, String error)
    {
		if ( urlId == null )
            urlId = "unretrievable";
        
        this.urlId = urlId;
        this.sourceUrl = escapeUrl(sourceUrl);	// The input may have non-expected '\"', '\\' or even '\\\"' which will be unescaped by JsonObject, and we have to re-escape them in the output.
        this.pageUrl = escapeUrl(pageUrl);
		this.docOrDatasetUrl = docOrDatasetUrl;
		this.wasUrlChecked = wasUrlChecked;
		this.wasUrlValid = wasUrlValid;
		this.wasDocumentOrDatasetAccessible = wasDocumentOrDatasetAccessible;
		this.wasDirectLink = wasDirectLink;
		this.couldRetry = couldRetry;
		this.hash = hash;
		this.size = size;
		this.mimeType = mimeType;
		this.filePath = filePath;
		this.error = error;
	}
	
	
	/**
	 * This method, escapes the <backSlashes> and the <doubleQuotes> from the sourceUrl and the pageUrl (in case it is the same).
	 * When we read from jsonObjects, the string returns unescaped.
	 * Now, there are libraries for escaping and unescaping chars, like "org.apache.commons.text.StringEscapeUtils".
	 * But they can't handle the case where you want this: \"   to be this: \\\"   as they think you are already satisfied what you have.
	 * Tha might be true in general.. just not when you want to have a valid-jason-output.
	 * @param url
	 * @return
	 */
	public static String escapeUrl(String url)
	{
		/*
			Here we might even have these in the input  <\\\"> which will be read by jsonObject as <\"> and we will have to re-make them <\\\"> in order to have a valid-json-output.
			http://www.scopus.com/record/display.url?eid=2-s2.0-82955208478&origin=resultslist&sort=plf-f&src=s&st1=aZZONI+r&nlo=&nlr=&nls=&sid=YfPXTZ5QQuqvNMHCo-geSvN%3a60&sot=b&sdt=cl&cluster=scoauthid%2c%227004337609%22%2ct%2bscosubtype%2c%22ar%22%2ct%2bscosubjabbr%2c%22MEDI%22%2ct%2c%22MULT%22%2ct&sl=21&s=AUTHOR-NAME%28aZZONI+r%29&relpos=0&relpos=0&searchTerm=AUTHOR-NAME(aZZONI r) AND ( LIMIT-TO(AU-ID,\\\"Azzoni, Roberto\\\" 7004337609) ) AND ( LIMIT-TO(DOCTYPE,\\\"ar\\\" ) ) AND ( LIMIT-TO(SUBJAREA,\\\"MEDI\\\" ) OR LIMIT-TO(SUBJAREA,\\\"MULT\\\" ) )
		 */
		
		// Escape backSlash.
		url = Strings.CS.replace(url, "\\", "\\\\", -1);	// http://koara.lib.keio.ac.jp/xoonips/modules/xoonips/detail.php?koara_id=pdf\AN00150430-00000039--001
		
		// Escape doubleQuotes and return.
		return Strings.CS.replace(url, "\"", "\\\"", -1);	// https://jual.nipissingu.ca/wp-content/uploads/sites/25/2016/03/v10202.pdf" rel="
	}
    
    
    /**
     * This method returns this object in a valid jsonString. It does not need to be synchronized.
     * @return jsonString
     */
    public String toJsonString()
    {
		JSONObject jsonObject = new JSONObject();
		try {	//	Change the underlying structure to a "LinkedHashMap", in order to avoid unordered-representation.
			Field changeMap = jsonObject.getClass().getDeclaredField("map");
			changeMap.setAccessible(true);
			changeMap.set(jsonObject, new LinkedHashMap<>());
			changeMap.setAccessible(false);
		} catch (Exception e) {
			logger.warn("Could not create an ordered JSONObject, so continuing with an unordered one. Exception msg: " + e.getMessage());
			jsonObject = new JSONObject();	// In this case, just create a new "normal" JSONObject.
		}

		try {
			if ( LoaderAndChecker.useIdUrlPairs ) {
				jsonObject.put("id", this.urlId);
			}
			jsonObject.put("sourceUrl", this.sourceUrl);
			jsonObject.put("pageUrl", this.pageUrl);
			jsonObject.put(ArgsUtils.targetUrlType, this.docOrDatasetUrl);
			jsonObject.put("wasUrlChecked", this.wasUrlChecked);
			jsonObject.put("wasUrlValid", this.wasUrlValid);
			jsonObject.put("wasDocumentOrDatasetAccessible", this.wasDocumentOrDatasetAccessible);
			jsonObject.put("wasDirectLink", this.wasDirectLink);
			jsonObject.put("couldRetry", this.couldRetry);
			jsonObject.put("fileHash", String.valueOf(this.hash));	// The hash may be null,  convert it to the string "null".
			jsonObject.put("fileSize", String.valueOf(this.size));	// Convert Long to String, otherwise the output-JSON will be broken. It may be null!
			jsonObject.put("mimeType", this.mimeType);
			jsonObject.put("filePath", this.filePath);
			jsonObject.put("error", this.error);
		} catch (JSONException je) {
			// Keep the jsonObject with what it has till now.. plus a "special" comment.
			jsonObject.put("comment", "There was a problem creating this JSON with the right values.");
			logger.warn("Invalid JsonOutput will be written as: " + jsonObject);	// Depending on which json-key is the pre-error one, we will know what caused the error (the one right after).
		}

        return jsonObject.toString();
    }

	public String getUrlId() {
		return urlId;
	}

	public void setUrlId(String urlId) {
		this.urlId = urlId;
	}

	public String getSourceUrl() {
		return sourceUrl;
	}

	public void setSourceUrl(String sourceUrl) {
		this.sourceUrl = sourceUrl;
	}

	public String getPageUrl() {
		return pageUrl;
	}

	public void setPageUrl(String pageUrl) {
		this.pageUrl = pageUrl;
	}

	public String getDocOrDatasetUrl() {
		return docOrDatasetUrl;
	}

	public void setDocOrDatasetUrl(String docOrDatasetUrl) {
		this.docOrDatasetUrl = docOrDatasetUrl;
	}

	public String getWasUrlChecked() {
		return wasUrlChecked;
	}

	public void setWasUrlChecked(String wasUrlChecked) {
		this.wasUrlChecked = wasUrlChecked;
	}

	public String getWasUrlValid() {
		return wasUrlValid;
	}

	public void setWasUrlValid(String wasUrlValid) {
		this.wasUrlValid = wasUrlValid;
	}

	public String getWasDocumentOrDatasetAccessible() {
		return wasDocumentOrDatasetAccessible;
	}

	public void setWasDocumentOrDatasetAccessible(String wasDocumentOrDatasetAccessible) {
		this.wasDocumentOrDatasetAccessible = wasDocumentOrDatasetAccessible;
	}

	public String getWasDirectLink() {
		return wasDirectLink;
	}

	public void setWasDirectLink(String wasDirectLink) {
		this.wasDirectLink = wasDirectLink;
	}

	public String getCouldRetry() {
		return couldRetry;
	}

	public void setCouldRetry(String couldRetry) {
		this.couldRetry = couldRetry;
	}

	public String getHash() {
		return hash;
	}

	public void setHash(String hash) {
		this.hash = hash;
	}

	public Long getSize() {
		return size;
	}

	public void setSize(Long size) {
		this.size = size;
	}

	public String getMimeType() {
		return mimeType;
	}

	public void setMimeType(String mimeType) {
		this.mimeType = mimeType;
	}

	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}
}
