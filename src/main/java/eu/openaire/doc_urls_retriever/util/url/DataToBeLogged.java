package eu.openaire.doc_urls_retriever.util.url;

import org.apache.commons.lang3.StringUtils;


/**
 * This class is responsible to store the quadruple <urlId, sourceUrl, docUrl, errorCause> for it to be written in the outputFile.
 * @author Lampros Smyrnaios
 */
public class DataToBeLogged
{
    private String urlId;
    private String sourceUrl;
    private String docUrl;
	String wasUrlChecked, wasUrlValid, wasDocumentOrDatasetAccessible, didDocOrDatasetUrlCameFromSourceUrlDirectly;
    private String comment;   // This will be an emptyString, unless there is an error causing the docUrl to be unreachable.
	
	private static final StringBuilder strB = new StringBuilder(1000);
	
	public DataToBeLogged(String urlId, String sourceUrl, String docUrl, String wasUrlChecked, String wasUrlValid, String wasDocumentOrDatasetAccessible, String didDocOrDatasetUrlCameFromSourceUrlDirectly, String comment)
    {
        if ( urlId == null )
            urlId = "unretrievable";
        
        this.urlId = urlId;
        this.sourceUrl = escapeSourceUrl(sourceUrl);	// The input may have non-expected '\"', '\\' or even '\\\"' which will be unescaped by JsonObject and we have to re-escape them in the output.
        this.docUrl = docUrl;
		this.wasUrlChecked = wasUrlChecked;
		this.wasUrlValid = wasUrlValid;
		this.wasDocumentOrDatasetAccessible = wasDocumentOrDatasetAccessible;
		this.didDocOrDatasetUrlCameFromSourceUrlDirectly = didDocOrDatasetUrlCameFromSourceUrlDirectly;
		this.comment = comment;
	}
	
	
	/**
	 * This method, escapes the <backSlashes> and the <doubleQuotes> from the sourceUrl.
	 * When we read from jsonObjects, the string returns unescaped.
	 * Now, there are libraries for escaping and unescaping chars, like "org.apache.commons.text.StringEscapeUtils".
	 * But they can't handle the case where you want this: \"   to be this: \\\"   as they thing you are already satisfied what what you have.
	 * Tha might be true in general.. just not when you want to have a valid-jason-output.
	 * @param sourceUrl
	 * @return
	 */
	public static String escapeSourceUrl(String sourceUrl)
	{
		/*
			Here we might even have these in the input  <\\\"> which will be read by jsonObject as <\"> and we will have to re-make them <\\\"> in order to have a valid-json-output.
			http://www.scopus.com/record/display.url?eid=2-s2.0-82955208478&origin=resultslist&sort=plf-f&src=s&st1=aZZONI+r&nlo=&nlr=&nls=&sid=YfPXTZ5QQuqvNMHCo-geSvN%3a60&sot=b&sdt=cl&cluster=scoauthid%2c%227004337609%22%2ct%2bscosubtype%2c%22ar%22%2ct%2bscosubjabbr%2c%22MEDI%22%2ct%2c%22MULT%22%2ct&sl=21&s=AUTHOR-NAME%28aZZONI+r%29&relpos=0&relpos=0&searchTerm=AUTHOR-NAME(aZZONI r) AND ( LIMIT-TO(AU-ID,\\\"Azzoni, Roberto\\\" 7004337609) ) AND ( LIMIT-TO(DOCTYPE,\\\"ar\\\" ) ) AND ( LIMIT-TO(SUBJAREA,\\\"MEDI\\\" ) OR LIMIT-TO(SUBJAREA,\\\"MULT\\\" ) )
		 */
		
		// Escape backSlash.
		sourceUrl = StringUtils.replace(sourceUrl, "\\", "\\\\", -1);	// http://koara.lib.keio.ac.jp/xoonips/modules/xoonips/detail.php?koara_id=pdf\AN00150430-00000039--001
		
		// Escape doubleQuotes and return.
		return StringUtils.replace(sourceUrl, "\"", "\\\"", -1);	// https://jual.nipissingu.ca/wp-content/uploads/sites/25/2016/03/v10202.pdf" rel="
	}
    
    
    /**
     * This method returns this object in a jsonString. It does not need to be synchronized.
     * @return jsonString
     */
    public String toJsonString()
    {
    	if ( LoaderAndChecker.useIdUrlPairs ) {
			strB.append("{\"id\":\"").append(this.urlId);
			strB.append("\",\"sourceUrl\":\"").append(this.sourceUrl);
		}
    	else {	//When there are no IDs in the input file and there's no point in writing that they are "unretrieved" in the outputFile.
			strB.append("{\"sourceUrl\":\"").append(this.sourceUrl);
		}
		strB.append("\",\"docUrl\":\"").append(this.docUrl);
		strB.append("\",\"wasUrlChecked\":\"").append(this.wasUrlChecked);
		strB.append("\",\"wasUrlValid\":\"").append(this.wasUrlValid);
		strB.append("\",\"wasDocumentOrDatasetAccessible\":\"").append(this.wasDocumentOrDatasetAccessible);
		strB.append("\",\"didDocOrDatasetUrlCameFromSourceUrlDirectly\":\"").append(this.didDocOrDatasetUrlCameFromSourceUrlDirectly);
		strB.append("\",\"comment\":\"").append(this.comment).append("\"}");
		
        String jsonString = strB.toString();
        strB.setLength(0);	// Reset "StringBuilder" WITHOUT re-allocating, thus allocation happens only once for the whole execution.
        return jsonString;
    }
    
}
