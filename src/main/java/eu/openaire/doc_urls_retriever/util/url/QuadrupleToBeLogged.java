package eu.openaire.doc_urls_retriever.util.url;



/**
 * This class is responsible to store the quadruple <urlId, sourceUrl, docUrl, errorCause> for it to be written in the outputFile.
 * @author Lampros A. Smyrnaios
 */
public class QuadrupleToBeLogged
{
    private String urlId;
    private String sourceUrl;
    private String docUrl;
    private String comment;   // This will be an emptyString, unless there is an error causing the docUrl to be unreachable.


    public QuadrupleToBeLogged(String urlId, String sourceUrl, String docUrl, String comment)
    {
        if ( urlId == null )
            urlId = "unretrievable";
        
        this.urlId = urlId;
        this.sourceUrl = sourceUrl;
        this.docUrl = docUrl;
        this.comment = comment;
    }


    /**
     * This method returns this object in a jsonString.
     * @return jsonString
     */
    public String toJsonString()
    {
        StringBuilder strB = new StringBuilder(400);
        
        strB.append("{\"id\":\"").append(this.urlId).append("\",");
        strB.append("\"sourceUrl\":\"").append(this.sourceUrl).append("\",");
        strB.append("\"docUrl\":\"").append(this.docUrl).append("\",");
        strB.append("\"comment\":\"").append(this.comment).append("\"}");
        
        return strB.toString();
    }

}
