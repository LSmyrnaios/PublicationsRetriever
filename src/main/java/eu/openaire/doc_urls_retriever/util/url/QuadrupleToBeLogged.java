package eu.openaire.doc_urls_retriever.util.url;

import eu.openaire.doc_urls_retriever.util.file.FileUtils;



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
            urlId = "Unretrievable";
        this.urlId = urlId;
        this.sourceUrl = sourceUrl;
        this.docUrl = docUrl;
        this.comment = comment;
    }


    /**
     * This method returns this object in a jsonString.
     * It uses the "FileUtils.jsonEncoder()" to encode the members of this class into a jsonLine.
     * It returns that jsonLine, otherwise, null if there was an encoding error.
     * @return jsonString
     */
    public String toJsonString()
    {
        return  FileUtils.jsonEncoder(this.urlId, this.sourceUrl, this.docUrl, this.comment);  // It may return null.
    }

}
