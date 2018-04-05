package eu.openaire.doc_urls_retriever.util.url;

import eu.openaire.doc_urls_retriever.util.file.FileUtils;



/**
 * This class is responsible to store the triple <sourceUrl, docUrl, errorCause> for it to be written in the outputFile.
 * @author Lampros A. Smyrnaios
 */
public class TripleToBeLogged
{
    private String sourceUrl;
    private String docUrl;
    private String comment;   // This will be an emptyString, unless there is an error causing the docUrl to be unreachable.


    public TripleToBeLogged(String sourceUrl, String docUrl, String comment)
    {
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
        return  FileUtils.jsonEncoder(this.sourceUrl, this.docUrl, this.comment);  // It may be null.
    }

}
