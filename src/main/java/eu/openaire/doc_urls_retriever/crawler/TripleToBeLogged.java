package eu.openaire.doc_urls_retriever.crawler;

import eu.openaire.doc_urls_retriever.util.file.FileUtils;


/**
 * This class is responsible to store the triple <sourceUrl, docUrl, errorCause> for it to be written in the outputFile.
 */
public class TripleToBeLogged
{
    private String sourceUrl;
    private String docUrl;
    private String errorCause = null;   // It will be null unless there is an error causing the docUrl to be unreachable.


    public TripleToBeLogged(String sourceUrl, String docUrl, String errorCause)
    {
        this.sourceUrl = sourceUrl;
        this.docUrl = docUrl;
        this.errorCause = errorCause;
    }


    /**
     * This method returns this object in a jsonString.
     * It uses the "FileUtils.jsonEncoder()" to encode the members of this class into a jsonLine.
     * It returns that jsonLine, otherwise, null if there was an encoding error.
     * @return jsonString
     */
    public String toJsonString()
    {
        return FileUtils.jsonEncoder(this.sourceUrl, this.docUrl, this.errorCause);  // It may be null.
    }

}
