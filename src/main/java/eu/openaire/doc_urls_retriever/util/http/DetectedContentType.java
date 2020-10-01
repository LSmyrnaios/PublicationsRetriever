package eu.openaire.doc_urls_retriever.util.http;

public class DetectedContentType {

    String detectedContentType;
    String firstHtmlLine;

    DetectedContentType(String detectedContentType, String firstHtmlLine) {
        this.detectedContentType = detectedContentType;
        this.firstHtmlLine = firstHtmlLine;
    }
}
