package eu.openaire.publications_retriever.util.http;

import java.io.BufferedReader;

public class DetectedContentType {

    public String detectedContentType;
    public String firstHtmlLine;
    public BufferedReader bufferedReader;

    DetectedContentType(String detectedContentType, String firstHtmlLine, BufferedReader bufferedReader) {
        this.detectedContentType = detectedContentType;
        this.firstHtmlLine = firstHtmlLine;
        this.bufferedReader = bufferedReader;
    }
}
