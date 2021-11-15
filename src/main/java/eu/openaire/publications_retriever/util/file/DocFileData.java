package eu.openaire.publications_retriever.util.file;

import java.io.File;

public class DocFileData {

    private File docFile;
    private String hash;
    private Long size;
    private String location;


    public DocFileData(File docFile, String hash, Long size, String location) {
        this.docFile = docFile;
        this.hash = hash;
        this.size = size;
        this.location = location;
    }

    public File getDocFile() {
        return docFile;
    }

    public void setDocFile(File docFile) {
        this.docFile = docFile;
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

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    @Override
    public String toString() {
        return "DocFileData{" +
                "docFile=" + docFile +
                ", hash='" + hash + '\'' +
                ", size=" + size +
                ", location='" + location + '\'' +
                '}';
    }
}
