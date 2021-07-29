package eu.openaire.publications_retriever.util.file;

import java.io.File;

public class DocFileData {

    File docFile;
    String md5;
    String s3Url;

    public DocFileData(File docFile, String md5, String s3Url) {
        this.docFile = docFile;
        this.md5 = md5;
        this.s3Url = s3Url;
    }

    public File getDocFile() {
        return docFile;
    }

    public void setDocFile(File docFile) {
        this.docFile = docFile;
    }

    public String getMd5() {
        return md5;
    }

    public void setMd5(String md5) {
        this.md5 = md5;
    }

    public String getS3Url() {
        return s3Url;
    }

    public void setS3Url(String s3Url) {
        this.s3Url = s3Url;
    }

    @Override
    public String toString() {
        return "DocFileData{" +
                "docFile=" + docFile +
                ", md5='" + md5 + '\'' +
                ", s3Url='" + s3Url + '\'' +
                '}';
    }
}
