package eu.openaire.publications_retriever.util.file;

import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Paths;

public class DocFileData {

    private static final Logger logger = LoggerFactory.getLogger(DocFileData.class);

    private File docFile;
    private String hash;
    private Long size;
    private String location;

    private FileOutputStream fileOutputStream;


    public DocFileData(File docFile, String hash, Long size, String location, FileOutputStream fileOutputStream) {
        this.docFile = docFile;
        this.hash = hash;
        this.size = size;
        this.location = location;
        this.fileOutputStream = fileOutputStream;
    }


    public DocFileData(File docFile, String hash, Long size, String location) {
        this.docFile = docFile;
        this.hash = hash;
        this.size = size;
        this.location = location;
    }

    public DocFileData(File docFile, FileOutputStream fileOutputStream) {
        this.docFile = docFile;
        this.fileOutputStream = fileOutputStream;
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

    /**
     * Set this as a separate method (not automatically applied in the contractor), in order to avoid long thread-blocking in the caller method, which downloads and constructs this object inside a synchronized block.
     * */
    public void calculateAndSetHashAndSize() {
        if ( this.docFile == null ) {  // Verify the "docFile" is already set, otherwise we get an NPE.
            logger.warn("The \"docFile\" was not previously set!");
            return;
        }

        String fileLocation = this.docFile.getAbsolutePath();
        try {
            this.hash = Files.asByteSource(this.docFile).hash(Hashing.md5()).toString();	// These hashing functions are deprecated, but just to inform us that MD5 is not secure. Luckily, we use MD5 just to identify duplicate files.
            //logger.debug("MD5 for file \"" + docFile.getName() + "\": " + this.hash); // DEBUG!
            this.size = java.nio.file.Files.size(Paths.get(fileLocation));
            //logger.debug("Size of file \"" + docFile.getName() + "\": " + this.size); // DEBUG!
        } catch (Exception e) {
            logger.error("Could not retrieve the size " + ((this.hash == null) ? "and the MD5-hash " : "") + "of the file: " + fileLocation, e);
        }
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public FileOutputStream getFileOutputStream() {
        return fileOutputStream;
    }

    public void setFileOutputStream(FileOutputStream fileOutputStream) {
        this.fileOutputStream = fileOutputStream;
    }

    @Override
    public String toString() {
        return "DocFileData{" +
                "docFile=" + docFile +
                ", hash='" + hash + '\'' +
                ", size=" + size +
                ", location='" + location + '\'' +
                ", fileOutputStream=" + fileOutputStream +
                '}';
    }
}
