package eu.openaire.publications_retriever.util.file;

import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Paths;

public class FileData {

    private static final Logger logger = LoggerFactory.getLogger(FileData.class);

    private File file;
    private String hash;
    private Long size;
    private String location;

    private FileOutputStream fileOutputStream;


    public FileData(File file, String hash, Long size, String location, FileOutputStream fileOutputStream) {
        this.file = file;
        this.hash = hash;
        this.size = size;
        this.location = location;
        this.fileOutputStream = fileOutputStream;
    }


    public FileData(File file, String hash, Long size, String location) {
        this.file = file;
        this.hash = hash;
        this.size = size;
        this.location = location;
    }

    public FileData(File file, String location, FileOutputStream fileOutputStream) {
        this.file = file;
        this.location = location;
        this.fileOutputStream = fileOutputStream;
    }

    public File getFile() {
        return file;
    }

    public void setFile(File file) {
        this.file = file;
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
        if ( this.file == null ) {  // Verify the "docFile" is already set, otherwise we get an NPE.
            logger.warn("The \"file\" was not previously set!");
            return;
        }

        try {
            this.hash = Files.asByteSource(this.file).hash(Hashing.md5()).toString();	// These hashing functions are deprecated, but just to inform us that MD5 is not secure. Luckily, we use MD5 just to identify duplicate files.
            //logger.debug("MD5 for file \"" + docFile.getName() + "\": " + this.hash); // DEBUG!
            this.size = java.nio.file.Files.size(Paths.get(this.location));
            //logger.debug("Size of file \"" + docFile.getName() + "\": " + this.size); // DEBUG!
        } catch (Exception e) {
            logger.error("Could not retrieve the size " + ((this.hash == null) ? "and the MD5-hash " : "") + "of the file: " + this.location, e);
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
        return "FileData{" +
                "file=" + file +
                ", hash='" + hash + '\'' +
                ", size=" + size +
                ", location='" + location + '\'' +
                ", fileOutputStream=" + fileOutputStream +
                '}';
    }
}
