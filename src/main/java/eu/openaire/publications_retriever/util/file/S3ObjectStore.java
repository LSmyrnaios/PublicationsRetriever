package eu.openaire.publications_retriever.util.file;

import java.io.File;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3ObjectStore {

    private static final Logger logger = LoggerFactory.getLogger(S3ObjectStore.class);

    private static String secretKey = null;
    private static String accessKey = null;
    private static String region = null;
    private static String bucketName = null;
    private static AmazonS3 s3Client;

    public static final boolean shouldEmptyBucket = false;  // Set true only for testing!
    public static final String credentialsFilePath = FileUtils.workingDir + "amazon_credentials.txt";


    /**
     * This must be called before any other methods.
     * */
    public S3ObjectStore()
    {
        // Take the credentials from the file.
        Scanner myReader = null;
        try {
            File credentialsFile = new File(credentialsFilePath);
            if ( !credentialsFile.exists() ) {
                throw new RuntimeException("credentialsFile \"" + credentialsFilePath + "\" does not exists!");
            }
            myReader = new Scanner(credentialsFile);
            if ( myReader.hasNextLine() ) {
                String[] credentials = myReader.nextLine().split(",");
                if ( credentials.length < 4 ) {
                    throw new RuntimeException("Not all credentials were retrieved from file \"" + credentialsFilePath + "\"!");
                }
                accessKey = credentials[0];
                secretKey = credentials[1];
                region = credentials[2];
                bucketName = credentials[3];
            }
        } catch (Exception e) {
            String errorMsg = "An error prevented the retrieval of the amazon credentials from the file: " + credentialsFilePath;
            logger.error(errorMsg);
            System.err.println(errorMsg);
            e.printStackTrace();
            System.exit(53);
        } finally {
            if ( myReader != null )
                myReader.close();
        }

        if ( (accessKey == null) || (secretKey == null) || (region == null) || (bucketName == null) ) {
            String errorMsg = "No \"accessKey\" or/and \"secretKey\" or/and \"region\" or/and \"bucketName\" could be retrieved!";
            logger.error(errorMsg);
            System.err.println(errorMsg);
            System.exit(54);
        }

        s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_EAST_2)  // TODO - Change the region if needed.
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                .build();

        boolean bucketExists = false;
        try {
            bucketExists = s3Client.doesBucketExistV2(bucketName);
        } catch (Exception e) {
            String errorMsg = "There was a problem while checking if the bucket \"" + bucketName + "\" exists!\n" + e.getMessage();
            logger.error(errorMsg);
            System.err.println(errorMsg);
            System.exit(55);
        }

        // Keep this commented-out to avoid objects-deletion by accident. The code is open-sourced, so it's easy to enable this ability if we really want it (e.g. for testing).
/*        if ( bucketExists && shouldEmptyBucket ) {
            emptyBucket(bucketName, false);
            //throw new RuntimeException("stop just for test!");
        }*/

        // Create the bucket if not exist.
        try {
            if ( !bucketExists )
                s3Client.createBucket(bucketName);
            else
                logger.warn("Bucket \"" + bucketName + "\" already exists.");
        } catch (Exception e) {
            String errorMsg = "Could not create the bucket \"" + bucketName + "\"!";
            logger.error(errorMsg);
            System.err.println(errorMsg);
            e.printStackTrace();
            System.exit(56);
        }
    }


    /**
     * @param fileObjKeyName = "**File object key name**";
     * @param fileFullPath = "**Path of the file to upload**";
     * @return
     */
    public static DocFileData uploadToS3(String fileObjKeyName, String fileFullPath)
    {
        ObjectMetadata metadata = new ObjectMetadata();

        // Take the Matcher to retrieve the extension.
        Matcher extensionMatcher = FileUtils.EXTENSION_PATTERN.matcher(fileFullPath);
        if ( extensionMatcher.find() ) {
            String extension = null;
            if ( (extension = extensionMatcher.group(0)) == null )
                metadata.setContentType("application/pdf");
            else {
                if ( extension.equals("pdf") )
                    metadata.setContentType("application/pdf");
                /*else if ( *//* TODO - other-extension-match *//* )
                    metadata.setContentType("application/pdf"); */
                else
                    metadata.setContentType("application/pdf");
            }
        } else {
            logger.warn("The file with key \"" + fileObjKeyName + "\" does not have a file-extension! Setting the \"pdf\"-mimeType.");
            metadata.setContentType("application/pdf");
        }

        PutObjectRequest request = new PutObjectRequest(bucketName, fileObjKeyName, new File(fileFullPath));
        request.withCannedAcl(CannedAccessControlList.PublicRead);   // Define which will have access to the file.
        request.setMetadata(metadata);

        PutObjectResult result;
        try {
            result = s3Client.putObject(request);
        } catch (Exception e) {
            logger.error("Could not upload the file \"" + fileObjKeyName + "\" to the S3 ObjectStore, exception: " + e.getMessage());
            return null;
        }

        String contentMD5 = result.getContentMd5();
        String S3Url = s3Client.getUrl(bucketName, fileObjKeyName).toString();  // Be aware: This url works only if the access to the bucket is public.
        logger.debug("fileObjKey \"" + fileObjKeyName + "\" has contentMD5 = " + contentMD5 + " and S3Url: " + S3Url);
        return new DocFileData(null, contentMD5, S3Url);
    }


    public static boolean emptyBucket(String bucketName, boolean shouldDeleteBucket)
    {
        logger.warn("Going to " + (shouldDeleteBucket ? "delete" : "empty") + " bucket \"" + bucketName + "\"");

        // First list the objects of the bucket -taken in multiple pages- and delete each one of them.
        ObjectListing objects;
        try {
            objects = s3Client.listObjects(bucketName);
            List<S3ObjectSummary> results = objects.getObjectSummaries();

            for ( S3ObjectSummary objSum : results ) {
                if ( !deleteFile(objSum.getKey(), bucketName) ) {
                    logger.error("Cannot proceed with bucket deletion, since only an empty bucket can be removed!");
                    return false;
                }
            }

            if ( objects.isTruncated() ) {  // The results are paginated, so additional calls are required.
                while ( (objects = s3Client.listNextBatchOfObjects(objects)) != null ) {
                    results = objects.getObjectSummaries();
                    for ( S3ObjectSummary objSum : results ) {
                        if ( !deleteFile(objSum.getKey(), bucketName) ) {
                            logger.error("Cannot proceed with bucket deletion, since only an empty bucket can be removed!");
                            return false;
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Could not retrieve the list of objects of bucket \"" + bucketName + "\"!");
            return false;
        }

        if ( shouldDeleteBucket ) {
            // Lastly, delete the empty bucket.
            try {
                s3Client.deleteBucket(bucketName);
            } catch (Exception e) {
                logger.error("Could not delete the bucket \"" + bucketName + "\" from the S3 ObjectStore, exception: " + e.getMessage());
                e.printStackTrace();
                return false;
            }
        }

        return true;
    }


    public static boolean deleteFile(String fileObjKeyName, String bucketName)
    {
        try {
            s3Client.deleteObject(bucketName, fileObjKeyName);
        } catch (Exception e) {
            logger.error("Could not delete the file \"" + fileObjKeyName + "\" from the S3 ObjectStore, exception: " + e.getMessage());
            return false;
        }
        return true;
    }

}
