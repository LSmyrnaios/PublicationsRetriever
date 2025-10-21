package eu.openaire.publications_retriever.util.file;

import io.minio.*;
import io.minio.messages.Bucket;
import io.minio.messages.Item;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;


public class S3ObjectStore {

    private static final Logger logger = LoggerFactory.getLogger(S3ObjectStore.class);

    private static String endpoint = null;
    private static String accessKey = null;
    private static String secretKey = null;
    private static String region = null;
    private static String bucketName = null;

    private static MinioClient minioClient;

    public static final boolean shouldEmptyBucket = false;  // Set true only for testing!
    public static final String credentialsFilePath = FileUtils.workingDir + "S3_credentials.txt";
    private static final boolean shouldShowAllS3Buckets = false;


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
                if ( credentials.length < 5 ) {
                    throw new RuntimeException("Not all credentials were retrieved from file \"" + credentialsFilePath + "\"!");
                }
                endpoint = credentials[0].trim();
                accessKey = credentials[1].trim();
                secretKey = credentials[2].trim();
                region = credentials[3].trim();
                bucketName = credentials[4].trim();
            }
        } catch (Exception e) {
            String errorMsg = "An error prevented the retrieval of the minIO credentials from the file: " + credentialsFilePath + "\n" + e.getMessage();
            logger.error(errorMsg, e);
            System.err.println(errorMsg);
            System.exit(53);
        } finally {
            if ( myReader != null )
                myReader.close();
        }

        if ( (endpoint == null) || (accessKey == null) || (secretKey == null) || (region == null) || (bucketName == null) ) {
            String errorMsg = "No \"endpoint\" or/and \"accessKey\" or/and \"secretKey\" or/and \"region\" or/and \"bucketName\" could be retrieved from the file: " + credentialsFilePath;
            logger.error(errorMsg);
            System.err.println(errorMsg);
            System.exit(54);
        }
        // It's not safe, nor helpful to show the credentials in the logs.

        minioClient = MinioClient.builder().endpoint(endpoint).credentials(accessKey, secretKey).region(region).build();
        // Default timeouts are: conn: 30secs, read: 60secs, write: 30secs.
        minioClient.setTimeout(TimeUnit.MINUTES.toMillis(1), TimeUnit.MINUTES.toMillis(5), TimeUnit.MINUTES.toMillis(5));

        boolean bucketExists = false;
        try {
            bucketExists = minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build());
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

        // Make the bucket, if not exist.
        try {
            if ( !bucketExists ) {
            	logger.info("Bucket \"" + bucketName + "\" does not exist! Going to create it..");
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
            } else
                logger.debug("Bucket \"" + bucketName + "\" already exists.");
        } catch (Exception e) {
            String errorMsg = "Could not create the bucket \"" + bucketName + "\"!";
            logger.error(errorMsg, e);
            System.err.println(errorMsg);
            System.exit(56);
        }

        if ( shouldShowAllS3Buckets ) {
            List<Bucket> buckets;
            try {
                buckets = minioClient.listBuckets();
                logger.debug("The buckets in the S3 ObjectStore are:");
                for ( Bucket bucket : buckets ) {
                    logger.debug(bucket.name());
                }
            } catch (Exception e) {
                logger.warn("Could not listBuckets: " + e.getMessage());
            }
        }
    }


    /**
     * @param fileObjKeyName = "**File object key name**";
     * @param fileFullPath = "**Path of the file to upload**";
     * @return
     */
    public static FileData uploadToS3(String fileObjKeyName, String fileFullPath)
    {
        String contentType = null;

        // Take the Matcher to retrieve the extension.
        Matcher extensionMatcher = FileUtils.EXTENSION_PATTERN.matcher(fileFullPath);
        if ( extensionMatcher.find() ) {
            String extension = null;
            if ( (extension = extensionMatcher.group(0)) == null )
                contentType = "application/pdf";
            else {
                if ( extension.equals("pdf") )
                    contentType = "application/pdf";
                    /*else if ( *//* TODO - other-extension-match *//* )
                    contentType = "application/pdf"; */
                else
                    contentType = "application/pdf";
            }
        } else {
            logger.warn("The file with key \"" + fileObjKeyName + "\" does not have a file-extension! Setting the \"pdf\"-mimeType.");
            contentType = "application/pdf";
        }

        ObjectWriteResponse response;
        try {
            response = minioClient.uploadObject(UploadObjectArgs.builder()
                                                    .bucket(bucketName)
                                                    .object(fileObjKeyName).filename(fileFullPath)
                                                    .contentType(contentType).build());

            // TODO - What if the fileObjKeyName already exists?
            // Right now it gets overwritten (unless we add versioning, which is irrelevant for different objects..)
            // Luckily, we use unique file-names.

        } catch (Exception e) {
            logger.error("Could not upload the file \"" + fileObjKeyName + "\" to the S3 ObjectStore, exception: " + e.getMessage(), e);
            return null;
        }

        String s3Url = endpoint + "/" + bucketName + "/" + fileObjKeyName;  // Be aware: This url works only if the access to the bucket is public.
        logger.debug("Uploaded file \"" + fileObjKeyName + "\". The s3Url is: " + s3Url);
        return new FileData(null, null, 0L, s3Url);
    }


    public static boolean emptyBucket(String bucketName, boolean shouldDeleteBucket)
    {
        logger.warn("Going to " + (shouldDeleteBucket ? "delete" : "empty") + " bucket \"" + bucketName + "\"!");

        // First list the objects of the bucket.
        Iterable<Result<Item>> results;
        try {
            results = minioClient.listObjects(ListObjectsArgs.builder().bucket(bucketName).build());
        } catch (Exception e) {
            logger.error("Could not retrieve the list of objects of bucket \"" + bucketName + "\"!");
            return false;
        }

        int countDeletedFiles = 0;
        int countFilesNotDeleted = 0;
        long totalSize = 0;
        Item item;

        // Then, delete the objects.
        for ( Result<Item> resultItem : results ) {
            try {
                item = resultItem.get();
            } catch (Exception e) {
                logger.error("Could not get the item-object of one of the S3-Objects returned from the bucket!", e);
                countFilesNotDeleted ++;
                continue;
            }
            totalSize += item.size();

            if ( !deleteFile(item.objectName(), bucketName) ) { // The reason and for what object, is already logged.
                logger.error("Cannot proceed with bucket deletion, since only an empty bucket can be removed!");
                countFilesNotDeleted ++;
            } else
                countDeletedFiles ++;
        }

        if ( shouldDeleteBucket ) {
            if ( countFilesNotDeleted == 0 ) {
                // Lastly, delete the empty bucket. We need to do this last, as in case it's not empty, we get an error!
                try {
                    minioClient.removeBucket(RemoveBucketArgs.builder().bucket(bucketName).build());
                    logger.info("Bucket \"" + bucketName + "\" was deleted!");
                } catch (Exception e) {
                    logger.error("Bucket \"" + bucketName + "\" could not be deleted!", e);
                    return false;
                }
            } else {
                logger.error("Cannot execute the \"removeBucket\" command for bucket \"" + bucketName + "\", as " + countFilesNotDeleted + " files failed to be deleted!");
                return false;
            }
        } else
            logger.info("Bucket \"" + bucketName + "\" was emptied!");

        logger.info(countDeletedFiles + " files were deleted, amounting to " + ((totalSize/1024)/1024) + " MB.");
        return true;
    }


    public static boolean deleteFile(String fileObjKeyName, String bucketName)
    {
        try {
            minioClient.removeObject(RemoveObjectArgs.builder().bucket(bucketName).object(fileObjKeyName).build());
        } catch (Exception e) {
            logger.error("Could not delete the file \"" + fileObjKeyName + "\" from the S3 ObjectStore, exception: " + e.getMessage(), e);
            return false;
        }
        return true;
    }

}
