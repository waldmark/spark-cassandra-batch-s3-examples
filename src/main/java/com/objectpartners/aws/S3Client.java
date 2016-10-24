package com.objectpartners.aws;


import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.*;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

@Component
public class S3Client {

    private AmazonS3Client s3 = null;

    public S3Client() {
        s3 = getClient();
    }

    /**
     * S3 client configured to use the Scality
     * @return
     */
    private AmazonS3Client getClient() {
        if(null == s3) {
            System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true");
            BasicAWSCredentials credentials = new BasicAWSCredentials("accessKey1", "verySecretKey1");
            s3 = new AmazonS3Client(credentials);
            S3ClientOptions options = S3ClientOptions.builder().setPathStyleAccess(true).build();
            s3.setS3ClientOptions(options);
            s3.setEndpoint("http://127.0.0.1:8000/");
        }
        return s3;
    }

    public List<String> getBucketDescriptions() {
        List<String> buckets = new ArrayList<>();
        for (Bucket bucket : s3.listBuckets()) {
            System.out.println(" - " + bucket.getName());
            buckets.add(bucket.getName() + " created on: " + bucket.getCreationDate());
        }
        return buckets;
    }

    public List<String> getBucketObjectDescriptions(String bucketName) {
        List<String> objects = new ArrayList<>();

        ObjectListing objectListing = s3.listObjects(new ListObjectsRequest().withBucketName(bucketName));
        for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
            String desc = " - " + objectSummary.getKey() + "  "
                    + "(size = " + objectSummary.getSize() + ")";
            objects.add(desc);
        }
        return objects;
    }

    public Bucket createBucket(String bucketName) {
        return s3.createBucket(new CreateBucketRequest(bucketName));
    }

    public void storeString(String bucketName, String key, String string) {
        // turn the String into an InputStream
        int contentLength = string.length();
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentLength(contentLength);
        InputStream stream = new ByteArrayInputStream(string.getBytes(StandardCharsets.UTF_8));
        s3.putObject(new PutObjectRequest(bucketName, key, stream, objectMetadata));
    }

    public String readS3Object(String bucketName, String key) throws IOException {
        S3Object s3o = s3.getObject(bucketName, key);
        InputStream in = s3o.getObjectContent();
        InputStreamReader inr = new InputStreamReader(in);
        BufferedReader br = new BufferedReader(inr);
        StringBuilder everything = new StringBuilder();
        String line;
        while( (line = br.readLine()) != null) {
            everything.append(line);
        }
        return everything.toString();
    }

    public void removeObject(String bucketName, String key) {
        s3.deleteObject(bucketName, key);
    }

    public void removeBucket(String bucketName) {
        ObjectListing objectListing = s3.listObjects(new ListObjectsRequest().withBucketName(bucketName));
        for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
            removeObject(bucketName, objectSummary.getKey());
        }
        s3.deleteBucket(bucketName);
    }

}
