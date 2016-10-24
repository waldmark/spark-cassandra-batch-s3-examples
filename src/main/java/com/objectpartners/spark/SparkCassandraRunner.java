package com.objectpartners.spark;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.objectpartners.aws.S3Client;
import com.objectpartners.cassandra.CassandraDataLoader;
import com.objectpartners.common.domain.RealTime911;
import org.apache.spark.api.java.JavaPairRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * orchestrates the demo example to
 * 1) create demo data in Cassandra
 * 2) read data from Cassandra and analyze with Spark
 * 3) write analysis results to S3
 */
@Component
//@PropertySource(name = "props", value = "classpath:/application.yml")
public class SparkCassandraRunner {
    private static Logger LOG = LoggerFactory.getLogger(SparkCassandraRunner.class);

    @Autowired
    private SparkProcessor sparkProcessor;

    @Autowired
    private CassandraDataLoader dataLoader;

    @Autowired
    private S3Client s3Client;

    @Value(value="${s3.bucket.name:test-bucket-100}")
    private String bucketName;

    public void runSparkStreamProcessing() {

        /**
         * initialize Cassandra with 911 call data
         * this requires that Cassandra is up and running
         * the demo is configured to run with a local Cassandra,
         * for example a docker Cassandra image
         * @link https://hub.docker.com/_/cassandra/
         */
        dataLoader.insertCalls();

        //now read data from Cassandra into Spark and batch process the data
        LOG.info("processing Cassandra data with Spark");

        JavaPairRDD<String, RealTime911> callsByCallDate = sparkProcessor.processCassandraData();

        //create JSON objects from the Spark results
        LOG.info("converting Spark results to JSON");

        JavaPairRDD<String, Iterable<RealTime911>> groupedCalls = callsByCallDate.groupByKey();
        Map<String, Iterable<RealTime911>> groupedCallMap = groupedCalls.collectAsMap();
        Set<String> keys = groupedCallMap.keySet();

        ObjectMapper mapper = new ObjectMapper();

        Map<String, String> s3BucketData = new HashMap<>();
        for(String key: keys) {
            List<String> jsonArrayElements = new ArrayList<>();
            Iterable<RealTime911> iterable = groupedCallMap.get(key);
            Iterator<RealTime911> iterator = iterable.iterator();
            while(iterator.hasNext()) {
                RealTime911 rt911 = iterator.next();
                LOG.debug(rt911.getDateTime() + " " + rt911.getCallType());
                try {
                    String jsonRT911 = mapper.writeValueAsString(rt911);
                    jsonArrayElements.add(jsonRT911);
                } catch (JsonProcessingException e) {
                    LOG.error(e.getMessage());
                }
            }

            StringJoiner joiner = new StringJoiner(",");
            jsonArrayElements.forEach(joiner::add);
            s3BucketData.put(key, "[" + joiner.toString() + "]");
        }

        /**
         * save to S3
         * this demo assumes S3 is available somewhere
         * the demo uses the Scality open source S3 docker image
         * @link https://s3.scality.com/
         */
        LOG.info("storing JSON into S3 bucket: " + bucketName);

        try {
            // remove the S3 bucket, this removes all objects in the bucket first
            s3Client.removeBucket(bucketName);
            LOG.info("S3 bucket " + bucketName + " deleted");
        } catch (Exception e) {
            // bucket not deleted, may not have been there
        }

        try {
            // create the bucket to start fresh
            s3Client.createBucket(bucketName);
            LOG.info("S3 bucket " + bucketName + " created");


            Set<String> bucketKeys = s3BucketData.keySet();
            // save to S3
            for(String key: bucketKeys) {
                s3Client.storeString(bucketName, key, s3BucketData.get(key));
            }
            LOG.info("finished saving JSON to S3 completed");

            LOG.info("displaying all JSON objects and their keys saved to " + bucketName + "\n");
            for(String key: bucketKeys) {
                String storedObject = s3Client.readS3Object(bucketName, key);
                LOG.info("key: " + key + " value: " + storedObject);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
        } finally {
            // clean up
            s3Client.removeBucket(bucketName);
        }
        LOG.info("Spark processing Cassandra data completed ....");

    }
}
