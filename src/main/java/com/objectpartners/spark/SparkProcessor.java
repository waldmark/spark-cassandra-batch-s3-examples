package com.objectpartners.spark;

import com.objectpartners.common.components.Map911Call;
import com.objectpartners.common.domain.RealTime911;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.Serializable;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

/**
 * Spark batch processing of Cassandra data
 */
@Component
public class SparkProcessor implements Serializable {
    static final long serialVersionUID = 100L;
    private static Logger LOG = LoggerFactory.getLogger(SparkProcessor.class);

    @Value(value="${spring.application.name:cassandra-rt911-client}")
    private String applicationName;

    @Value(value="${spark.master:local}")
    private String sparkMaster;

    @Value(value="${spark.executor.memory}")
    private String sparkExecutorMemory;

    @Value(value="${spark.cassandra.connection.host}")
    private String sparkCassandraConnectionHost;

    JavaPairRDD<String, RealTime911> processCassandraData() {

//         *************************************************************************************************************
//         set up Spark context
//         *************************************************************************************************************

        SparkConf conf = new SparkConf()
                .setAppName(applicationName)
                .setMaster(sparkMaster)
                .set("spark.executor.memory", sparkExecutorMemory)
                .set("spark.cassandra.connection.host", sparkCassandraConnectionHost);
        JavaSparkContext sc = new JavaSparkContext(conf);

//         *************************************************************************************************************
//         read from Cassandra into Spark RDD
//         *************************************************************************************************************

        // read the rt911 table and map to RealTime911 java objects
        // this custom mapping does not require the Cassandra columns
        // and Java class fields to have the same naming
        JavaRDD<RealTime911> callData = javaFunctions(sc)
                .cassandraTable("testkeyspace", "rt911")
                .map(new Map911Call());

        LOG.info("unfiltered callRDD count = " + callData.count());

        // filter by fire type
        callData = callData.filter( c -> (c.getCallType().matches("(?i:.*\\bFire\\b.*)")));
        LOG.info("filtered callRDD count = " + callData.count());

        // group the data by date (MM/dd/yyyy)
        MapByCallDate mapByCallDate = new MapByCallDate();

        return callData.mapToPair(mapByCallDate);
    }

}
