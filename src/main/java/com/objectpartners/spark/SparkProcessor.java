package com.objectpartners.spark;

import com.objectpartners.common.components.Map911Call;
import com.objectpartners.common.domain.CallFrequency;
import com.objectpartners.common.domain.RealTime911;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;

/**
 * Spark batch processing of Cassandra data
 */
@Component
public class SparkProcessor implements Serializable {
    static final long serialVersionUID = 100L;
    private static Logger LOG = LoggerFactory.getLogger(SparkProcessor.class);


    JavaPairRDD<String, RealTime911> processCassandraData() {

//         *************************************************************************************************************
//         set up Spark context
//         *************************************************************************************************************

        SparkConf conf = new SparkConf()
                .setAppName("CassandraClient")
                .setMaster("local")
                .set("spark.executor.memory", "1g")
                .set("spark.cassandra.connection.host", "127.0.0.1");
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

//         *************************************************************************************************************
//         sort by frequecy on cleansed data
//         *************************************************************************************************************
//
//        JavaRDD<String> cleansedCallTypes = callData.map(x -> (
//                x.getCallType().replaceAll("\"", "").replaceAll("[-|,]", "")));
//        // create pair for reduction
//        JavaPairRDD<String, Integer> cpairs = cleansedCallTypes.mapToPair(s -> new Tuple2<>(s, 1)); // 1. create pairs
//        JavaPairRDD<String, Integer> creduced = cpairs.reduceByKey((a, b) -> a + b); // 2. reduce by callType
//        // swap pair order so we can sort on frequency
//        JavaPairRDD<Integer, String> scounts = creduced.mapToPair(Tuple2::swap); // 3. swap so we can order by frequency
//        JavaPairRDD<Integer, String> oscounts = scounts.sortByKey(); // 4. sort by key (frequency)
//        // swap again so the key is call type, not frequency
//        JavaPairRDD<String, Integer> xscounts = oscounts.mapToPair(Tuple2::swap); // 5. swap back so the key is again callType
//        // get a list
//        List<Tuple2<String, Integer>> cleansedList = xscounts.collect(); //6. get a List

//         *************************************************************************************************************
//         save call frequency data to Casandra
//         *************************************************************************************************************
//
//        List<CallFrequency> callFrequencyList = new ArrayList<>();
//        // get list iterator that starts with last element
//        ListIterator<Tuple2<String, Integer>> cleansedListIterator = cleansedList.listIterator(cleansedList.size());
//        while (cleansedListIterator.hasPrevious()) { // iterate over list backwards, last to first
//            Tuple2<String, Integer> callCounts = cleansedListIterator.previous();
//            // save each callType frequency
//            CallFrequency callFrequency = new CallFrequency();
//            callFrequency.setCalltype(callCounts._1());
//            callFrequency.setCount(callCounts._2());
//            callFrequencyList.add(callFrequency);
//        }
//
//        // write call frequency data to Cassandra
//        JavaRDD<CallFrequency> cfRDD = sc.parallelize(callFrequencyList);
//        javaFunctions(cfRDD)
//                .writerBuilder("testkeyspace", "calltypes", mapToRow(CallFrequency.class)).saveToCassandra();

//         *************************************************************************************************************
//         filter data to 'Fire' events and group by event date
//         *************************************************************************************************************

        LOG.info("unfiltered callRDD count = " + callData.count());

        // filter by fire type
        callData = callData.filter( c -> (c.getCallType().matches("(?i:.*\\bFire\\b.*)")));
        LOG.info("filtered callRDD count = " + callData.count());

        // group the data by date (MM/dd/yyyy)
        MapByCallDate mapByCallDate = new MapByCallDate();

        return callData.mapToPair(mapByCallDate);
    }

}
