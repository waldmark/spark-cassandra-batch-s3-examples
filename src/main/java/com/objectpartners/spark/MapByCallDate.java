package com.objectpartners.spark;


import com.objectpartners.common.domain.RealTime911;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class MapByCallDate implements PairFunction<RealTime911, String, RealTime911> {

    @Override
    public Tuple2<String, RealTime911> call(RealTime911 realTime911) throws Exception {
        // create time bucket to group by dates (no time) - use MM/dd/yyyy
        String timeBucket = realTime911.getDateTime().substring(0,10);
        return new Tuple2<>(timeBucket, realTime911);
    }
}
