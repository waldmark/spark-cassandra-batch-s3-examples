package com.objectpartners.common.components;


import com.datastax.spark.connector.japi.CassandraRow;
import com.objectpartners.common.domain.RealTime911;
import org.apache.spark.api.java.function.Function;
import org.springframework.stereotype.Component;

@Component
public class Map911Call implements Function<CassandraRow, RealTime911> {

    @Override
    public RealTime911 call(CassandraRow row) throws Exception {
        RealTime911 call = new RealTime911();
        call.setAddress(row.getString("address"));
        call.setCallType(row.getString("calltype"));
        call.setDateTime(row.getString("calltime"));
        call.setLatitude(row.getString("latitude"));
        call.setLongitude(row.getString("longitude"));
        call.setReportLocation(row.getString("location"));
        call.setIncidentId(row.getString("id"));
        return call;
    }
}
