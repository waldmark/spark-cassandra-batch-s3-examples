package com.objectpartners.cassandra;

import com.datastax.driver.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

/**
 * Create Cassandra database and
 * load demo data (911 calls) into Casandra
 */
@Component
public class CassandraDataLoader {

    private static Logger LOG = LoggerFactory.getLogger(CassandraDataLoader.class);

    @Value(value="${cassandra.keyspaces[0].dropCommand}")
    private String dropKeyspaceCommand;

    @Value(value="${cassandra.keyspaces[0].createCommand}")
    private String createKeyspaceCommand;

    @Value(value="${cassandra.keyspaces[0].truncateCommand}")
    private String truncateKeyspaceCommand;

    @Value(value="${cassandra.keyspaces[0].tables[0].createCommand}")
    private String createRT911TableCommand;

    @Value(value="${cassandra.keyspaces[0].tables[0].insertPreparedStatementCommand}")
    private String insertRT911DataCommand;

    @Value(value="${demo.data.filename:Seattle_Real_Time_Fire_911_Calls_10_Test.csv.gz}")
    private String dataFileName;

    @Value(value="${cassandra.host:127.0.0.1}")
    private String cassandraHost;

    private Session session;

    public void insertCalls() {
        Cluster cluster = Cluster.builder().addContactPoint(cassandraHost).build();

        Metadata metadata = cluster.getMetadata();
        LOG.info("Connected to cluster: " + metadata.getClusterName());
        for (Host host : metadata.getAllHosts()) {
            LOG.info("Datatacenter: " + host.getDatacenter()
                            + " Host: " + host.getAddress()
                            + " Rack: " + host.getRack() + "n");
        }

        session = cluster.connect();
        createSchema();
        loadData();
        cluster.close();
    }

    private void createSchema() {
        try {
            session.execute(dropKeyspaceCommand);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.exit(-1);
        }

        session.execute(createKeyspaceCommand);
        session.execute(createRT911TableCommand);
    }

    private void loadData() {
        // first, clean out existing data
        session.execute(truncateKeyspaceCommand);
        readInputData();
    }

    private void readInputData() {
        LOG.info("reading data from " + dataFileName);
        try {
            final InputStream is = CassandraDataLoader.class.getResourceAsStream("/" + dataFileName);
            final BufferedInputStream bis = new BufferedInputStream(is);
            final GZIPInputStream iis = new GZIPInputStream(bis);
            final InputStreamReader gzipReader = new InputStreamReader(iis);
            final BufferedReader br = new BufferedReader(gzipReader);
            br.readLine(); // skip header or first line
            PreparedStatement prepared = session.prepare(insertRT911DataCommand);

            LOG.info("START DATA LOADING");

            int counter = 1;
            String line;
            while ((line = br.readLine()) != null) {
                // parse into values
                String[] values = parse(line);
                if (null != values) {
                    counter++;
                    try {
                        session.execute(prepared.bind(
                                values[0],
                                values[1],
                                values[2],
                                values[3],
                                values[4],
                                values[5],
                                values[6])
                        );
                    } catch (Exception e) {
                        LOG.info("error " + e.getMessage());
                    }
                }
            }

            LOG.info("FINSIHED INPUT LOADING - read and loaded " + counter + " lines from " + dataFileName);

            br.close();
            iis.close();
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

    private String[] parse(String line) {
        String[] columns = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        if (columns.length < 8) {
            String[] values = {"", "", "", "", "", "", ""}; // 7 values
            System.arraycopy(columns, 0, values, 0, columns.length);
            values[5] = values[5].replaceAll("\"", "");
            for(int i=0; i< values.length; i++) {
                values[i] = values[i].replace("'", "[esc quote]"); // remove single quote -- used sometimes for 'feet'
            }
            return values;
        } else {
            LOG.warn("bad row " + line);
        }
        return null;
    }
}
