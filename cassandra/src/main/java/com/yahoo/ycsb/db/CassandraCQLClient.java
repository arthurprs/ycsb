/**
 * Copyright (c) 2013 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License. See accompanying LICENSE file.
 *
 * Submitted by Chrisjan Matser on 10/11/2010.
 */
package com.yahoo.ycsb.db;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.*;
import com.datastax.driver.core.querybuilder.*;
import com.yahoo.ycsb.*;
import org.apache.commons.lang3.mutable.MutableObject;

import java.nio.ByteBuffer;

import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Tested with Cassandra 2.0, CQL client for YCSB framework
 */

/**
 * Type the following commands in cqlsh
 * <p/>
 * CREATE KEYSPACE ycsb WITH replication = {
 * 'class': 'SimpleStrategy',
 * 'replication_factor': '1'
 * };
 * <p/>
 * USE ycsb;
 * <p/>
 * CREATE TABLE usertable (
 * user_id text,
 * "timestamp" text,
 * field0 text,
 * field1 text,
 * field2 text,
 * field3 text,
 * field4 text,
 * field5 text,
 * field6 text,
 * field7 text,
 * field8 text,
 * field9 text,
 * PRIMARY KEY (user_id, "timestamp")
 * ) WITH CLUSTERING ORDER BY ("timestamp" DESC) AND
 * bloom_filter_fp_chance=0.010000 AND
 * caching='KEYS_ONLY' AND
 * comment='' AND
 * dclocal_read_repair_chance=0.000000 AND
 * gc_grace_seconds=864000 AND
 * index_interval=128 AND
 * read_repair_chance=0.100000 AND
 * replicate_on_write='true' AND
 * populate_io_cache_on_flush='false' AND
 * default_time_to_live=0 AND
 * speculative_retry='99.0PERCENTILE' AND
 * memtable_flush_period_in_ms=0 AND
 * compaction={'class': 'SizeTieredCompactionStrategy'} AND
 * compression={'sstable_compression': 'LZ4Compressor'};
 */

public class CassandraCQLClient extends DB {

    private static Cluster cluster = null;
    private static Session session = null;

    private static ConsistencyLevel readConsistencyLevel = ConsistencyLevel.ONE;
    private static ConsistencyLevel writeConsistencyLevel = ConsistencyLevel.ONE;

    public static final int OK = 0;
    public static final int ERR = -1;

    public static final String PARTITION_KEY = "user_id";
    public static final String INDEX_KEY = "timestamp";
    public static final String KEYSPACE_PROPERTY = "cassandra.keyspace";
    public static final String KEYSPACE_PROPERTY_DEFAULT = "ycsb";
    public static final String USERNAME_PROPERTY = "cassandra.username";
    public static final String PASSWORD_PROPERTY = "cassandra.password";

    public static final String READ_CONSISTENCY_LEVEL_PROPERTY = "cassandra.readconsistencylevel";
    public static final String READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";
    public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY = "cassandra.writeconsistencylevel";
    public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";

    /**
     * Count the number of times initialized to teardown on the last {@link #cleanup()}.
     */
    private static final AtomicInteger initCount = new AtomicInteger(0);

    private static boolean _debug = false;

    /**
     * Initialize any state for this DB. Called once per DB instance; there is
     * one DB instance per client thread.
     */
    @Override
    public void init() throws DBException {

        //Keep track of number of calls to init (for later cleanup)
        initCount.incrementAndGet();

        //Synchronized so that we only have a single
        //  cluster/session instance for all the threads.
        synchronized (initCount) {

            //Check if the cluster has already been initialized
            if (cluster != null) {
                return;
            }

            try {
                _debug = Boolean.parseBoolean(getProperties().getProperty("debug", "false"));

                String host = getProperties().getProperty("host");
                if (host == null) {
                    throw new DBException("Required property \"host\" missing for CassandraClient");
                }
                String hosts[] = host.split("[, ]");
                String port = getProperties().getProperty("port", "9042");
                if (port == null) {
                    throw new DBException("Required property \"port\" missing for CassandraClient");
                }

                String username = getProperties().getProperty(USERNAME_PROPERTY);
                String password = getProperties().getProperty(PASSWORD_PROPERTY);

                String keyspace = getProperties().getProperty(KEYSPACE_PROPERTY, KEYSPACE_PROPERTY_DEFAULT);

                readConsistencyLevel = ConsistencyLevel.valueOf(getProperties().getProperty(READ_CONSISTENCY_LEVEL_PROPERTY, READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));
                writeConsistencyLevel = ConsistencyLevel.valueOf(getProperties().getProperty(WRITE_CONSISTENCY_LEVEL_PROPERTY, WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));


                Cluster.Builder clusterBuilder = Cluster.builder()
                        .withPort(Integer.valueOf(port))
                        .withReconnectionPolicy(new ConstantReconnectionPolicy(0))
                        .withLoadBalancingPolicy(new TokenAwarePolicy(new LatencyAwarePolicy.Builder(new RoundRobinPolicy()).build()))
                        .withRetryPolicy(FallthroughRetryPolicy.INSTANCE)
                        .addContactPoints(hosts);
                if ((username != null) && !username.isEmpty())
                    clusterBuilder.withCredentials(username, password);

                cluster = clusterBuilder.build();

                //Update number of connections based on threads
                int threadcount = Integer.parseInt(getProperties().getProperty("threadcount", "100"));

                cluster.getConfiguration().getQueryOptions().setFetchSize(Integer.MAX_VALUE);

                cluster.getConfiguration().getPoolingOptions().setMaxConnectionsPerHost(HostDistance.LOCAL, threadcount);

                //Set connection timeout 3min (default is 5s)
                cluster.getConfiguration().getSocketOptions().setConnectTimeoutMillis(3 * 60 * 1000);
                //Set read (execute) timeout 3min (default is 12s)
                cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(3 * 60 * 1000);

                Metadata metadata = cluster.getMetadata();
                System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());

                for (Host discoveredHost : metadata.getAllHosts()) {
                    System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
                            discoveredHost.getDatacenter(),
                            discoveredHost.getAddress(),
                            discoveredHost.getRack());
                }

                session = cluster.connect(keyspace);

            } catch (Exception e) {
                throw new DBException(e);
            }
        }//synchronized
    }

    /**
     * Cleanup any state for this DB. Called once per DB instance; there is one
     * DB instance per client thread.
     */
    @Override
    public void cleanup() throws DBException {
        if (initCount.decrementAndGet() <= 0) {
            cluster.close();
            cluster = null;
        }
    }

    /**
     * Read a record from the database. Each field/value pair from the result
     * will be stored in a HashMap.
     *
     * @param table  The name of the table
     * @param key    The record key of the record to read.
     * @param fields The list of fields to read, or null for all of them
     * @param result A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error
     */
    @Override
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {

        try {
            Statement stmt;
            Select.Builder selectBuilder;

            if (fields == null) {
                selectBuilder = QueryBuilder.select().all();
            } else {
                selectBuilder = QueryBuilder.select();
                for (String col : fields) {
                    ((Select.Selection) selectBuilder).column(col);
                }
            }

            stmt = selectBuilder.from(table).where(QueryBuilder.eq(PARTITION_KEY, key)).limit(1);
            stmt.setConsistencyLevel(readConsistencyLevel);

            if (_debug) {
                System.out.println(stmt.toString());
            }

            ResultSet rs = session.execute(stmt);

            //Should be only 1 row
            if (!rs.isExhausted()) {
                Row row = rs.one();
                ColumnDefinitions cd = row.getColumnDefinitions();

                for (ColumnDefinitions.Definition def : cd) {
                    ByteBuffer val = row.getBytesUnsafe(def.getName());
                    if (val != null) {
                        result.put(def.getName(),
                                new ByteArrayByteIterator(val.array()));
                    } else {
                        result.put(def.getName(), null);
                    }
                }

            }

            return OK;

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error reading key: " + key);
            return ERR;
        }

    }

    /**
     * Perform a range scan for a set of records in the database. Each
     * field/value pair from the result will be stored in a HashMap.
     * <p/>
     * Cassandra CQL uses "token" method for range scan which doesn't always
     * yield intuitive results.
     *
     * @param table       The name of the table
     * @param startkey    The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param fields      The list of fields to read, or null for all of them
     * @param result      A Vector of HashMaps, where each HashMap is a set
     *                    field/value pairs for one record
     * @return Zero on success, a non-zero error code on error
     */
    @Override
    public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {

        try {
            Statement stmt;
            Select.Builder selectBuilder;

            if (fields == null) {
                selectBuilder = QueryBuilder.select().all();
            } else {
                selectBuilder = QueryBuilder.select();
                for (String col : fields) {
                    ((Select.Selection) selectBuilder).column(col);
                }
            }

            stmt = selectBuilder.from(table);

            //The statement builder is not setup right for tokens.
            //  So, we need to build it manually.
            String initialStmt = stmt.toString();
            StringBuilder scanStmt = new StringBuilder();
            scanStmt.append(
                    initialStmt.substring(0, initialStmt.length() - 1));
            scanStmt.append(" WHERE ");
            scanStmt.append(QueryBuilder.token(PARTITION_KEY));
            scanStmt.append(" >= ");
            scanStmt.append("token('");
            scanStmt.append(startkey);
            scanStmt.append("')");
            scanStmt.append(" LIMIT ");
            scanStmt.append(recordcount);

            stmt = new SimpleStatement(scanStmt.toString());
            stmt.setConsistencyLevel(readConsistencyLevel);

            if (_debug) {
                System.out.println(stmt.toString());
            }

            ResultSet rs = session.execute(stmt);

            HashMap<String, ByteIterator> tuple;
            while (!rs.isExhausted()) {
                Row row = rs.one();
                tuple = new HashMap<>();

                ColumnDefinitions cd = row.getColumnDefinitions();

                for (ColumnDefinitions.Definition def : cd) {
                    ByteBuffer val = row.getBytesUnsafe(def.getName());
                    if (val != null) {
                        tuple.put(def.getName(),
                                new ByteArrayByteIterator(val.array()));
                    } else {
                        tuple.put(def.getName(), null);
                    }
                }

                result.add(tuple);
            }

            return OK;

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error scanning with startkey: " + startkey);
            return ERR;
        }

    }

    /**
     * Update a record in the database. Any field/value pairs in the specified
     * values HashMap will be written into the record with the specified record
     * key, overwriting any existing values with the same field name.
     *
     * @param table  The name of the table
     * @param key    The record key of the record to write.
     * @param values A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error
     */
    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        //Insert and updates provide the same functionality
        return insert(table, key, values);
    }

    /**
     * Insert a record in the database. Any field/value pairs in the specified
     * values HashMap will be written into the record with the specified record
     * key.
     *
     * @param table  The name of the table
     * @param key    The record key of the record to insert.
     * @param values A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error
     */
    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {

        try {
            Insert insertStmt = QueryBuilder.insertInto(table);

            //Add key
            insertStmt.value(PARTITION_KEY, key);

            //Add fields
            for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
                Object value;
                ByteIterator byteIterator = entry.getValue();
                value = byteIterator.toString();

                insertStmt.value(entry.getKey(), value);
            }

            insertStmt.setConsistencyLevel(writeConsistencyLevel);

            if (_debug) {
                System.out.println(insertStmt.toString());
            }

            ResultSet rs = session.execute(insertStmt);

            return OK;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return ERR;
    }

    /**
     * Delete a record from the database.
     *
     * @param table The name of the table
     * @param key   The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error
     */
    @Override
    public int delete(String table, String key) {

        try {
            Statement stmt;

            stmt = QueryBuilder.delete().from(table).where(QueryBuilder.eq(PARTITION_KEY, key));
            stmt.setConsistencyLevel(writeConsistencyLevel);

            if (_debug) {
                System.out.println(stmt.toString());
            }

            ResultSet rs = session.execute(stmt);

            return OK;
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error deleting key: " + key);
        }

        return ERR;
    }

    public int readIndex(String table, String key, String indexKey, int limit,
                         Vector<Map<String, ByteIterator>> results, MutableObject<String> nextKey) {

        try {
            Select select = QueryBuilder.select().all().from(table);

            if (indexKey != null)
                select.where(QueryBuilder.eq(PARTITION_KEY, key)).and(QueryBuilder.lt(INDEX_KEY, indexKey));
            else
                select.where(QueryBuilder.eq(PARTITION_KEY, key));

            select.orderBy(QueryBuilder.desc(INDEX_KEY)).limit(limit);

            select.setConsistencyLevel(readConsistencyLevel);

            ResultSet rs = session.execute(select);

            for (Row row : rs) {
                ColumnDefinitions cd = row.getColumnDefinitions();

                HashMap<String, ByteIterator> result = new HashMap<>();
                for (int i = 0; i < cd.size(); i++) {
                    ByteBuffer val = row.getBytesUnsafe(i);
                    if (val != null) {
                        result.put(cd.getName(i), new ByteArrayByteIterator(val.array()));
                    } else {
                        result.put(cd.getName(i), null);
                    }
                }

                results.add(result);
            }

            return OK;
        } catch (Exception e) {
            if (_debug) {
                e.printStackTrace();
                System.out.println("readIndex Error");
            }
            return ERR;
        }
    }

    public int multiInsertIndex(String table, Set<String> keys, String indexKey, Map<String, ByteIterator> values) {

        try {
            Batch bs = QueryBuilder.unloggedBatch();

            for (String key : keys) {
                Insert insertStmt = QueryBuilder.insertInto(table);

                //Add key
                insertStmt.value(PARTITION_KEY, key).value(INDEX_KEY, indexKey);

                //Add fields
                for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
                    insertStmt.value(entry.getKey(), entry.getValue().toString());
                }

                bs.add(insertStmt);
            }

            bs.setConsistencyLevel(writeConsistencyLevel);

            session.execute(bs);

//            if (_debug) {
//                System.out.println(bs.toString());
//            }
            return OK;

        } catch (Exception e) {
            if (_debug) {
                e.printStackTrace();
                System.out.println("multiInsertIndex Error");
            }
            return ERR;
        }
    }

}
