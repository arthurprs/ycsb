/**
 * MongoDB client binding for YCSB.
 *
 * Submitted by Yen Pai on 5/11/2010.
 *
 * https://gist.github.com/000a66b8db2caf42467b#file_mongo_db.java
 *
 */

package com.yahoo.ycsb.db;

import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.mongodb.*;
import com.sun.corba.se.spi.activation.Server;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import org.apache.commons.lang3.mutable.MutableObject;

/**
 * MongoDB client for YCSB framework.
 *
 * Properties to set:
 *
 * mongodb.url=mongodb://localhost:27017 mongodb.database=ycsb
 * mongodb.writeConcern=acknowledged
 *
 * @author ypai
 */

/**
 * Make sure to add the shard and index collection:
 * db.runCommand( { enableSharding: "ycsb" } )
 * db.runCommand( { shardCollection: "ycsb.usertable", key: {user_id: "hashed"} )
 * db.usertable.ensureIndex( { user_id: 1, timestamp: -1 } )
 */
public class MongoDbClient extends DB {


    private static final String USER_KEY = "user_id";

    private static final String INDEX_KEY = "timestamp";

    /**
     * Used to include a field in a response.
     */
    protected static final Integer INCLUDE = 1;

    /**
     * A singleton Mongo instance.
     */
    private static MongoClient mongoclient;

    /**
     * The default write concern for the test.
     */
    private static WriteConcern writeConcern;

    /**
     * The default read preference for the test
     */
    private static ReadPreference readPreference;

    /**
     * The database to access.
     */
    private static String database;

    /**
     * Count the number of times initialized to teardown on the last {@link #cleanup()}.
     */
    private static final AtomicInteger initCount = new AtomicInteger(0);

    private static boolean _debug = false;

    /**
     * Initialize any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    @Override
    public void init() throws DBException {
        initCount.incrementAndGet();
        synchronized (initCount) {
            try {
                if (mongoclient != null) {
                    return;
                }

                _debug = Boolean.parseBoolean(getProperties().getProperty("debug", "false"));

                // initialize MongoDb driver
                Properties props = getProperties();
                String urls = props.getProperty("mongodb.url", "localhost:27017");

                String[] clients = urls.split("[, ]");
                Vector<ServerAddress> serverAddresses = new Vector<>();
                for (String client: clients) {
                    String[] hostAndPort = client.split(":");
                    if (hostAndPort.length == 1)
                        serverAddresses.add(new ServerAddress(hostAndPort[0]));
                    else
                        serverAddresses.add(new ServerAddress(hostAndPort[0], Integer.parseInt(hostAndPort[1])));
                }
                mongoclient = new MongoClient(serverAddresses);

                database = props.getProperty("mongodb.database", "ycsb");

                // Set connectionpool to size of ycsb thread pool
                int maxConnections = Integer.parseInt(props.getProperty("threadcount", "100"));

                mongoclient.getMongoOptions().setCursorFinalizerEnabled(false);
                mongoclient.getMongoOptions().setAutoConnectRetry(false);
                mongoclient.getMongoOptions().setConnectionsPerHost(maxConnections);

                // write concern
                String writeConcernType = props.getProperty("mongodb.writeConcern", "acknowledged").toLowerCase();
                switch (writeConcernType) {
                    case "errors_ignored":
                        writeConcern = WriteConcern.ERRORS_IGNORED;
                        break;
                    case "unacknowledged":
                        writeConcern = WriteConcern.UNACKNOWLEDGED;
                        break;
                    case "acknowledged":
                        writeConcern = WriteConcern.ACKNOWLEDGED;
                        break;
                    case "journaled":
                        writeConcern = WriteConcern.JOURNALED;
                        break;
                    case "replica_acknowledged":
                        writeConcern = WriteConcern.REPLICA_ACKNOWLEDGED;
                        break;
                    default:
                        System.err.println("ERROR: Invalid writeConcern: '"
                                + writeConcernType
                                + "'. "
                                + "Must be [ errors_ignored | unacknowledged | acknowledged | journaled | replica_acknowledged ]");
                        System.exit(1);
                }

                // readPreference
                String readPreferenceType = props.getProperty("mongodb.readPreference", "primary").toLowerCase();
                switch (readPreferenceType) {
                    case "primary":
                        readPreference = ReadPreference.primary();
                        break;
                    case "primary_preferred":
                        readPreference = ReadPreference.primaryPreferred();
                        break;
                    case "secondary":
                        readPreference = ReadPreference.secondary();
                        break;
                    case "secondary_preferred":
                        readPreference = ReadPreference.secondaryPreferred();
                        break;
                    case "nearest":
                        readPreference = ReadPreference.nearest();
                        break;
                    default:
                        System.err.println("ERROR: Invalid readPreference: '"
                                + readPreferenceType
                                + "'. Must be [ primary | primary_preferred | secondary | secondary_preferred | nearest ]");
                        System.exit(1);
                }

                mongoclient.setReadPreference(readPreference);
                mongoclient.setWriteConcern(writeConcern);

                for (ServerAddress client: mongoclient.getServerAddressList()) {
                    System.out.println("Connected to Mongo at " + client);
                }

            } catch (Exception e1) {
                System.err.println("Could not initialize MongoDB connection pool for Loader: "
                        + e1.toString());
                e1.printStackTrace();
            }
        }
    }

    /**
     * Cleanup any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    @Override
    public void cleanup() throws DBException {
        if (initCount.decrementAndGet() <= 0) {
            mongoclient.close();
            mongoclient = null;
        }
    }

    /**
     * Delete a record from the database.
     *
     * @param table The name of the table
     * @param key   The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    @Override
    public int delete(String table, String key) {
        com.mongodb.DB db = null;
        try {
            db = mongoclient.getDB(database);
            db.requestStart();
            DBCollection collection = db.getCollection(table);
            DBObject q = new BasicDBObject().append("_id", key);
            WriteResult res = collection.remove(q, writeConcern);
            return 0;
        } catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        } finally {
            if (db != null) {
                db.requestDone();
            }
        }
    }

    /**
     * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key.
     *
     * @param table  The name of the table
     * @param key    The record key of the record to insert.
     * @param values A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    @Override
    public int insert(String table, String key,
                      HashMap<String, ByteIterator> values) {
        com.mongodb.DB db = null;
        try {
            db = mongoclient.getDB(database);

            db.requestStart();

            DBCollection collection = db.getCollection(table);
            DBObject r = new BasicDBObject().append("_id", key);
            for (String k : values.keySet()) {
                r.put(k, values.get(k).toArray());
            }
            WriteResult res = collection.insert(r, writeConcern);
            return 0;
        } catch (Exception e) {
            e.printStackTrace();
            return 1;
        } finally {
            if (db != null) {
                db.requestDone();
            }
        }
    }

    /**
     * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table  The name of the table
     * @param key    The record key of the record to read.
     * @param fields The list of fields to read, or null for all of them
     * @param result A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    @Override
    @SuppressWarnings("unchecked")
    public int read(String table, String key, Set<String> fields,
                    HashMap<String, ByteIterator> result) {
        com.mongodb.DB db = null;
        try {
            db = mongoclient.getDB(database);

            db.requestStart();

            DBCollection collection = db.getCollection(table);
            DBObject q = new BasicDBObject().append("_id", key);
            DBObject fieldsToReturn = new BasicDBObject();

            DBObject queryResult = null;
            if (fields != null) {
                Iterator<String> iter = fields.iterator();
                while (iter.hasNext()) {
                    fieldsToReturn.put(iter.next(), INCLUDE);
                }
                queryResult = collection.findOne(q, fieldsToReturn, readPreference);
            } else {
                queryResult = collection.findOne(q, null, readPreference);
            }

            if (queryResult != null) {
                result.putAll(queryResult.toMap());
            }
            return queryResult != null ? 0 : 1;
        } catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        } finally {
            if (db != null) {
                db.requestDone();
            }
        }
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key, overwriting any existing values with the same field name.
     *
     * @param table  The name of the table
     * @param key    The record key of the record to write.
     * @param values A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    @Override
    public int update(String table, String key,
                      HashMap<String, ByteIterator> values) {
        com.mongodb.DB db = null;
        try {
            db = mongoclient.getDB(database);

            db.requestStart();

            DBCollection collection = db.getCollection(table);
            DBObject q = new BasicDBObject().append("_id", key);
            DBObject u = new BasicDBObject();
            DBObject fieldsToSet = new BasicDBObject();
            Iterator<String> keys = values.keySet().iterator();
            while (keys.hasNext()) {
                String tmpKey = keys.next();
                fieldsToSet.put(tmpKey, values.get(tmpKey).toArray());

            }
            u.put("$set", fieldsToSet);
            WriteResult res = collection.update(q, u, false, false,
                    writeConcern);
            return 0;
        } catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        } finally {
            if (db != null) {
                db.requestDone();
            }
        }
    }

    /**
     * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table       The name of the table
     * @param startkey    The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param fields      The list of fields to read, or null for all of them
     * @param result      A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    @Override
    public int scan(String table, String startkey, int recordcount,
                    Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        com.mongodb.DB db = null;
        DBCursor cursor = null;
        try {
            db = mongoclient.getDB(database);
            db.requestStart();
            DBCollection collection = db.getCollection(table);
            // { "_id":{"$gte":startKey, "$lte":{"appId":key+"\uFFFF"}} }
            DBObject scanRange = new BasicDBObject().append("$gte", startkey);
            DBObject q = new BasicDBObject().append("_id", scanRange);
            cursor = collection.find(q).limit(recordcount);
            while (cursor.hasNext()) {
                // toMap() returns a Map, but result.add() expects a
                // Map<String,String>. Hence, the suppress warnings.
                HashMap<String, ByteIterator> resultMap = new HashMap<String, ByteIterator>();

                DBObject obj = cursor.next();
                fillMap(resultMap, obj);

                result.add(resultMap);
            }

            return 0;
        } catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        } finally {
            if (db != null) {
                if (cursor != null) {
                    cursor.close();
                }
                db.requestDone();
            }
        }

    }

    /**
     * TODO - Finish
     *
     * @param resultMap
     * @param obj
     */
    @SuppressWarnings("unchecked")
    protected void fillMap(HashMap<String, ByteIterator> resultMap, DBObject obj) {
        Map<String, Object> objMap = obj.toMap();
        for (Map.Entry<String, Object> entry : objMap.entrySet()) {
            if (entry.getValue() instanceof byte[]) {
                resultMap.put(entry.getKey(), new ByteArrayByteIterator(
                        (byte[]) entry.getValue()));
            }
        }
    }

    public int readIndex(String table, String key, String indexKey, int limit,
                         Vector<Map<String, ByteIterator>> results, MutableObject<String> nextKey) {

        DBCursor cursor = null;
        try {
            com.mongodb.DB db = mongoclient.getDB(database);
            DBCollection collection = db.getCollection(table);

            BasicDBObject q = new BasicDBObject().append(USER_KEY, key);
            if (indexKey != null)
                q.append(INDEX_KEY, indexKey);

            cursor = collection.find(q).sort(new BasicDBObject().append(INDEX_KEY, -1)).limit(limit);
            while (cursor.hasNext()) {
                HashMap<String, ByteIterator> result = new HashMap<>();

                for (Object _entry : cursor.next().toMap().entrySet()) {
                    Map.Entry<String, Object> entry = (Map.Entry<String, Object>) _entry;
                    result.put(entry.getKey(), new ByteArrayByteIterator(entry.getValue().toString().getBytes()));
                }

                results.add(result);
            }

            return 0;
        } catch (Exception e) {
            if (_debug) {
                e.printStackTrace();
                System.out.println(e.toString());
            }
            return 1;
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    public int multiInsertIndex(String table, Set<String> keys, String indexKey, Map<String, ByteIterator> values) {
        com.mongodb.DB db = null;
        try {
            db = mongoclient.getDB(database);
            db.requestStart();
            DBCollection collection = db.getCollection(table);

            BasicDBObject[] objs = new BasicDBObject[keys.size()];
            int objIndex = 0;

            for (String key : keys) {
                BasicDBObject obj = new BasicDBObject();
                obj.put(USER_KEY, key);
                obj.put(INDEX_KEY, indexKey);
                for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
                    obj.put(entry.getKey(), entry.getValue().toString());
                }
                objs[objIndex++] = obj;
            }

            WriteResult wr = collection.insert(objs);

            return 0;
        } catch (Exception e) {
            if (_debug) {
                e.printStackTrace();
                System.out.println(e.toString());
            }
            return 1;
        } finally {
            if (db != null) {
                db.requestDone();
            }
        }
    }
}
