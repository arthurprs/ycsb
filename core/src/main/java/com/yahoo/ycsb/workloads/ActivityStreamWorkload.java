/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved. 
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.*;
import com.yahoo.ycsb.measurements.Measurements;
import org.apache.commons.lang3.mutable.MutableObject;

import java.io.IOException;
import java.util.*;

/**
 * Represents a set of clients of an activity stream. The relative
 * proportion of different kinds of operations, and other properties of the workload, are controlled
 * by parameters specified at runtime.
 * <p/>
 * Properties to control the client:
 * <UL>
 * <LI><b>fieldcount</b>: the number of fields in a record (default: 10)
 * <LI><b>fieldlength</b>: the size of each field (default: 100)
 * <LI><b>readallfields</b>: should reads read all fields (true) or just one (false) (default: true)
 * <LI><b>readproportion</b>: what proportion of operations should be reads (default: 1)
 * <LI><b>pagenextprobability</b>: probability of reading the next page (default: 0.75)
 * <LI><b>insertproportion</b>: what proportion of operations should be inserts (default: 0)
 * <LI><b>requestdistribution</b>: what distribution should be used to select the records to operate on - uniform, zipfian, hotspot, or latest (default: uniform)
 * <LI><b>maxscanlength</b>: for scans, what is the maximum number of records to scan per page (default: 50)
 * <LI><b>scanlengthdistribution</b>: for scans, what distribution should be used to choose the number of records to scan, for each scan, between 1 and maxscanlength (default: uniform)
 * <LI><b>insertorder</b>: should records be inserted in order by key ("ordered"), or in hashed order ("hashed") (default: hashed)
 * <LI><b>ignoreinserterrors</b>: if set to true the insert operations are continues ever when one of the operations failed (default: false)
 * </ul>
 */
public class ActivityStreamWorkload extends Workload {

    /**
     * The name of the database tables to run queries against.
     */

    public static String table = "usertable";

    /**
     * The name of the property for the number of fields in a record.
     */
    public static final String FIELD_COUNT_PROPERTY = "fieldcount";

    /**
     * Default number of fields in a record.
     */
    public static final String FIELD_COUNT_PROPERTY_DEFAULT = "10";

    int fieldcount;

    /**
     * The name of the property for the field length distribution. Options are "uniform", "zipfian" (favoring short records), "constant", and "histogram".
     * <p/>
     * If "uniform", "zipfian" or "constant", the maximum field length will be that specified by the fieldlength property.  If "histogram", then the
     * histogram will be read from the filename specified in the "fieldlengthhistogram" property.
     */
    public static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY = "fieldlengthdistribution";
    /**
     * The default field length distribution.
     */
    public static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";

    /**
     * The name of the property for the length of a field in bytes.
     */
    public static final String FIELD_LENGTH_PROPERTY = "fieldlength";
    /**
     * The default maximum length of a field in bytes.
     */
    public static final String FIELD_LENGTH_PROPERTY_DEFAULT = "100";

    /**
     * The name of a property that specifies the filename containing the field length histogram (only used if fieldlengthdistribution is "histogram").
     */
    public static final String FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY = "fieldlengthhistogram";
    /**
     * The default filename containing a field length histogram.
     */
    public static final String FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT = "hist.txt";

    /**
     * Generator object that produces field lengths.  The value of this depends on the properties that start with "FIELD_LENGTH_".
     */
    IntegerGenerator fieldlengthgenerator;

    /**
     * The name of the property for deciding whether to read one field (false) or all fields (true) of a record.
     */
    public static final String READ_ALL_FIELDS_PROPERTY = "readallfields";

    /**
     * The default value for the readallfields property.
     */
    public static final String READ_ALL_FIELDS_PROPERTY_DEFAULT = "true";

    boolean readallfields;

    /**
     * The name of the property for the proportion of transactions that are reads.
     */
    public static final String READ_PROPORTION_PROPERTY = "readproportion";

    /**
     * The default proportion of transactions that are reads.
     */
    public static final String READ_PROPORTION_PROPERTY_DEFAULT = "1.0";

    /**
     * The name of the property for the proportion of transactions that are aggregates.
     */
    public static final String PAGE_NEXT_PROBABILITY_PROPERTY = "pagenextprobability";

    /**
     * The default proportion of transactions that are aggregates.
     */
    public static final String PAGE_NEXT_PROBABILITY_PROPERTY_DEFAULT = "0.75";

    /**
     * The name of the property for the proportion of transactions that are inserts.
     */
    public static final String INSERT_PROPORTION_PROPERTY = "insertproportion";

    /**
     * The default proportion of transactions that are inserts.
     */
    public static final String INSERT_PROPORTION_PROPERTY_DEFAULT = "0.0";

    /**
     * The name of the property for the fan out factor.
     */
    public static final String MAX_FANOUT_FACTOR_PROPERTY = "maxfanoutfactor";

    /**
     * The default fan out factor.
     */
    public static final String MAX_FANOUT_FACTOR_PROPERTY_DEFAULT = "100";

    /**
     * The name of the property for the the distribution of requests across the keyspace. Options are "uniform", "zipfian" and "latest"
     */
    public static final String REQUEST_DISTRIBUTION_PROPERTY = "requestdistribution";

    /**
     * The default distribution of requests across the keyspace
     */
    public static final String REQUEST_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";

    /**
     * The name of the property for the max scan length (number of records)
     */
    public static final String MAX_SCAN_LENGTH_PROPERTY = "maxscanlength";

    /**
     * The default max scan length.
     */
    public static final String MAX_SCAN_LENGTH_PROPERTY_DEFAULT = "50";

    /**
     * The name of the property for the scan length distribution. Options are "uniform" and "zipfian" (favoring short scans)
     */
    public static final String SCAN_LENGTH_DISTRIBUTION_PROPERTY = "scanlengthdistribution";

    /**
     * The default max scan length.
     */
    public static final String SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";

    /**
     * The name of the property for the order to insert records. Options are "ordered" or "hashed"
     */
    public static final String INSERT_ORDER_PROPERTY = "insertorder";

    /**
     * Default insert order.
     */
    public static final String INSERT_ORDER_PROPERTY_DEFAULT = "hashed";

    /**
     * Percentage data items that constitute the hot set.
     */
    public static final String HOTSPOT_DATA_FRACTION = "hotspotdatafraction";

    /**
     * Default value of the size of the hot set.
     */
    public static final String HOTSPOT_DATA_FRACTION_DEFAULT = "0.2";

    /**
     * Percentage operations that access the hot set.
     */
    public static final String HOTSPOT_OPN_FRACTION = "hotspotopnfraction";

    /**
     * Default value of the percentage operations accessing the hot set.
     */
    public static final String HOTSPOT_OPN_FRACTION_DEFAULT = "0.8";

    /**
     * Field name prefix.
     */
    public static final String FIELD_NAME_PREFIX = "fieldnameprefix";

    /**
     * Default value of the field name prefix.
     */
    public static final String FIELD_NAME_PREFIX_DEFAULT = "field";

    /**
     * Ignore insert errors.
     */
    public static final String IGNORE_INSERT_ERRORS = "ignoreinserterrors";

    /**
     * Default value of the ignore insert errors.
     */
    public static final String IGNORE_INSERT_ERRORS_DEFAULT = "false";


    IntegerGenerator keysequence;

    DiscreteGenerator operationchooser;

    IntegerGenerator keychooser;

    CounterGenerator transactioninsertkeysequence;

    IntegerGenerator scanlength;

    boolean orderedinserts;

    int recordcount;

    String fieldnameprefix;

    boolean ignoreinserterrors;

    Measurements measurements;

    double pagenextprobability;

    private UniformIntegerGenerator fanoutfactorgenerator;

    protected static IntegerGenerator getFieldLengthGenerator(Properties p) throws WorkloadException {
        IntegerGenerator fieldlengthgenerator;
        String fieldlengthdistribution = p.getProperty(FIELD_LENGTH_DISTRIBUTION_PROPERTY, FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);
        int fieldlength = Integer.parseInt(p.getProperty(FIELD_LENGTH_PROPERTY, FIELD_LENGTH_PROPERTY_DEFAULT));
        String fieldlengthhistogram = p.getProperty(FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY, FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT);
        if (fieldlengthdistribution.compareTo("constant") == 0) {
            fieldlengthgenerator = new ConstantIntegerGenerator(fieldlength);
        } else if (fieldlengthdistribution.compareTo("uniform") == 0) {
            fieldlengthgenerator = new UniformIntegerGenerator(1, fieldlength);
        } else if (fieldlengthdistribution.compareTo("zipfian") == 0) {
            fieldlengthgenerator = new ZipfianGenerator(1, fieldlength);
        } else if (fieldlengthdistribution.compareTo("histogram") == 0) {
            try {
                fieldlengthgenerator = new HistogramGenerator(fieldlengthhistogram);
            } catch (IOException e) {
                throw new WorkloadException("Couldn't read field length histogram file: " + fieldlengthhistogram, e);
            }
        } else {
            throw new WorkloadException("Unknown field length distribution \"" + fieldlengthdistribution + "\"");
        }
        return fieldlengthgenerator;
    }

    /**
     * Initialize the scenario.
     * Called once, in the main client thread, before any operations are started.
     */
    public void init(Properties p) throws WorkloadException {
        fieldcount = Integer.parseInt(p.getProperty(FIELD_COUNT_PROPERTY, FIELD_COUNT_PROPERTY_DEFAULT));
        fieldlengthgenerator = ActivityStreamWorkload.getFieldLengthGenerator(p);

        fieldnameprefix = p.getProperty(FIELD_NAME_PREFIX, FIELD_NAME_PREFIX_DEFAULT);

        ignoreinserterrors = Boolean.parseBoolean(p.getProperty(IGNORE_INSERT_ERRORS, IGNORE_INSERT_ERRORS_DEFAULT));

        double readproportion = Double.parseDouble(p.getProperty(READ_PROPORTION_PROPERTY, READ_PROPORTION_PROPERTY_DEFAULT));
        double insertproportion = Double.parseDouble(p.getProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_PROPERTY_DEFAULT));
        recordcount = Integer.parseInt(p.getProperty(Client.RECORD_COUNT_PROPERTY));
        String requestdistrib = p.getProperty(REQUEST_DISTRIBUTION_PROPERTY, REQUEST_DISTRIBUTION_PROPERTY_DEFAULT);
        int maxscanlength = Integer.parseInt(p.getProperty(MAX_SCAN_LENGTH_PROPERTY, MAX_SCAN_LENGTH_PROPERTY_DEFAULT));
        String scanlengthdistrib = p.getProperty(SCAN_LENGTH_DISTRIBUTION_PROPERTY, SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);

        int insertstart = Integer.parseInt(p.getProperty(INSERT_START_PROPERTY, INSERT_START_PROPERTY_DEFAULT));

        readallfields = Boolean.parseBoolean(p.getProperty(READ_ALL_FIELDS_PROPERTY, READ_ALL_FIELDS_PROPERTY_DEFAULT));
        orderedinserts = !p.getProperty(INSERT_ORDER_PROPERTY, INSERT_ORDER_PROPERTY_DEFAULT).equals("hashed");
        keysequence = new CounterGenerator(insertstart);
        operationchooser = new DiscreteGenerator();
        if (readproportion > 0) {
            operationchooser.addValue(readproportion, "READ");
        }

        if (insertproportion > 0) {
            operationchooser.addValue(insertproportion, "INSERT");
        }

        transactioninsertkeysequence = new CounterGenerator(recordcount);
        if (requestdistrib.compareTo("uniform") == 0) {
            keychooser = new UniformIntegerGenerator(0, recordcount - 1);
        } else if (requestdistrib.compareTo("uniquerandom") == 0) {
            keychooser = new UniqueRandomGenerator(recordcount);
        } else if (requestdistrib.compareTo("exponential") == 0) {
            double percentile = Double.parseDouble(p.getProperty(ExponentialGenerator.EXPONENTIAL_PERCENTILE_PROPERTY,
                    ExponentialGenerator.EXPONENTIAL_PERCENTILE_DEFAULT));
            double frac = Double.parseDouble(p.getProperty(ExponentialGenerator.EXPONENTIAL_FRAC_PROPERTY,
                    ExponentialGenerator.EXPONENTIAL_FRAC_DEFAULT));
            keychooser = new ExponentialGenerator(percentile, recordcount * frac);
        } else if (requestdistrib.compareTo("zipfian") == 0) {
            //it does this by generating a random "next key" in part by taking the modulus over the number of keys
            //if the number of keys changes, this would shift the modulus, and we don't want that to change which keys are popular
            //so we'll actually construct the scrambled zipfian generator with a keyspace that is larger than exists at the beginning
            //of the test. that is, we'll predict the number of inserts, and tell the scrambled zipfian generator the number of existing keys
            //plus the number of predicted keys as the total keyspace. then, if the generator picks a key that hasn't been inserted yet, will
            //just ignore it and pick another key. this way, the size of the keyspace doesn't change from the perspective of the scrambled zipfian generator

            int opcount = Integer.parseInt(p.getProperty(Client.OPERATION_COUNT_PROPERTY));
            int expectednewkeys = (int) (((double) opcount) * insertproportion * 2.0); //2 is fudge factor

            keychooser = new ScrambledZipfianGenerator(recordcount + expectednewkeys);
        } else if (requestdistrib.compareTo("latest") == 0) {
            keychooser = new SkewedLatestGenerator(transactioninsertkeysequence);
        } else if (requestdistrib.equals("hotspot")) {
            double hotsetfraction = Double.parseDouble(p.getProperty(
                    HOTSPOT_DATA_FRACTION, HOTSPOT_DATA_FRACTION_DEFAULT));
            double hotopnfraction = Double.parseDouble(p.getProperty(
                    HOTSPOT_OPN_FRACTION, HOTSPOT_OPN_FRACTION_DEFAULT));
            keychooser = new HotspotIntegerGenerator(0, recordcount - 1,
                    hotsetfraction, hotopnfraction);
        } else {
            throw new WorkloadException("Unknown request distribution \"" + requestdistrib + "\"");
        }

        if (scanlengthdistrib.compareTo("uniform") == 0) {
            scanlength = new UniformIntegerGenerator(maxscanlength / 2, maxscanlength);
        } else if (scanlengthdistrib.compareTo("zipfian") == 0) {
            scanlength = new ZipfianGenerator(1, maxscanlength);
        } else {
            throw new WorkloadException("Distribution \"" + scanlengthdistrib + "\" not allowed for scan length");
        }

        pagenextprobability = Double.parseDouble(p.getProperty(PAGE_NEXT_PROBABILITY_PROPERTY, PAGE_NEXT_PROBABILITY_PROPERTY_DEFAULT));
        int maxfanoutfactor = Integer.parseInt(p.getProperty(MAX_FANOUT_FACTOR_PROPERTY, MAX_FANOUT_FACTOR_PROPERTY_DEFAULT));
        fanoutfactorgenerator = new UniformIntegerGenerator(maxfanoutfactor / 2, maxfanoutfactor);

        measurements = Measurements.getMeasurements();
    }

    public String buildKeyName(long keynum) {
        if (!orderedinserts) {
            keynum = Utils.hash(keynum);
        }
        return "user" + keynum;
    }

    int nextKeynum() {
        int keynum;
        do {
            keynum = keychooser.nextInt();
        }
        while (keynum > transactioninsertkeysequence.lastInt());
        return keynum;
    }

    HashMap<String, ByteIterator> buildValues() {
        HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();

        for (int i = 0; i < fieldcount; i++) {
            String fieldkey = fieldnameprefix + i;
            ByteIterator data = new RandomByteIterator(fieldlengthgenerator.nextInt());
            values.put(fieldkey, data);
        }
        return values;
    }

    /**
     * Do one transaction operation. Because it will be called concurrently from multiple client threads, this
     * function must be thread safe. However, avoid synchronized, or the threads will block waiting for each
     * other, and it will be difficult to reach the target throughput. Ideally, this function would have no side
     * effects other than DB operations.
     */
    public boolean doTransaction(DB db, Object threadstate) {
        String op = operationchooser.nextString();

        switch (op) {
            case "READ":
                doTransactionRead(db);
                break;
            case "INSERT":
                doTransactionInsert(db);
                break;
        }

        return true;
    }

    public void doTransactionRead(DB db) {
        int keyNum = nextKeynum();
        String keyName = buildKeyName(keyNum);
        MutableObject<String> indexKey = new MutableObject<>();
        Vector<Map<String, ByteIterator>> results = new Vector<>();

        while (true) {
            int len = scanlength.nextInt();
            long st = System.nanoTime();
            int result = db.readIndex(table, keyName, indexKey.getValue(), len, results, indexKey);
            long en = System.nanoTime();

            measurements.measure("READ", (int) ((en - st) / 1000));
            measurements.reportReturnCode("READ", result);

            if (indexKey.getValue() == null || Utils.random().nextDouble() > pagenextprobability)
                break;
        }
    }

    public void doTransactionInsert(DB db) {
        String indexKey = Utils.newTimestampUUID();
        int numkeys = transactioninsertkeysequence.lastInt();
        int keynum = nextKeynum();
        int fanoutfactor = fanoutfactorgenerator.nextInt();

        Set<String> keys = new HashSet<>(fanoutfactor + 1);
        // user own timeline
        keys.add(buildKeyName(keynum));
        // friends timelines
        for (int i = 0; i < fanoutfactor; i++) {
            keys.add(buildKeyName(Utils.hash(keynum + i) % numkeys));
        }

        HashMap<String, ByteIterator> values = buildValues();

        long st = System.nanoTime();
        int result = db.multiInsertIndex(table, keys, indexKey, values);
        long en = System.nanoTime();

        measurements.measure("WRITE", (int) ((en - st) / 1000));
        measurements.reportReturnCode("WRITE", result);
    }

    public boolean doRead(DB db, Object threadstate) {
        doTransactionRead(db);
        return true;
    }

    public boolean doInsert(DB db, Object threadstate) {
        doTransactionInsert(db);
        return true;
    }

}
