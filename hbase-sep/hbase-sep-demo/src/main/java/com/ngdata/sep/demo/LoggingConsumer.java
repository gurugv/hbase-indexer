/*
 * Copyright 2012 NGDATA nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ngdata.sep.demo;

import com.ngdata.sep.EventListener;
import com.ngdata.sep.PayloadExtractor;
import com.ngdata.sep.SepEvent;
import com.ngdata.sep.SepModel;
import com.ngdata.sep.impl.BasePayloadExtractor;
import com.ngdata.sep.impl.SepConsumer;
import com.ngdata.sep.impl.SepModelImpl;
import com.ngdata.sep.util.zookeeper.ZkUtil;
import com.ngdata.sep.util.zookeeper.ZooKeeperItf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * A simple consumer that just logs the events.
 */
public class LoggingConsumer {
    private static List<String> switches;
    private static Configuration conf;

    private static ObjectMapper jsonMapper = new ObjectMapper();

    private static Properties props = new Properties();


    static {

        props.put("metadata.broker.list", "slm-dev1.nm.flipkart.com:9092,slm-dev2.nm.flipkart.com:9092");
        props.put("bootstrap.servers", "slm-dev1.nm.flipkart.com:9092,slm-dev2.nm.flipkart.com:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("key.serializer", org.apache.kafka.common.serialization.StringSerializer.class);
        props.put("value.serializer", org.apache.kafka.common.serialization.StringSerializer.class);
        //  props.put("partitioner.class", "example.producer.SimplePartitioner");
        props.put("request.required.acks", "1");
    }

    public static void main(String[] args) throws Exception {

        switches = Arrays.asList(args);
        conf = HBaseConfiguration.create();
        conf.setBoolean("hbase.replication", true);

        final Producer<String, String> producer = new KafkaProducer<String, String>(props);

        ZooKeeperItf zk = ZkUtil.connect("localhost", 20000);
        SepModel sepModel = new SepModelImpl(zk, conf);

        final String subscriptionName = "logger";

        if (!sepModel.hasSubscription(subscriptionName)) {
            sepModel.addSubscriptionSilent(subscriptionName);
        }

        PayloadExtractor payloadExtractor = new BasePayloadExtractor(Bytes.toBytes("sep-user-demo"), Bytes.toBytes("info"),
                Bytes.toBytes("payload"));

        int numThreads = 1;

        if (switches.contains("multithreaded"))
            numThreads = 4;

        SepConsumer sepConsumer = new SepConsumer(subscriptionName, 0, new EventLogger(producer), numThreads, "slm-dev2.nm.flipkart.com", zk, conf,
                null);

        sepConsumer.start();
        System.out.println("Started");

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println("Shutting down...");
                producer.close();
                System.out.println("Shutdown complete.");
            }
        });
        while (true) {
            Thread.sleep(Long.MAX_VALUE);
        }

    }

    static private boolean notSilent() {
        return (!switches.contains("silent"));
    }

    private static class EventLogger implements EventListener {
        private static long lastSeqReceived = -1;
        private final Producer<String, String> producer;

        public EventLogger(Producer<String, String> producer) {
            this.producer = producer;
        }


        @Override
        public void processEvents(List<SepEvent> sepEvents) {
            System.out.println(Thread.currentThread().getId() + "  : " + Thread.currentThread().getName());
            if (switches.contains("throwerror")) {

                System.out.println("Throwing erorr " + Bytes.toString(sepEvents.get(0).getRow()));
                throw new RuntimeException(sepEvents.toString()
                );
            }
            HTable htable = null;
            try {
                htable = new HTable(conf, DemoSchema.DEMO_TABLE);

            } catch (IOException e) {
                e.printStackTrace();
            }
            if (switches.contains("delay")) {
                System.out.println("Sleeping 1000ms");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            for (SepEvent sepEvent : sepEvents) {
                final long consumeStartTs = System.currentTimeMillis();
                if (notSilent())
                    System.out.println("Received event: ");
                boolean allDelete = false;
                for (KeyValue kv : sepEvent.getKeyValues()) {
                    if (kv.isDelete()) {
                        allDelete = true;
                    } else {
                        allDelete = false;
                        break;
                    }
                }
                if (allDelete) {
                    if (notSilent())
                        System.out.println("Skpping event , looks like all delete : " + sepEvent.getKeyValues().size());
                    try {
                        Thread.sleep(0);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    continue;
                }
                String tableName = Bytes.toString(sepEvent.getTable());

                String topic = tableName;
                final long getAllVerTs = System.currentTimeMillis();
                Get getAllVer = new Get(sepEvent.getRow());
                try {
                    getAllVer.setMaxVersions(DemoSchema.LAG_TOLARANCE);
                    Result result = htable.get(getAllVer);
                    if (notSilent())
                        System.out.println("Get all versions took " + Long.toString(System.currentTimeMillis() - getAllVerTs) + " ms");
                    List<KeyValue> allOldVersions = result.getColumn(DemoSchema.logCq, DemoSchema.oldDataCq);

                    if (allOldVersions.size() == 0) {
                        if (notSilent())
                            System.out.println(" ALLREADY CONSUMED ?? " + Bytes.toString(sepEvent.getRow()));
                        continue;
                    } else {

                        System.out.println("  table = " + tableName);
                        System.out.println("  row = " + Bytes.toString(sepEvent.getRow()));
                        System.out.println("  payload = " + Bytes.toString(sepEvent.getPayload()));
                        System.out.println("  key values = ");
                        System.out.println("AllVersuibs - " + allOldVersions.size());
                        String key = Bytes.toString(sepEvent.getRow());

                        if (switches.contains("waitperrow")) {
                            //
                        }
                        List<KeyValue> allUpdates = result.getColumn(DemoSchema.logCq, DemoSchema.updateMapCq);
                        List<Future<RecordMetadata>> resultList = new ArrayList<Future<RecordMetadata>>();
                        System.out.println("TODO : Take a lock on " + key + " Zk Ephemeral Node, if not already exist");
                        System.out.println("TODO : Get last processed timestamp for this key " + key + " : Get lastprocessedTs/offset from a persistent store, and mov");
                        long batchSt = System.currentTimeMillis();
                        for (int i = allUpdates.size() - 1; i >= 0; i--) {

                            KeyValue keyValue = allUpdates.get(i);
                            long currentSq = Bytes.toLong(keyValue.getValue());
                            if (lastSeqReceived != -1) {
                                if (currentSq == lastSeqReceived + 1) {

                                    System.out.println(currentSq + " OK " + allOldVersions.get(i) + " new " + currentSq);
                                    long kafkaSt = System.currentTimeMillis();
                                    ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, key, allOldVersions.get(i).toString());
                                    Future<RecordMetadata> sendResult = producer.send(producerRecord);
                                    long offset = sendResult.get().offset();
                                    System.out.println(" Sent to kafka " + offset + " - " + (System.currentTimeMillis() - kafkaSt));

                                    resultList.add(sendResult);

                                    if (switches.contains("waitpercl")) {
                                        Thread.sleep(100);
                                    }
                                } else {
                                    System.out.println("SEQUENCE NOT OK !!!! " + allOldVersions.get(i) + ":" + currentSq + " :: " + lastSeqReceived);

                                    if (!switches.contains("nowait")) {
                                        try {
                                            System.out.println("Waiting 10000ms <<backoff>>");
                                            Thread.sleep(10000);
                                        } catch (InterruptedException e) {
                                            e.printStackTrace();
                                        }
                                        //  throw new RuntimeException("SEQUENCE NOT OK " + currentSq + " : " + lastSeqReceived);
                                    }
                                }
                            }
                            lastSeqReceived = currentSq;
                        }


                        System.out.println(" Time taken to process " + allUpdates.size() + "  - " + (System.currentTimeMillis() - batchSt));
                        // resultList.get(result.size() - 1).get(); //waitfor last ack
                        final long deleteOldVersTs = System.currentTimeMillis();
                        Delete deleteOldVers = new Delete(sepEvent.getRow());
                        deleteOldVers.deleteColumns(DemoSchema.logCq, DemoSchema.oldDataCq, allOldVersions.get(0).getTimestamp());
                        deleteOldVers.deleteColumns(DemoSchema.logCq, DemoSchema.updateMapCq, allOldVersions.get(0).getTimestamp());
                        htable.delete(deleteOldVers);
                        System.out.println("Delete versions took " + Long.toString(System.currentTimeMillis() - deleteOldVersTs) + " ms");
                    }

                    System.out.println("SepEvent consumption took " + Long.toString(System.currentTimeMillis() - consumeStartTs) + " ms");
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}