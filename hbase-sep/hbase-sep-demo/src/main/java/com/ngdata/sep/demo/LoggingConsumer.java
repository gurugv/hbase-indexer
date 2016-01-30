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

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * A simple consumer that just logs the events.
 */
public class LoggingConsumer {
    private static List<String> switches;
    private static Configuration conf;

    public static void main(String[] args) throws Exception {
        switches = Arrays.asList(args);
        conf = HBaseConfiguration.create();
        conf.setBoolean("hbase.replication", true);

        ZooKeeperItf zk = ZkUtil.connect("localhost", 20000);
        SepModel sepModel = new SepModelImpl(zk, conf);

        final String subscriptionName = "logger";

        if (!sepModel.hasSubscription(subscriptionName)) {
            sepModel.addSubscriptionSilent(subscriptionName);
        }

        PayloadExtractor payloadExtractor = new BasePayloadExtractor(Bytes.toBytes("sep-user-demo"), Bytes.toBytes("info"),
                Bytes.toBytes("payload"));

        SepConsumer sepConsumer = new SepConsumer(subscriptionName, 0, new EventLogger(), 1, "slm-dev2.nm.flipkart.com", zk, conf,
                payloadExtractor);

        sepConsumer.start();
        System.out.println("Started");

        while (true) {
            Thread.sleep(Long.MAX_VALUE);
        }
    }

    private static class EventLogger implements EventListener {
        private static long lastSeqReceived = -1;


        @Override
        public void processEvents(List<SepEvent> sepEvents) {
            HTable htable = null;
            try {
                htable = new HTable(conf, DemoSchema.DEMO_TABLE);

            } catch (IOException e) {
                e.printStackTrace();
            }
            if (switches.contains("delay")) {
                System.out.println("Sleeping ");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            for (SepEvent sepEvent : sepEvents) {
                System.out.println("Received event: ");
                boolean allDelete = false;
                for(KeyValue  kv : sepEvent.getKeyValues()){
                    if(kv.isDelete() ){
                        allDelete = true;
                    }else{
                        allDelete = false;
                        break;
                    }
                }
                if(allDelete){
                    System.out.println("Skpping event , looks like all delete : "+sepEvent.getKeyValues().size());
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    continue;
                }
                String tableName = Bytes.toString(sepEvent.getTable());
                System.out.println("  table = " + tableName);
                System.out.println("  row = " + Bytes.toString(sepEvent.getRow()));
                System.out.println("  payload = " + Bytes.toString(sepEvent.getPayload()));
                System.out.println("  key values = ");

                Get getAllVer = new Get(sepEvent.getRow());
                try {
                    getAllVer.setMaxVersions(1000);
                    Result result = htable.get(getAllVer);
                    List<KeyValue> column = result.getColumn(DemoSchema.logCq, DemoSchema.oldDataCq);
                    if (column.size() == 0) {
                        System.out.println(" ALLREADY CONSUMED ?? ");
                    }else {
                        System.out.println("AllVersuibs - " + column.size());
                        System.out.println(column.get(0));
                    }
                    Delete deleteOldVers = new Delete();
                    for (int i = 0; i < column.size(); i++) {
                        deleteOldVers.addDeleteMarker(column.get(i));
                    }
                    htable.delete(deleteOldVers);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                for (KeyValue kv : sepEvent.getKeyValues()) {

                    if (new String(kv.getKey()).contains("sequencer")) {
                        long currentSq = Bytes.toLong(kv.getValue());
                        if (lastSeqReceived != -1) {
                            if (currentSq == lastSeqReceived + 1) {
                                //ok
                            } else {
                                System.out.println("SEQUENCE NOT OK !!!! " + currentSq + " :: " + lastSeqReceived);

                                if (!switches.contains("nowait")) {
                                    try {
                                        Thread.sleep(100000);
                                    } catch (InterruptedException e) {
                                        e.printStackTrace();
                                    }
                                    throw new RuntimeException("SEQUENCE NOT OK " + currentSq + " : " + lastSeqReceived);
                                }

                            }
                        }
                        lastSeqReceived = currentSq;
                    }
                    System.out.println(lastSeqReceived + "    " + new String(kv.getKey()) + " - (" + kv.toString() + ") - " + new String(kv.getValue()) + " : " + new Date(kv.getTimestamp()));
                }
            }
        }
    }
}
