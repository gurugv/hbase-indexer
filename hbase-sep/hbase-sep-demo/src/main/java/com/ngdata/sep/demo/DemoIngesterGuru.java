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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class DemoIngesterGuru {
    private List<String> names;
    private List<String> domains;

    public static void main(String[] args) throws Exception {
        new DemoIngesterGuru().run();
    }

    public void run() throws Exception {
        Configuration conf = HBaseConfiguration.create();

        DemoSchema.createSchema(conf);

        final byte[] infoCf = Bytes.toBytes("info");

        // column qualifiers
        final byte[] nameCq = Bytes.toBytes("name");
        final byte[] emailCq = Bytes.toBytes("email");
        final byte[] ageCq = Bytes.toBytes("age");
        final byte[] payloadCq = Bytes.toBytes("payload");
        final byte[] sequencerCq = Bytes.toBytes("sequencer");

        loadData();

        ObjectMapper jsonMapper = new ObjectMapper();

        HTable htable = new HTable(conf, "sep-user-demo");
        byte[] rowkey = Bytes.toBytes(UUID.randomUUID().toString());
        long i = 0;
        while (true) {

            Put put = new Put(rowkey);

            String name = "guru" + i;
            // pickName(i++);
            String email = name.toLowerCase() + "@" + pickDomain();
            String age = String.valueOf((int) Math.ceil(Math.random() * 100));

            put.add(infoCf, nameCq, Bytes.toBytes(name));
            put.add(infoCf, emailCq, Bytes.toBytes(email));
            put.add(infoCf, ageCq, Bytes.toBytes(age));
            put.add(infoCf, payloadCq, Bytes.toBytes(UUID.randomUUID().toString()));
            put.add(infoCf, sequencerCq, Bytes.toBytes(i));

            MyPayload payload = new MyPayload();
            payload.setPartialUpdate(false);
            put.add(infoCf, payloadCq, jsonMapper.writeValueAsBytes(payload));

            htable.put(put);
            System.out.println("Added row " + Bytes.toString(rowkey) + " ( " + i + ")");
            i++;
        }
    }

    private String pickName(int i) {
        return names.get((int) Math.floor(Math.random() * names.size()));
    }

    private String pickDomain() {
        return domains.get((int) Math.floor(Math.random() * domains.size()));
    }

    private void loadData() throws IOException {
        // Names
        BufferedReader reader =
                new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("names/names.txt")));

        names = new ArrayList<String>();

        String line;
        while ((line = reader.readLine()) != null) {
            names.add(line);
        }

        // Domains
        domains = new ArrayList<String>();
        domains.add("gmail.com");
        domains.add("hotmail.com");
        domains.add("yahoo.com");
        domains.add("live.com");
        domains.add("ngdata.com");
    }
}
