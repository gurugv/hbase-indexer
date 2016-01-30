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
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;

public class DemoIngesterGuru {
    private static List<String> switches;
    private List<String> names;
    private List<String> domains;

    public static void main(String[] args) throws Exception {
        switches = Arrays.asList(args);
        new DemoIngesterGuru().run();
    }

    public void run() throws Exception {
        Configuration conf = HBaseConfiguration.create();

        DemoSchema.createSchema(conf);



        loadData();

        ObjectMapper jsonMapper = new ObjectMapper();

        HTable htable = new HTable(conf, DemoSchema.DEMO_TABLE);
        //94c89881-9ee5-4cf1-933c-9a2afa1dad0c
        byte[] rowkey = Bytes.toBytes("94c89881-9ee5-4cf1-933c-9a2afa1dad0c");//UUID.randomUUID().toString());
        long i = 0;
        while (true) {

            Put put = new Put(rowkey);
            Get get = new Get(rowkey);
            Result oldVal = htable.get(get);
            if (oldVal != null) {
                NavigableMap<byte[], byte[]> infoMap = oldVal.getFamilyMap(DemoSchema.infoCf);
                put.add(DemoSchema.logCq, DemoSchema.oldDataCq, jsonMapper.writeValueAsBytes(infoMap));
            }


            String name = "guru" + i;

            put.add(DemoSchema.infoCf, DemoSchema.nameCq, Bytes.toBytes(name));
            put.add(DemoSchema.infoCf, DemoSchema.sequencerCq, Bytes.toBytes(i));

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
