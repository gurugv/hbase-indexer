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
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class DemoSchema {

    public static final String DEMO_TABLE = "sep-user-demo-4";
    // column qualifiers
    public static final byte[] nameCq = Bytes.toBytes("name");
    public static final byte[] sequencerCq = Bytes.toBytes("sequencer");
    public static final byte[] oldDataCq = Bytes.toBytes("oldData");
    public static final byte[] updateMapCq = Bytes.toBytes("updateMap");
    public static final byte[] infoCf = Bytes.toBytes("info");
    public static final byte[] logCq = Bytes.toBytes("log");
    public static final int LAG_TOLARANCE = 10000;

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        createSchema(conf);
    }

    public static void createSchema(Configuration hbaseConf) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(hbaseConf);
        if (!admin.tableExists(DEMO_TABLE)) {
            HTableDescriptor tableDescriptor = new HTableDescriptor(DEMO_TABLE);

            HColumnDescriptor infoCf = new HColumnDescriptor("info");
            infoCf.setScope(1);
            tableDescriptor.addFamily(infoCf);
            HColumnDescriptor logCf = new HColumnDescriptor("log");
            logCf.setScope(1);
            logCf.setMaxVersions(LAG_TOLARANCE);
            logCf.setCompressionType(Compression.Algorithm.LZO);
            tableDescriptor.addFamily(logCf);
            admin.createTable(tableDescriptor);
        }
        admin.close();
    }
}
