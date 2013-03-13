/*
 * Copyright 2013 NGDATA nv
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
package com.ngdata.hbaseindexer;

import com.ngdata.hbaseindexer.master.IndexerMaster;
import com.ngdata.hbaseindexer.model.api.WriteableIndexerModel;
import com.ngdata.hbaseindexer.model.impl.IndexerModelImpl;
import com.ngdata.hbaseindexer.supervisor.IndexerRegistry;
import com.ngdata.hbaseindexer.supervisor.IndexerSupervisor;
import com.ngdata.hbaseindexer.util.zookeeper.StateWatchingZooKeeper;
import com.ngdata.sep.SepModel;
import com.ngdata.sep.impl.SepModelImpl;
import com.ngdata.sep.util.io.Closer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.net.DNS;

public class Main {
    private Log log = LogFactory.getLog(getClass());
    private HTablePool tablePool;
    private WriteableIndexerModel indexerModel;
    private SepModel sepModel;
    private IndexerMaster indexerMaster;
    private IndexerSupervisor indexerSupervisor;
    private StateWatchingZooKeeper zk;

    public static void main(String[] args) throws Exception {
        new Main().run(args);
    }

    public void run(String[] args) throws Exception {
        startServices();

        Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownHandler()));
    }

    void startServices() throws Exception {
        // The same configuration object is used for both hbase-indexer as for hbase client access
        Configuration conf = HBaseIndexerConfiguration.create();
        conf.setBoolean("hbase.replication", true);

        String hostname = Strings.domainNamePointerToHostName(DNS.getDefaultHost(
                conf.get("hbase.regionserver.dns.interface", "default"),
                conf.get("hbase.regionserver.dns.nameserver", "default")));

        log.debug("Using hostname " + hostname);

        String zkConnectString = conf.get(ConfKeys.ZK_CONNECT_STRING);
        int zkSessionTimeout = conf.getInt(ConfKeys.ZK_SESSION_TIMEOUT, 30000);
        zk = new StateWatchingZooKeeper(zkConnectString, zkSessionTimeout);

        tablePool = new HTablePool(conf, 10 /* TODO configurable */);

        String zkRoot = conf.get(ConfKeys.ZK_ROOT_NODE);

        indexerModel = new IndexerModelImpl(zk, zkRoot);

        sepModel = new SepModelImpl(zk, conf);

        indexerMaster = new IndexerMaster(zk, indexerModel, null, null, conf, zkConnectString, zkSessionTimeout,
                sepModel, hostname);
        indexerMaster.start();

        IndexerRegistry indexerRegistry = new IndexerRegistry();
        indexerSupervisor = new IndexerSupervisor(indexerModel, zk, hostname, indexerRegistry,
                tablePool, conf);

        indexerSupervisor.init();
    }

    void stopServices() {
        log.debug("Stopping indexer supervisor");
        Closer.close(indexerSupervisor);
        log.debug("Stopping indexer master");
        Closer.close(indexerMaster);
        log.debug("Stopping indexer model");
        Closer.close(indexerModel);
        log.debug("Stopping HBase table pool");
        Closer.close(tablePool);
        log.debug("Stopping ZooKeeper connection");
        Closer.close(zk);
    }

    public class ShutdownHandler implements  Runnable {
        @Override
        public void run() {
            stopServices();
        }
    }
}