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
import java.util.*;

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
        int row = 0;
        String rowId = UUID.randomUUID().toString();
        while (true) {
            long startTs = System.currentTimeMillis();

            if (switches.contains("manyrows")) {
                rowkey = Bytes.toBytes("row-" + rowId + "=" + row);
            }

            Put put = new Put(rowkey);
            Get get = new Get(rowkey);
            Result oldVal = htable.get(get);
            if (oldVal != null) {
                NavigableMap<byte[], byte[]> infoMap = oldVal.getFamilyMap(DemoSchema.infoCf);
                put.add(DemoSchema.logCq, DemoSchema.oldDataCq, jsonMapper.writeValueAsBytes("{\"listingId\":\"LSTPOSE7ZHQAUNG8RHD9JPVCE\",\"productId\":\"POSE7ZHQAUNG8RHD\",\"sellerId\":\"jwyc2i409pzlfv6k\",\"listingStatus\":\"ACTIVE\",\"listingState\":\"current\",\"isAvailable\":true,\"shippingDetails\":{\"minShippingDays\":100,\"maxShippingDays\":100,\"procurementSLA\":3,\"__isset_bit_vector\":[1,1,1]},\"mrp\":{\"amount\":5068,\"currency\":\"INR\",\"__isset_bit_vector\":[1]},\"sellerSellingPrice\":{\"amount\":2534,\"currency\":\"INR\",\"__isset_bit_vector\":[1]},\"pricingAttributes\":{\"national_shipping_fee_from_buyer\":{\"amount\":100,\"currency\":\"INR\",\"__isset_bit_vector\":[1]},\"flipkart_selling_price\":{\"amount\":2534,\"currency\":\"INR\",\"__isset_bit_vector\":[1]},\"ssp\":{\"amount\":2534,\"currency\":\"INR\",\"__isset_bit_vector\":[1]},\"mrp\":{\"amount\":5068,\"currency\":\"INR\",\"__isset_bit_vector\":[1]},\"local_shipping_fee_from_buyer\":{\"amount\":0,\"currency\":\"INR\",\"__isset_bit_vector\":[1]},\"zonal_shipping_fee_from_buyer\":{\"amount\":50,\"currency\":\"INR\",\"__isset_bit_vector\":[1]}},\"listingAttributes\":{\"show_mrp\":{\"valuesList\":[{\"value\":\"yes\"}],\"hasQualifier\":false,\"isMultivalued\":false,\"hasType\":true,\"type\":\"General\",\"__isset_bit_vector\":[1,1,1]},\"service_profile\":{\"valuesList\":[{\"value\":\"NON_FBF\"}],\"hasQualifier\":false,\"isMultivalued\":false,\"hasType\":false,\"__isset_bit_vector\":[1,1,1]},\"items_confirmed_by_seller\":{\"valuesList\":[{\"value\":\"0\"}],\"hasQualifier\":false,\"isMultivalued\":false,\"hasType\":false,\"__isset_bit_vector\":[1,1,1]},\"listing_version\":{\"valuesList\":[{\"value\":\"65185468\"}],\"hasQualifier\":false,\"isMultivalued\":false,\"hasType\":false,\"__isset_bit_vector\":[1,1,1]},\"internal_state\":{\"valuesList\":[{\"value\":\"ACTIVE\"}],\"hasQualifier\":false,\"isMultivalued\":false,\"hasType\":false,\"__isset_bit_vector\":[1,1,1]},\"inventory_count\":{\"valuesList\":[{\"value\":\"30\"}],\"hasQualifier\":false,\"isMultivalued\":false,\"hasType\":true,\"type\":\"General\",\"__isset_bit_vector\":[1,1,1]},\"return_policy\":{\"valuesList\":[{\"value\":\"10_day_replacement\"}],\"hasQualifier\":false,\"isMultivalued\":true,\"hasType\":true,\"type\":\"General\",\"__isset_bit_vector\":[1,1,1]},\"sku_id\":{\"valuesList\":[{\"value\":\"FF0066CS\"}],\"hasQualifier\":false,\"isMultivalued\":false,\"hasType\":true,\"type\":\"General\",\"__isset_bit_vector\":[1,1,1]},\"items_approved\":{\"valuesList\":[{\"value\":\"0\"}],\"hasQualifier\":false,\"isMultivalued\":false,\"hasType\":false,\"__isset_bit_vector\":[1,1,1]},\"max_order_quantity_allowed\":{\"valuesList\":[{\"value\":\"8\"}],\"hasQualifier\":false,\"isMultivalued\":false,\"hasType\":true,\"type\":\"General\",\"__isset_bit_vector\":[1,1,1]},\"fk_release_date\":{\"valuesList\":[{\"value\":\"2015-05-28 21:08:53\"}],\"hasQualifier\":false,\"isMultivalued\":false,\"hasType\":true,\"type\":\"General\",\"__isset_bit_vector\":[1,1,1]},\"shipping_days\":{\"valuesList\":[{\"value\":\"3\"}],\"hasQualifier\":false,\"isMultivalued\":false,\"hasType\":true,\"type\":\"General\",\"__isset_bit_vector\":[1,1,1]},\"listing_status\":{\"valuesList\":[{\"value\":\"ACTIVE\"}],\"hasQualifier\":false,\"isMultivalued\":false,\"hasType\":false,\"__isset_bit_vector\":[1,1,1]},\"items_cancelled_by_seller\":{\"valuesList\":[{\"value\":\"0\"}],\"hasQualifier\":false,\"isMultivalued\":false,\"hasType\":false,\"__isset_bit_vector\":[1,1,1]},\"is_available\":{\"valuesList\":[{\"value\":\"true\"}],\"hasQualifier\":false,\"isMultivalued\":false,\"hasType\":false,\"__isset_bit_vector\":[1,1,1]},\"stock_size\":{\"valuesList\":[{\"value\":\"30\"}],\"hasQualifier\":false,\"isMultivalued\":false,\"hasType\":true,\"type\":\"General\",\"__isset_bit_vector\":[1,1,1]},\"is_live\":{\"valuesList\":[{\"value\":\"true\"}],\"hasQualifier\":false,\"isMultivalued\":false,\"hasType\":true,\"type\":\"System Attributes\",\"__isset_bit_vector\":[1,1,1]},\"actual_stock_size\":{\"valuesList\":[{\"value\":\"30\"}],\"hasQualifier\":false,\"isMultivalued\":false,\"hasType\":false,\"__isset_bit_vector\":[1,1,1]},\"seller_listing_state\":{\"valuesList\":[{\"value\":\"current\"}],\"hasQualifier\":false,\"isMultivalued\":false,\"hasType\":false,\"__isset_bit_vector\":[1,1,1]}},\"tagDetails\":{\"santa.promos.discovery\":[{\"tagId\":\"b:mp:c:046e8c0c06.\",\"tagStatus\":\"ACTIVE\",\"tenant\":\"santa.promos.discovery\",\"tagAssociationData\":\"\"},{\"tagId\":\"b:mp:c:0381ea8e10.\",\"tagStatus\":\"ACTIVE\",\"tenant\":\"santa.promos.discovery\",\"tagAssociationData\":\"\"},{\"tagId\":\"b:mp:c:06b3414524.\",\"tagStatus\":\"ACTIVE\",\"tenant\":\"santa.promos.discovery\",\"tagAssociationData\":\"\"},{\"tagId\":\"b:mp:c:02a3bd1917.\",\"tagStatus\":\"ACTIVE\",\"tenant\":\"santa.promos.discovery\",\"tagAssociationData\":\"\"},{\"tagId\":\"b:mp:c:03f29f9723.\",\"tagStatus\":\"ACTIVE\",\"tenant\":\"santa.promos.discovery\",\"tagAssociationData\":\"\"},{\"tagId\":\"b:mp:c:06b75b2525.\",\"tagStatus\":\"ACTIVE\",\"tenant\":\"santa.promos.discovery\",\"tagAssociationData\":\"\"},{\"tagId\":\"b:mp:c:046d487013.\",\"tagStatus\":\"ACTIVE\",\"tenant\":\"santa.promos.discovery\",\"tagAssociationData\":\"\"},{\"tagId\":\"b:mp:c:065a70e707.\",\"tagStatus\":\"ACTIVE\",\"tenant\":\"santa.promos.discovery\",\"tagAssociationData\":\"\"}],\"igor.stores\":[{\"tagId\":\"tjw\",\"tagName\":\"Posters\",\"tagStatus\":\"ACTIVE\",\"tenant\":\"igor.stores\",\"tagAssociationData\":\"\"},{\"tagId\":\"3vm\",\"tagName\":\"Wall Decor\",\"tagStatus\":\"ACTIVE\",\"tenant\":\"igor.stores\",\"tagAssociationData\":\"\"},{\"tagId\":\"att\",\"tagName\":\"Clocks & Wall Decor\",\"tagStatus\":\"ACTIVE\",\"tenant\":\"igor.stores\",\"tagAssociationData\":\"\"},{\"tagId\":\"1m7\",\"tagName\":\"Home Decor & Festive Needs\",\"tagStatus\":\"ACTIVE\",\"tenant\":\"igor.stores\",\"tagAssociationData\":\"\"},{\"tagId\":\"all\",\"tagName\":\"All\",\"tagStatus\":\"ACTIVE\",\"tenant\":\"igor.stores\",\"tagAssociationData\":\"\"}],\"santa.promos.disbursal\":[{\"tagId\":\"b:mp:p:06b0416725.\",\"tagStatus\":\"ACTIVE\",\"tenant\":\"santa.promos.disbursal\",\"tagAssociationData\":\"\"},{\"tagId\":\"b:mp:p:068d77bb07.\",\"tagStatus\":\"ACTIVE\",\"tenant\":\"santa.promos.disbursal\",\"tagAssociationData\":\"\"},{\"tagId\":\"b:mp:p:06e444b407.\",\"tagStatus\":\"ACTIVE\",\"tenant\":\"santa.promos.disbursal\",\"tagAssociationData\":\"\"}]},\"__isset_bit_vector\":[1]}"));

                put.add(DemoSchema.logCq, DemoSchema.updateMapCq, Bytes.toBytes(i));
            }


            String name = "guru" + i;

            put.add(DemoSchema.infoCf, DemoSchema.nameCq, Bytes.toBytes(name));
            put.add(DemoSchema.infoCf, DemoSchema.sequencerCq, Bytes.toBytes(i));

            htable.put(put);
            System.out.println("Added row " + Bytes.toString(rowkey) + " ( " + i + ")");
            i++;
            if (!switches.contains("nowait"))
                Thread.sleep(1, 500); //assuming atleast a single ms delay between updates to same row, park concurrency for now.
            if (i % 10000 == 0) {
                row++;
                System.out.println("Time taken to put 10000 versions: " + Long.toString(System.currentTimeMillis() - startTs));
            }
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
