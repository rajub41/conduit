package com.inmobi.conduit.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
/*import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;*/
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestHiveUtil {
 /* private static final Log LOG = LogFactory.getLog(TestHiveUtil.class);
  static HiveConf hiveConf = null;
  //@BeforeTest
  public void setUp() {
LOG.info("AAAAAAAAAAAAAAAAAAAAAAAAAAA start hive server");
 hiveConf = TestHCatUtil.getHcatConf(20102, "/tmp/metadata/testhive/", "testHive");
    TestHCatUtil.startMetaStoreServer(hiveConf, 20102);
  }

  public static HiveConf getHiveConf() {
    return hiveConf;
  }
  //@Test
  public void testHive() {
    LOG.info("AAAAAAAAAAA test testHive");
    TestHCatUtil thutil = new TestHCatUtil();
    
    try {
      TestHCatUtil.createDatabase("conduitDB");
    } catch (Exception e) {
      LOG.warn("AAAAAAAAAAAAAAAAAa got exception while creating db ", e);  
    }
    org.apache.hadoop.hive.ql.metadata.Table table = null;

    try {
      LOG.info("AAAAAAAAAAAAAa going to create table now ");
      table = thutil.createTable("conduitDB", "tabletest");
    } catch (Exception e) {
      LOG.warn("AAAAAAAAAAAAAAAAa got exception while creating table ", e);
    }
    Map<String, String> partSpec = new HashMap<String, String>();
    partSpec.put("year", "2014");
    partSpec.put("month", "10");
    partSpec.put("day", "12");
    partSpec.put("hour", "10");
    partSpec.put("minute", "12");
    try {
      LOG.info("AAAAAAAAAAAAAAAAA going to create add partition ");
      Hive.get().createPartition(table, partSpec);
      LOG.info("AAAAAAAAAAAAAAAAAAAAA list of partitions : " + Hive.get().getPartitions(table));
      List<String> list = new ArrayList<String>();
      list.addAll(partSpec.values());
      
      LOG.info("AAAAAAAAAAAAAAA part values " + list);
      //Hive.get().dropPartition("conduitDB","tabletest", list, true);
      //LOG.info("AAAAAAAAAAAAAAAAAAAAA list of partitions after dropping : " + Hive.get().getPartitions(table));
    } catch (HiveException e) {
      LOG.warn("AAAAAAAAAAAAAAAAAA exception while creating partition : ", e);
    }
  }*/
}
