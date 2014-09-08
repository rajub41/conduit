package com.inmobi.conduit;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatAddPartitionDesc;
import org.apache.hive.hcatalog.cli.SemanticAnalysis.HCatSemanticAnalyzer;
import org.apache.hive.hcatalog.common.HCatException;

public class HCatClientUtil {
  private static final Log LOG = LogFactory.getLog(HCatClientUtil.class);
  private String metastoreURL = null;
  protected BlockingQueue<HCatClient> buffer;

  public HCatClientUtil(String metastoreURL) {
    this.metastoreURL = metastoreURL;
  }

  public void createHCatClients(int numOfHCatClients)
      throws HCatException, InterruptedException {
    HiveConf hcatConf = new HiveConf();
    hcatConf.set("hive.metastore.local", "false");
    hcatConf.setVar(HiveConf.ConfVars.METASTOREURIS, metastoreURL);
    
    buffer = new LinkedBlockingDeque<HCatClient>(numOfHCatClients);
    for (int i = 0; i < numOfHCatClients; i++) {
      HCatClient hcatClient = HCatClient.create(hcatConf);
      buffer.put(hcatClient);
      LOG.info("HCatClient is created " + hcatClient);
    }
    LOG.info("Total number of hcat cleints are " + buffer.size());;
  }

  public HCatClient getHCatClient() throws InterruptedException {
    if (buffer != null) {
      LOG.info("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA buffer size : " + buffer.size());
      return buffer.poll(60, TimeUnit.SECONDS);
    } else {
      return null;
    }
  }
}