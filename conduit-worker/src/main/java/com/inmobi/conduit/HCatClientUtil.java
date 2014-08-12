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
  /*  hcatConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
    hcatConf.set(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK.varname,
        HCatSemanticAnalyzer.class.getName());
    hcatConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");

    hcatConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    hcatConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");*/
    buffer = new LinkedBlockingDeque<HCatClient>(numOfHCatClients);
    for (int i = 0; i < numOfHCatClients; i++) {
      HCatClient hcatClient = HCatClient.create(hcatConf);
      buffer.put(hcatClient);
    }
  }

  public HCatClient getHCatClient() throws InterruptedException {
    if (buffer != null) {
      return buffer.poll(20, TimeUnit.SECONDS);
    } else {
      return null;
    }
  }

  public void submitBack(HCatClient hcatClient) throws InterruptedException {
    if (buffer != null) {
      buffer.offer(hcatClient);
    }
  }

  public void close() {
    Iterator<HCatClient> hcatIt = buffer.iterator();
    while(hcatIt.hasNext()) {
      try {
        hcatIt.next().close();
      } catch (HCatException e) {
       LOG.info("Exception occured while closing HCatClient ");
      }
    }
  }
}
