package com.inmobi.conduit;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.service.HiveClient;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.common.HCatException;

public class HCatClientUtil {
  private static final Log LOG = LogFactory.getLog(HCatClientUtil.class);
  private String metastoreURL = null;
  protected BlockingQueue<HCatClient> buffer;

  public HCatClientUtil(String metastoreURL) {
    this.metastoreURL = metastoreURL;
  }

  public String getMetastoreUrl() {
    return metastoreURL; 
  }

  public void createHCatClients(int numOfHCatClients, HiveConf hcatConf)
      throws HCatException, InterruptedException {
    buffer = new LinkedBlockingDeque<HCatClient>(numOfHCatClients);
    for (int i = 0; i < numOfHCatClients; i++) {
      /*IMetaStoreClient hcatClient= null;
      try {
        HiveMetaHookLoader hookLoader = new HiveMetaHookLoader() {
                    @Override
                    public HiveMetaHook getHook(Table table) throws MetaException {
                       metadata hook implementation, or null if this
                       * storage handler does not need any metadata notifications
                       
                      return null;
                    }

                   
                  };
        hcatClient =  RetryingMetaStoreClient.getProxy(hcatConf,
hookLoader, HiveMetaStoreClient.class.getName());
      } catch (MetaException e) {
        
        // TODO Auto-generated catch block
        e.printStackTrace();
        throw new HCatException("AAAAAAAAAAAAAAAAa uable to create retrying metastore client");
      }
*/      HCatClient hcatClient = HCatClient.create(hcatConf);
      buffer.put(hcatClient);
    }
    LOG.info("Total number of hcat clients are " + buffer.size());
  }

  public HCatClient getHCatClient() throws InterruptedException {
    if (buffer != null) {
      return buffer.poll(30, TimeUnit.SECONDS);
    } else {
      return null;
    }
  }

  public void addToPool(HCatClient hcatClient) {
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
