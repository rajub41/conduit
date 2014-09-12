package com.inmobi.conduit.local;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hive.hcatalog.api.HCatAddPartitionDesc;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatCreateDBDesc;
import org.apache.hive.hcatalog.api.HCatCreateTableDesc;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema.Type;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.inmobi.conduit.Cluster;
import com.inmobi.conduit.Conduit;
import com.inmobi.conduit.ConduitConfig;
import com.inmobi.conduit.ConduitConfigParser;
import com.inmobi.conduit.ConduitConstants;
import com.inmobi.conduit.DestinationStream;
import com.inmobi.conduit.FSCheckpointProvider;
import com.inmobi.conduit.HCatClientUtil;
import com.inmobi.conduit.SourceStream;
import com.inmobi.conduit.TestMiniClusterUtil;
import com.inmobi.conduit.distcp.TestMergePartition;
import com.inmobi.conduit.distcp.TestMergedStreamService;
import com.inmobi.conduit.distcp.TestMirrorPartition;
import com.inmobi.conduit.distcp.TestMirrorStreamService;

public class LocalStreamPartitionTest extends TestMiniClusterUtil {
  private static final Log LOG = LogFactory.getLog(LocalStreamPartitionTest.class);
  private static final int msPort = 20104;
  private static final int msPort1 = 20105;
  private static HiveConf hcatConf;
  private static HiveConf hcatConf1;
  private List<HCatClientUtil> hcatUtilList = new ArrayList<HCatClientUtil>();

  public static void startMetaStoreServer() throws Exception {

    Thread hcatServer = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          HiveConf hiveconf = new HiveConf();
          hiveconf.set("hive.metastore.warehouse.dir", new File("target/metastore").getAbsolutePath());
          hiveconf.set("javax.jdo.option.ConnectionURL",
              "jdbc:derby:;databaseName=target/test/junit_metastore_db;create=true");
          hiveconf.set("javax.jdo.option.ConnectionDriverName",
              "org.apache.derby.jdbc.EmbeddedDriver");
          hiveconf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
          hiveconf.setIntVar(HiveConf.ConfVars. METASTORETHRIFTFAILURERETRIES, 3);
          hiveconf.setIntVar(HiveConf.ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY, 3);
          hiveconf.setIntVar(HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT, 60);
          HiveMetaStore.startMetaStore(msPort, null, hiveconf);
        } catch (Throwable t) {
          throw new RuntimeException(t);
        }
      }
    });
    hcatServer.start();
    
    Thread hcatServer1 = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          HiveConf hiveconf = new HiveConf();
          hiveconf.set("hive.metastore.warehouse.dir", new File("target/metastore1").getAbsolutePath());
          hiveconf.set("javax.jdo.option.ConnectionURL",
              "jdbc:derby:;databaseName=target/test/junit_metastore_db1;create=true");
          hiveconf.set("javax.jdo.option.ConnectionDriverName",
              "org.apache.derby.jdbc.EmbeddedDriver");
          hiveconf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
          hiveconf.setIntVar(HiveConf.ConfVars. METASTORETHRIFTFAILURERETRIES, 3);
          hiveconf.setIntVar(HiveConf.ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY, 3);
          hiveconf.setIntVar(HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT, 60);
          HiveMetaStore.startMetaStore(msPort1, null, hiveconf);
        } catch (Throwable t) {
          throw new RuntimeException(t);
        }
      }
    });
    hcatServer1.start();
    Thread.sleep(10000);

    hcatConf = new HiveConf();
    System.out.println("AAAAAAAAAAAAAAAAAAAAAAa hcat metastore before set : "
        + hcatConf.getVar(HiveConf.ConfVars.METASTOREURIS));
    System.out.println("AAAAAAAAAAAAAAAAa metastore db : " + hcatConf.get("hive.metastore.warehouse.dir"));
    hcatConf.set("hive.metastore.local", "false");
    hcatConf.set("hive.metastore.warehouse.dir", new File("target/metastore").getAbsolutePath());
    hcatConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:"
        + msPort);
    hcatConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
    hcatConf.setIntVar(HiveConf.ConfVars. METASTORETHRIFTFAILURERETRIES, 3);
    hcatConf.setIntVar(HiveConf.ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY, 3);
    hcatConf.setIntVar(HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT, 60);
     
    hcatConf1 = new HiveConf();
    
    hcatConf1.set("hive.metastore.local", "false");
    hcatConf1.set("hive.metastore.warehouse.dir", new File("target/metastore1").getAbsolutePath());
    hcatConf1.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:"
        + msPort1);
    hcatConf1.set("javax.jdo.option.ConnectionURL",
        "jdbc:derby:;databaseName=../build/test/junit_metastore_db1;create=true");
    hcatConf1.set("javax.jdo.option.ConnectionDriverName",
        "org.apache.derby.jdbc.EmbeddedDriver");
    hcatConf1.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
    hcatConf1.setIntVar(HiveConf.ConfVars. METASTORETHRIFTFAILURERETRIES, 3);
    hcatConf1.setIntVar(HiveConf.ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY, 3);
    hcatConf1.setIntVar(HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT, 60);
   
  }

  //@BeforeSuite
  public void setup() throws Exception {
    // clean up the test data if any thing is left in the previous runs
    cleanup();
    super.setup(2, 6, 1);
    System.setProperty(ConduitConstants.AUDIT_ENABLED_KEY, "true");
    Conduit.setHCatEnabled(true);
    startMetaStoreServer();
    createHCatDBAndTable();
   //createExpectedOutput();
  }
  
  private void createHCatDBAndTable() throws HCatException {
//  client.
    HCatClientUtil hcatClientUtil = new HCatClientUtil(hcatConf.getVar(HiveConf.ConfVars.METASTOREURIS));
    try {
      hcatClientUtil.createHCatClients(10, hcatConf);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    HCatClientUtil hcatClientUtil1 = new HCatClientUtil(hcatConf1.getVar(HiveConf.ConfVars.METASTOREURIS));
    try {
      hcatClientUtil1.createHCatClients(10, hcatConf1);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    hcatUtilList.add(0, hcatClientUtil);
    hcatUtilList.add(1, hcatClientUtil1);
    HCatClient client = null; // = HCatClient.create(new Configuration(hcatConf));
    HCatClient client1 = null;// = HCatClient.create(new Configuration(hcatConf1));
    try {
      client = hcatUtilList.get(0).getHCatClient();
    } catch (InterruptedException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    
    try {
      client1 = hcatUtilList.get(1).getHCatClient();
    } catch (InterruptedException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    String dbName1 = "test_conduit";
    String tableName = "conduit_local_test1";
    String mergeTableName = "conduit_test1";
    Conduit.setHcatDBName(dbName1);
    LOG.info("AAAAAAAAAAAAAAAAAAAAAAAAAAAAa Going to create dbs");

    //client.dropDatabase(dbName1, true, HCatClient.DropDBMode.CASCADE);
    //sclient1.dropDatabase(dbName1, true, HCatClient.DropDBMode.CASCADE);

    HCatCreateDBDesc dbDesc = HCatCreateDBDesc.create(dbName1)
        .ifNotExists(true).build();
    client.createDatabase(dbDesc);
    LOG.info("AAAAAAAAAAAAAAAAAAA client 1   db is created ");
    client1.createDatabase(dbDesc);

LOG.info("AAAAAAAAAAAAAAAAAAA clinet 2 db is created ");
    ArrayList<HCatFieldSchema> cols = new ArrayList<HCatFieldSchema>();
    cols.add(new HCatFieldSchema("stringcolumn", Type.STRING, "id columns"));

    ArrayList<HCatFieldSchema> ptnCols = new ArrayList<HCatFieldSchema>();
    ptnCols.add(new HCatFieldSchema("year", Type.STRING, "id columns"));
    ptnCols.add(new HCatFieldSchema("month", Type.STRING,
        "view time columns"));
    ptnCols.add(new HCatFieldSchema("day", Type.STRING, ""));
    ptnCols.add(new HCatFieldSchema("hour", Type.STRING,
        "IP Address of the User"));
    ptnCols.add(new HCatFieldSchema("minute", Type.STRING,
        "IP Address of the User"));
    HCatCreateTableDesc tableDesc = HCatCreateTableDesc
        .create(dbName1, tableName, cols).fileFormat("sequencefile")
        .partCols(ptnCols).build();
    client.createTable(tableDesc);
    LOG.info("AAAAAAAAAAAAAAAAAAAA table11111111111111111111111 client  " + tableDesc);

    client1.createTable(tableDesc);
    LOG.info("AAAAAAAAAAAAAAAAAAAA table2222222222222222222222 client1111111111111111111111  " + tableDesc);

    HCatCreateTableDesc mergeTableDesc = HCatCreateTableDesc
        .create(dbName1, mergeTableName, cols).fileFormat("sequencefile")
        .partCols(ptnCols).build();
    client.createTable(mergeTableDesc);
    LOG.info("AAAAAAAAAAAAAAAAAAAA table111111111111111111 merge  client  " + tableDesc);

    client1.createTable(mergeTableDesc);
    LOG.info("AAAAAAAAAAAAAAAAAAAA table2222222222222222  merge client11111111111111111  " + tableDesc);

    
    Map<String, String> firstPtn = new HashMap<String, String>();
    firstPtn.put("year", "2014");
    firstPtn.put("month", "09");
    firstPtn.put("day", "10");
    firstPtn.put("hour", "09");
    firstPtn.put("minute", "20");

    HCatAddPartitionDesc addPtn = HCatAddPartitionDesc.create(dbName1,
        tableName, "file:///tmp/mergeservicetest/testcluster1/mergeservice/streams_local/test1/2014/09/10/09/20", firstPtn).build();
    client.addPartition(addPtn);

    HCatAddPartitionDesc addPtn2 = HCatAddPartitionDesc.create(dbName1,
        tableName, "file:///tmp/mergeservicetest/testcluster2/mergeservice/streams_local/test1/2014/09/10/09/20", firstPtn).build();
    client1.addPartition(addPtn2);
    
    HCatAddPartitionDesc mergeAddPtn = HCatAddPartitionDesc.create(dbName1,
        mergeTableName, "file:///tmp/mergeservicetest/testcluster1/mergeservice/streams/test1/2014/09/10/09/20", firstPtn).build();
    client.addPartition(mergeAddPtn);

    HCatAddPartitionDesc mirrorAddPtn = HCatAddPartitionDesc.create(dbName1,
        mergeTableName, "file:///tmp/mergeservicetest/testcluster2/mergeservice/streams/test1/2014/09/10/09/20", firstPtn).build();
    client1.addPartition(mirrorAddPtn);
   LOG.info("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAa sample part : " + addPtn);
    TestLocalPartition.setHCatClient(dbName1);
    TestMergePartition.setHCatClient(dbName1);
    TestMirrorPartition.setHCatClient(dbName1);
  }

  //@AfterSuite
  public void cleanup() throws Exception {
    super.cleanup();
  }

  //@Test
  private void testMapReduce() throws Exception {

    //String fileName = "test-lss-hcat-conduit.xml";
    String fileName = "test-mss-hcat-conduit.xml";
    int timesToRun = 2;
    boolean throttle = false;
    ConduitConfigParser parser = new ConduitConfigParser(fileName);
    ConduitConfig config = parser.getConfig();
    Set<String> streamsToProcess = new HashSet<String>();
    streamsToProcess.addAll(config.getSourceStreams().keySet());
    Set<String> clustersToProcess = new HashSet<String>();
    Set<TestLocalStreamService> services = new HashSet<TestLocalStreamService>();
    Set<TestMergedStreamService> mergeServices = new HashSet<TestMergedStreamService>();
    Set<TestMirrorStreamService> mirrorServices = new HashSet<TestMirrorStreamService>();

    for (SourceStream sStream : config.getSourceStreams().values()) {
      for (String cluster : sStream.getSourceClusters()) {
        clustersToProcess.add(cluster);
      }
    }

    int j = 1;
    for (String clusterName : clustersToProcess) {
      HCatClientUtil hcatUtil = hcatUtilList.get(j--);
      Cluster cluster = config.getClusters().get(clusterName);
      cluster.getHadoopConf().set("mapred.job.tracker",
          super.CreateJobConf().get("mapred.job.tracker"));
      TestLocalStreamService service = new TestLocalPartition(config,
          cluster, null, new FSCheckpointProvider(cluster.getCheckpointDir()),
          streamsToProcess, hcatUtil);
      cluster.getHadoopConf().set("mapred.job.tracker", "local");
     
      services.add(service);
      TestMergedStreamService mergeService = null;
      TestMirrorStreamService mirrorService = null;
      for (DestinationStream cStream : cluster.getDestinationStreams().values()) {
        LOG.info("AAAAAAAAAAAAAAAAAAAAAAAAAAAAA cstream " + cStream + " primary " + cStream.isPrimary() + "   cluster " + cluster.getName());

        if (cStream.isPrimary()) {
          mergeService =
              new TestMergePartition(config, cluster, cluster, null, streamsToProcess, hcatUtil);
          mergeServices.add(mergeService);
        } else {
          mirrorService = new TestMirrorPartition(config,
              config.getPrimaryClusterForDestinationStream(cStream.getName()), cluster, null, streamsToProcess, hcatUtil);
          mirrorServices.add(mirrorService);
        }
      }

      service.prepareLastAddedPartitionMap();
      if (mergeService != null) {
      mergeService.prepareLastAddedPartitionMap();
      }
      if (mirrorService != null) {
      mirrorService.prepareLastAddedPartitionMap();
      }
      service.getFileSystem().delete(
          new Path(service.getCluster().getRootDir()), true);
    }

    for (TestLocalStreamService service : services) {
      for (int i = 0; i < timesToRun; ++i) {
        service.preExecute();
        // set BYTES_PER_MAPPER to a lower value for test
        service.setBytesPerMapper(100);
        service.execute();
        /* If throttle is true then need to run the LocalStreamService
           max(total num of files per stream / files per stream) times to process all files
         */
        if (throttle) {
          service.execute();
        }
        long finishTime = System.currentTimeMillis();
        service.postExecute();
        Thread.sleep(1000);
        /*
         * check for number of times local stream service should run and no need
         * of waiting if it is the last run of service
         */
        if (timesToRun > 1 && (i < (timesToRun - 1))) {
          long sleepTime = service.getMSecondsTillNextRun(finishTime);
          Thread.sleep(sleepTime);
        }
        //Thread.sleep(60001);
      }
    }
      for (TestMergedStreamService mergeService : mergeServices) {
        mergeService.runPreExecute();
        mergeService.runExecute();
        mergeService.runPostExecute();
      }
      for (TestMirrorStreamService mirrorService : mirrorServices) {
        mirrorService.runPreExecute();
        mirrorService.runExecute();
        mirrorService.runPostExecute();
      }
     /* service.getFileSystem().delete(
          new Path(service.getCluster().getRootDir()), true);*/
    
  }

}
