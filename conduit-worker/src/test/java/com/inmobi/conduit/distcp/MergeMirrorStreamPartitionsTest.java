package com.inmobi.conduit.distcp;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.mapred.pipes.Submitter;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
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
import com.inmobi.conduit.local.TestLocalPartition;
import com.inmobi.conduit.local.TestLocalStreamService;
import com.inmobi.conduit.metrics.ConduitMetrics;
import com.inmobi.conduit.utils.CalendarHelper;
import com.inmobi.conduit.utils.TestHCatUtil;
import com.inmobi.messaging.publisher.MessagePublisher;
import com.inmobi.messaging.publisher.MessagePublisherFactory;

public class MergeMirrorStreamPartitionsTest extends TestMiniClusterUtil {
  private static final Log LOG = LogFactory.getLog(MergeMirrorStreamPartitionsTest.class);

  private List<HCatClientUtil> hcatUtilList = new ArrayList<HCatClientUtil>();
  private static final int msPort1 = 20104;
  private static final int msPort2 = 20105;
  private static final String DB_NAME = "test_conduit";
  private static final String TABLE_NAME_PREFIX = "conduit";
  private static final String LOCAL_TABLE_NAME_PREFIX = TABLE_NAME_PREFIX + "_local";

  @BeforeMethod
  public void beforeTest() throws Exception{
    Properties prop = new Properties();
    prop.setProperty("com.inmobi.conduit.metrics.enabled", "true");
    prop.setProperty("com.inmobi.conduit.metrics.slidingwindowtime", "100000000");
    ConduitMetrics.init(prop);
    ConduitMetrics.startAll();

    // clean up the test data if any thing is left in the previous runs
    cleanup();
    super.setup(2, 6, 1);
    System.setProperty(ConduitConstants.AUDIT_ENABLED_KEY, "true");
    Conduit.setHCatEnabled(true);

    TestHCatUtil testHCatUtil = new TestHCatUtil();
    HiveConf hcatConf1 = TestHCatUtil.getHcatConf(msPort1, "target/metaStore1", "metadb1");
    HiveConf hcatConf2 = TestHCatUtil.getHcatConf(msPort2, "target/metaStore2", "metadb2");

    TestHCatUtil.startMetaStoreServer(hcatConf1, msPort1);
    TestHCatUtil.startMetaStoreServer(hcatConf2, msPort2);
    Thread.sleep(10000);

    HCatClientUtil hcatUtil1 = TestHCatUtil.getHCatUtil(hcatConf1);
    TestHCatUtil.createHCatClients(hcatConf1, hcatUtil1);
    HCatClientUtil hcatUtil2 = TestHCatUtil.getHCatUtil(hcatConf2);
    TestHCatUtil.createHCatClients(hcatConf2, hcatUtil2);
    hcatUtilList.add(hcatUtil1);
    hcatUtilList.add(hcatUtil2);
  }

  @AfterMethod
  public void afterTest() throws Exception{
    ConduitMetrics.stopAll();;
  }

  @Test
  public void testMergeMirrorStreamPartitionTest() throws Exception {
    testMergeMirrorStreamPartitionTest("test-mss-hcat-conduit-1.xml", null, null);
  }

  private void testMergeMirrorStreamPartitionTest(String filename, String currentClusterName,
      Set<String> additionalClustersToProcess)
          throws Exception {
    testMergeMirrorStreamPartitionTest(filename, currentClusterName,
        additionalClustersToProcess, true);
  }

  private void testMergeMirrorStreamPartitionTest(String filename,
      String currentClusterName, Set<String> additionalClustersToProcess,
      boolean addAllSourceClusters) throws Exception {

    Set<TestLocalStreamService> localStreamServices = new HashSet<TestLocalStreamService>();
    Set<TestMergedStreamService> mergedStreamServices = new HashSet<TestMergedStreamService>();
    Set<TestMirrorStreamService> mirrorStreamServices = new HashSet<TestMirrorStreamService>();

    initializeConduit(filename, currentClusterName, additionalClustersToProcess,
        addAllSourceClusters, localStreamServices, mergedStreamServices,
        mirrorStreamServices);

    LOG.info("Running LocalStream Service");

    for (TestLocalStreamService service : localStreamServices) {
      Thread.currentThread().setName(service.getName());
      service.runPreExecute();
      service.runExecute();
      service.runPostExecute();
    }

    LOG.info("Running MergedStream Service");

    for (TestMergedStreamService service : mergedStreamServices) {
      Thread.currentThread().setName(service.getName());
      service.runPreExecute();
      service.runExecute();
      service.runPostExecute();

    }
    
    LOG.info("Running MirrorStreamService Service");
    
    for (TestMirrorStreamService service : mirrorStreamServices) {
      Thread.currentThread().setName(service.getName());
      service.runPreExecute();
      service.runExecute();
      service.runPostExecute();

    }

    LOG.info("Running LocalStream Service");

    for (TestLocalStreamService service : localStreamServices) {
      Thread.currentThread().setName(service.getName());
      service.runPreExecute();
      service.runExecute();
      service.runPostExecute();
    }

    LOG.info("Running MergedStream Service");

    for (TestMergedStreamService service : mergedStreamServices) {
      Thread.currentThread().setName(service.getName());
      service.runPreExecute();
      service.runExecute();
      service.runPostExecute();

    }
    
    LOG.info("Running MirrorStreamService Service");
    
    for (TestMirrorStreamService service : mirrorStreamServices) {
      Thread.currentThread().setName(service.getName());
      service.runPreExecute();
      service.runExecute();
      service.runPostExecute();

    }

    LOG.info("Cleaning up leftovers");

    /*for (TestLocalStreamService service : localStreamServices) {
      service.getFileSystem().delete(
          new Path(service.getCluster().getRootDir()), true);
    } */
  }

  private void initializeConduit(String filename, String currentClusterName,
      Set<String> additionalClustersToProcess, boolean addAllSourceClusters,
      Set<TestLocalStreamService> localStreamServices,
      Set<TestMergedStreamService> mergedStreamServices,
      Set<TestMirrorStreamService> mirrorStreamServices)
          throws Exception {
    ConduitConfigParser parser = new ConduitConfigParser(filename);
    ConduitConfig config = parser.getConfig();

    Set<String> streamsToProcessLocal = new HashSet<String>();
    streamsToProcessLocal.addAll(config.getSourceStreams().keySet());
    for (String stream : streamsToProcessLocal) {
      LOG.info("AAAAAAAAAAAAAAAAAAAAa  dest streams   " + config.getPrimaryClusterForDestinationStream(stream));
    }
    System.setProperty(ConduitConstants.DIR_PER_DISTCP_PER_STREAM, "200");
    MessagePublisher publisher = MessagePublisherFactory.create();
    Cluster currentCluster = null;
    if (currentClusterName != null) {
      currentCluster = config.getClusters().get(currentClusterName);
      Assert.assertNotNull(currentCluster);
      Assert.assertEquals(currentClusterName, currentCluster.getName());
    }

    Set<String> clustersToProcess = new HashSet<String>();
    if (additionalClustersToProcess != null)
      clustersToProcess.addAll(additionalClustersToProcess);

    if (addAllSourceClusters) {
      for (SourceStream sStream : config.getSourceStreams().values()) {
        for (String cluster : sStream.getSourceClusters()) {
          clustersToProcess.add(cluster);
        }
      }
    }

    int i = 0;
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.HOUR_OF_DAY, -1);
    String dateStr = CalendarHelper.getDateTimeAsString(cal);
    Map<String, String> partSpec = TestHCatUtil.getPartitionMap(cal);
    List<HCatFieldSchema> ptnCols = TestHCatUtil.getPartCols();
    Conduit.setHcatDBName(DB_NAME);
    for (String clusterName : clustersToProcess) {
      HCatClientUtil hcatClientUtil = hcatUtilList.get(i++);
      HCatClient hcatClient = TestHCatUtil.getHCatClient(hcatClientUtil);
      TestHCatUtil.createDataBase(DB_NAME, hcatClient);

      Cluster cluster = config.getClusters().get(clusterName);
      cluster.getHadoopConf().set("mapred.job.tracker",
          super.CreateJobConf().get("mapred.job.tracker"));
      //TestLocalPartition.setHCatClient(DB_NAME, table);
      TestLocalStreamService service = new TestLocalPartition(config,
          cluster, currentCluster,new FSCheckpointProvider(cluster
              .getCheckpointDir()), streamsToProcessLocal, hcatClientUtil);
      String localrootDir = cluster.getLocalFinalDestDirRoot();
      LOG.info("AAAAAAAAAAAAAAAAAAAAAA: streamsToProcess : " + streamsToProcessLocal);
      for (String stream : streamsToProcessLocal) {
        String tableName = LOCAL_TABLE_NAME_PREFIX + "_" + stream;
        TestHCatUtil.createTable(hcatClient, DB_NAME, tableName, ptnCols);
        Path streamPath = new Path(localrootDir, stream);
        String location = CalendarHelper.getPathFromDate(cal.getTime(), streamPath).toString();
        TestLocalPartition.setHCatClient(DB_NAME);
        LOG.info("AAAAAAAAAAAAAAAAAa going to ceate partition with " + DB_NAME + "   " + tableName + "  " + location + "   " + partSpec);
        /*TestHCatUtil.addPartition(hcatClient, DB_NAME, tableName, location,
            partSpec);*/
      }
      localStreamServices.add(service);
      service.prepareLastAddedPartitionMap();
      service.getFileSystem().delete(
          new Path(service.getCluster().getRootDir()), true);
      cluster.getHadoopConf().set("mapred.job.tracker", "local");

      Set<String> mergedStreamRemoteClusters = new HashSet<String>();
      Set<String> mirroredRemoteClusters = new HashSet<String>();
      Map<String, Set<String>> mergedSrcClusterToStreamsMap = new HashMap<String, Set<String>>();
      Map<String, Set<String>> mirrorSrcClusterToStreamsMap = new HashMap<String, Set<String>>();
      for (DestinationStream cStream : cluster.getDestinationStreams().values()) {
        // Start MergedStreamConsumerService instances for this cluster for each
        // cluster
        // from where it has to fetch a partial stream and is hosting a primary
        // stream
        // Start MirroredStreamConsumerService instances for this cluster for
        // each cluster
        // from where it has to mirror mergedStreams


        if (cStream.isPrimary()) {
          for (String cName : config.getSourceStreams().get(cStream.getName())
              .getSourceClusters()) {
            mergedStreamRemoteClusters.add(cName);
            if (mergedSrcClusterToStreamsMap.get(cName) == null) {
              Set<String> tmp = new HashSet<String>();
              tmp.add(cStream.getName());
              mergedSrcClusterToStreamsMap.put(cName, tmp);
            } else {
              mergedSrcClusterToStreamsMap.get(cName).add(cStream.getName());
            }
          }
        }
        if (!cStream.isPrimary()) {
          LOG.info("AAAAAAAAAAAAAAAAA cStream is not primary : " + cStream.getName());
          Cluster primaryCluster = config
              .getPrimaryClusterForDestinationStream(cStream.getName());
          if (primaryCluster != null) {
            LOG.info("AAAAAAAAAAAAAAAAA primary cluster for cstream : " + primaryCluster.getName());
            mirroredRemoteClusters.add(primaryCluster.getName());
                        String clusterName1 = primaryCluster.getName();
            if (mirrorSrcClusterToStreamsMap.get(clusterName1) == null) {
              Set<String> tmp = new HashSet<String>();
              tmp.add(cStream.getName());
              mirrorSrcClusterToStreamsMap.put(clusterName1, tmp);
            } else {
              mirrorSrcClusterToStreamsMap.get(clusterName1).add(
                  cStream.getName());
            }
          } else {
            LOG.warn("AAAAAAAAAAAAAAAAAAAAAA primary cluster is null ");
          }
        }
      }
     // TestHCatUtil.submitBack(hcatClientUtil, hcatClient);
      //  TestHCatUtil.createDataBase(DB_NAME, hcatClient);
      String destRootDir = cluster.getFinalDestDirRoot();
      /* for (String stream : streamsToProcessLocal) {
         String tableName = TABLE_NAME_PREFIX + "_" + stream;
         TestHCatUtil.createTable(hcatClient, DB_NAME, tableName, ptnCols);
         Path streamPath = new Path(destRootDir, stream);
         String location = new Path(streamPath, dateStr).toString();
         TestHCatUtil.addPartition(hcatClient, DB_NAME, tableName, location,
             partSpec);
       }*/
      //int j= 0;
      for (String remote : mergedStreamRemoteClusters) {
        //HCatClientUtil hcatUtil = hcatUtilList.get(j++);
        //HCatClient hcatClient1 = TestHCatUtil.getHCatClient(hcatUtil);
        TestMergePartition.setHCatClient(DB_NAME);
        LOG.info("AAAAAAAAAAAAAAAAAAAAAAAAA merge    " + remote);
        TestMergedStreamService remoteMergeService = new TestMergePartition(
            config, config.getClusters().get(remote), cluster, currentCluster,
            mergedSrcClusterToStreamsMap.get(remote), hcatClientUtil);
        mergedStreamServices.add(remoteMergeService);
        LOG.info("AAAAAAAAAAAAAAAAAAAA remote merge : streams : " + mergedSrcClusterToStreamsMap.get(remote));
        for (String stream : mergedSrcClusterToStreamsMap.get(remote)) {
          String tableName = TABLE_NAME_PREFIX + "_" + stream;
          try {
            TestHCatUtil.createTable(hcatClient, DB_NAME, tableName, ptnCols);
          } catch (HCatException e) {
            if (e.getCause() instanceof AlreadyExistsException) {
              LOG.warn("AAAAAAAAAAAAAAAAAA Already created exception  : ");
            } else {
              LOG.warn("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAaa hcatexception ", e);
            }
          }
          Path streamPath = new Path(destRootDir, stream);
          String location = CalendarHelper.getPathFromDate(cal.getTime(), streamPath).toString();
          try {
            /*TestHCatUtil.addPartition(hcatClient, DB_NAME, tableName, location,
                partSpec);
         */ } catch (Exception e) {
            if (e.getCause() instanceof AlreadyExistsException) {
              LOG.warn("AAAAAAAAAAAAAAAAa Partition already exists in the table : " + partSpec + "   " + tableName );
            } else {
              LOG.warn("EEEEEEEEEEEEEEEEEEEEEEEEEEEEException ");
            }
          }
        }
        remoteMergeService.prepareLastAddedPartitionMap();
        if (currentCluster != null)
          Assert.assertEquals(remoteMergeService.getCurrentCluster(),
              currentCluster);
        else
          Assert.assertEquals(remoteMergeService.getCurrentCluster(), cluster);
      }

      for (String remote : mirroredRemoteClusters) {
        LOG.info("AAAAAAAAAAAAAAAAAAAAAa remote mirror : " + remote);
        LOG.info("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAa remote mirror streams: " + mirrorSrcClusterToStreamsMap.get(remote));
        TestMirrorPartition.setHCatClient(DB_NAME);
        TestMirrorStreamService remoteMirrorService = new TestMirrorPartition(
            config, config.getClusters().get(remote), cluster, currentCluster,
            mirrorSrcClusterToStreamsMap.get(remote), hcatClientUtil);
        mirrorStreamServices.add(remoteMirrorService);
        for (String stream : mirrorSrcClusterToStreamsMap.get(remote)) {
          String tableName = TABLE_NAME_PREFIX + "_" + stream;
          try {
            TestHCatUtil.createTable(hcatClient, DB_NAME, tableName, ptnCols);
          } catch (HCatException e) {
            if (e.getCause() instanceof AlreadyExistsException) {
              LOG.warn("AAAAAAAAAAAAAAAAAA Already created exception  : ");
            } else {
              LOG.warn("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAaa hcatexception ", e);
            }
          }
          Path streamPath = new Path(destRootDir, stream);
          String location = CalendarHelper.getPathFromDate(cal.getTime(), streamPath).toString();
          try {
         /*   TestHCatUtil.addPartition(hcatClient, DB_NAME, tableName, location,
                partSpec);
        */  } catch (Exception e) {
            if (e.getCause() instanceof AlreadyExistsException) {
              LOG.warn("AAAAAAAAAAAAAAAAa Partition already exists in the table : " + partSpec + "   " + tableName );
            } else {
              LOG.warn("EEEEEEEEEEEEEEEEEEEEEEEEEEEEException ");
            }
          }
        }
        remoteMirrorService.prepareLastAddedPartitionMap();
        if (currentCluster != null)
          Assert.assertEquals(remoteMirrorService.getCurrentCluster(),
              currentCluster);
        else
          Assert.assertEquals(remoteMirrorService.getCurrentCluster(), cluster);
      }
      TestHCatUtil.submitBack(hcatClientUtil, hcatClient);
    }
  }
}
