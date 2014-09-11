package com.inmobi.conduit.local;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.util.Shell;
import org.apache.hive.hcatalog.api.HCatAddPartitionDesc;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatCreateDBDesc;
import org.apache.hive.hcatalog.api.HCatCreateTableDesc;
import org.apache.hive.hcatalog.api.HCatDatabase;
import org.apache.hive.hcatalog.api.HCatPartition;
import org.apache.hive.hcatalog.cli.SemanticAnalysis.HCatSemanticAnalyzer;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema.Type;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import org.testng.Assert;

public class TestLocalHcatClient {
  private static final Log LOG = LogFactory.getLog(TestLocalHcatClient.class);
  private static final int msPort = 20102;
  private static HiveConf hcatConf;
  private static SecurityManager securityManager;

  private static class RunMS implements Runnable {

    @Override
    public void run() {
      try {
        //  HiveMetaStore.main(new String[]{"-v", "-p", msPort});
        System.out.println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAa called hivemetastore main ");
      } catch (Throwable t) {
        System.out.println("Exiting. Got exception from metastore: "+ t);
      }
    }
  }

  @AfterTest
  public static void tearDown() throws Exception {
    LOG.info("Shutting down metastore.");
//    System.setSecurityManager(securityManager);
  }

  @BeforeClass
  public static void startMetaStoreServer() throws Exception {

    Thread hcatServer = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          HiveConf hiveconf = new HiveConf();
          hiveconf.set("hive.metastore.warehouse.dir", new File("target/metastore").getAbsolutePath());
          hiveconf.set("javax.jdo.option.ConnectionURL",
              "jdbc:derby:;databaseName=../build/test/junit_metastore_db;create=true");
          hiveconf.set("javax.jdo.option.ConnectionDriverName",
              "org.apache.derby.jdbc.EmbeddedDriver");
          HiveMetaStore.startMetaStore(msPort, null, hiveconf);
        } catch (Throwable t) {
          throw new RuntimeException(t);
        }
      }
    });
    hcatServer.start();
    Thread.sleep(10000);

    hcatConf = new HiveConf();
    hcatConf.set("hive.metastore.local", "false");
    hcatConf.set("hive.metastore.warehouse.dir", "/tmp/user/hive/warehouse");

    hcatConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:"
        + msPort);
  }

  /*  public static String fixPath(String path) {
    if(!Shell.WINDOWS) {
      return path;
    }
    String expectedDir = path.replaceAll("\\\\", "/");
    if (!expectedDir.startsWith("/")) {
      expectedDir = "/" + expectedDir;
    }
    return expectedDir;
  }
   */  
  @Test
  public void testBasicDDLCommands() throws Exception {
    String db = "testdb";
    String tableOne = "testTable1";
    String tableTwo = "testTable2";
    System.out.println("AAAAAAAAAAAAAAAAA warehouse dir : " + hcatConf.get("hive.metastore.warehouse.dir"));
    System.out.println("AAAAAAAAAAAAAAAAAAA metastore " + hcatConf.get("hive.metastore.uris"));
    Thread.sleep(10000);
    HCatClient client = HCatClient.create(new Configuration(hcatConf));
    System.out.println("AAAAAAAAAAAAAAAAAAA cerated hcatc client  " + client);
    client.dropDatabase(db, true, HCatClient.DropDBMode.CASCADE);

    HCatCreateDBDesc dbDesc = HCatCreateDBDesc.create(db).ifNotExists(false)
        .build();
    client.createDatabase(dbDesc);
    LOG.info("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA  DB is created");
    List<String> dbNames = client.listDatabaseNamesByPattern("*");
    Assert.assertTrue(dbNames.contains("default"));
    Assert.assertTrue(dbNames.contains(db));

    HCatDatabase testDb = client.getDatabase(db);
    Assert.assertTrue(testDb.getComment() == null);
    Assert.assertTrue(testDb.getProperties().size() == 0);
    //    String warehouseDir = System
    //    .getProperty("test.warehouse.dir", "/user/hive/warehouse");
    /*    String expectedDir = fixPath(warehouseDir).replaceFirst("pfile:///", "pfile:/");
    Assert.assertEquals(expectedDir + "/" + db + ".db", testDb.getLocation());
    ArrayList<HCatFieldSchema> cols = new ArrayList<HCatFieldSchema>();
    cols.add(new HCatFieldSchema("id", Type.INT, "id comment"));
    cols.add(new HCatFieldSchema("value", Type.STRING, "value comment"));
    HCatCreateTableDesc tableDesc = HCatCreateTableDesc
      .create(db, tableOne, cols).fileFormat("rcfile").build();
    client.createTable(tableDesc);
     */   /* HCatTable table1 = client.getTable(db, tableOne);
    assertTrue(table1.getInputFileFormat().equalsIgnoreCase(
      RCFileInputFormat.class.getName()));
    assertTrue(table1.getOutputFileFormat().equalsIgnoreCase(
      RCFileOutputFormat.class.getName()));
    assertTrue(table1.getSerdeLib().equalsIgnoreCase(
      ColumnarSerDe.class.getName()));
    assertTrue(table1.getCols().equals(cols));
    // Since "ifexists" was not set to true, trying to create the same table
    // again
    // will result in an exception.
    try {
      client.createTable(tableDesc);
      fail("Expected exception");
    } catch (HCatException e) {
      assertTrue(e.getMessage().contains(
        "AlreadyExistsException while creating table."));
    }

    client.dropTable(db, tableOne, true);
      */   /* HCatCreateTableDesc tableDesc2 = HCatCreateTableDesc.create(db,
      tableTwo, cols).fieldsTerminatedBy('\001').escapeChar('\002').linesTerminatedBy('\003').
      mapKeysTerminatedBy('\004').collectionItemsTerminatedBy('\005').nullDefinedAs('\006').build();
    client.createTable(tableDesc2);
    HCatTable table2 = client.getTable(db, tableTwo);
    assertTrue(table2.getInputFileFormat().equalsIgnoreCase(
      TextInputFormat.class.getName()));
    assertTrue(table2.getOutputFileFormat().equalsIgnoreCase(
      IgnoreKeyTextOutputFormat.class.getName()));
    assertTrue("SerdeParams not found", table2.getSerdeParams() != null);
    assertEquals("checking " + serdeConstants.FIELD_DELIM, Character.toString('\001'),
      table2.getSerdeParams().get(serdeConstants.FIELD_DELIM));
    assertEquals("checking " + serdeConstants.ESCAPE_CHAR, Character.toString('\002'),
      table2.getSerdeParams().get(serdeConstants.ESCAPE_CHAR));
    assertEquals("checking " + serdeConstants.LINE_DELIM, Character.toString('\003'),
      table2.getSerdeParams().get(serdeConstants.LINE_DELIM));
    assertEquals("checking " + serdeConstants.MAPKEY_DELIM, Character.toString('\004'),
      table2.getSerdeParams().get(serdeConstants.MAPKEY_DELIM));
    assertEquals("checking " + serdeConstants.COLLECTION_DELIM, Character.toString('\005'),
      table2.getSerdeParams().get(serdeConstants.COLLECTION_DELIM));
    assertEquals("checking " + serdeConstants.SERIALIZATION_NULL_FORMAT, Character.toString('\006'),
      table2.getSerdeParams().get(serdeConstants.SERIALIZATION_NULL_FORMAT));

    assertEquals((expectedDir + "/" + db + ".db/" + tableTwo).toLowerCase(), table2.getLocation().toLowerCase());*/
   // client.close();
  }

  @Test
  public void testPartitionsHCatClientImpl() throws Exception {
    HCatClient client = HCatClient.create(new Configuration(hcatConf));
    String dbName = "ptnDB";
    String tableName = "pageView";
    client.dropDatabase(dbName, true, HCatClient.DropDBMode.CASCADE);

    HCatCreateDBDesc dbDesc = HCatCreateDBDesc.create(dbName)
        .ifNotExists(true).build();
    client.createDatabase(dbDesc);
LOG.info("AAAAAAAAAAAAAAAAAAA db is created ");
    ArrayList<HCatFieldSchema> cols = new ArrayList<HCatFieldSchema>();
    cols.add(new HCatFieldSchema("userid", Type.INT, "id columns"));
    cols.add(new HCatFieldSchema("viewtime", Type.BIGINT,
        "view time columns"));
    cols.add(new HCatFieldSchema("pageurl", Type.STRING, ""));
    cols.add(new HCatFieldSchema("ip", Type.STRING,
        "IP Address of the User"));

    ArrayList<HCatFieldSchema> ptnCols = new ArrayList<HCatFieldSchema>();
    ptnCols.add(new HCatFieldSchema("dt", Type.STRING, "date column"));
    ptnCols.add(new HCatFieldSchema("country", Type.STRING,
        "country column"));
    HCatCreateTableDesc tableDesc = HCatCreateTableDesc
        .create(dbName, tableName, cols).fileFormat("sequencefile")
        .partCols(ptnCols).build();
    LOG.info("AAAAAAAAAAAAAAAAAAAA table desc " + tableDesc);
    client.createTable(tableDesc);
   // LOG.info("AAAAAAAAAAAAAAa table is created successfully");

    //LOG.info("AAAAAAAAAAAAAAAAAAA trying to create s");
    
    Map<String, String> firstPtn = new HashMap<String, String>();
    firstPtn.put("dt", "04/30/2012");
    firstPtn.put("country", "usa");
    HCatAddPartitionDesc addPtn = HCatAddPartitionDesc.create(dbName,
        tableName, null, firstPtn).build();
    //client.addPartition(addPtn);
    LOG.info("AAAAAAAAAAAAAAAAAAA partition added successfully : " + firstPtn);
    LOG.info("AAAAAAAAAAAAAAAAAAAAAAAA adding same partition again");
    try {
      client.addPartition(addPtn);
      LOG.info("AAAAAAAAAAAAAAAAAAA partition added successfully : " + firstPtn);

    } catch (HCatException e) {
      if (e.getCause() instanceof AlreadyExistsException) {
        LOG.info("AAAAAAAAAAAAAAAAAAA  GOTTTTTTTT ALready Exception " + e.getCause());
      } else if (e.getCause() instanceof NoSuchObjectException) {
        LOG.info("AAAAAAAAAAAAAAAAAAA  GOTTTTTTTT no such object Exception " + e.getCause());
      } else {
        LOG.info("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAa other exception : " + e.getCause());
      }
    }
    LOG.info("AAAAAAAAAAAAAAAAAAAAAAAA list of partitions : " + client.getPartitions(dbName, tableName));
    client.dropPartitions(dbName, tableName, firstPtn, true);
    LOG.info("AAAAAAAAAAAAAA partition dropped " + firstPtn);
    try {
      client.dropPartitions(dbName, tableName, firstPtn, false);
      LOG.info("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA no exception while droppign second time ");
    } catch (HCatException e) {
      LOG.info("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA exception while dropping twice " + e.getCause());
    }
    /*
    Map<String, String> secondPtn = new HashMap<String, String>();
    secondPtn.put("dt", "04/12/2012");
    secondPtn.put("country", "brazil");
    HCatAddPartitionDesc addPtn2 = HCatAddPartitionDesc.create(dbName,
        tableName, null, secondPtn).build();
    client.addPartition(addPtn2);

    Map<String, String> thirdPtn = new HashMap<String, String>();
    thirdPtn.put("dt", "04/13/2012");
    thirdPtn.put("country", "argentina");
    HCatAddPartitionDesc addPtn3 = HCatAddPartitionDesc.create(dbName,
        tableName, null, thirdPtn).build();
    client.addPartition(addPtn3);

    List<HCatPartition> ptnList = client.listPartitionsByFilter(dbName,
        tableName, null);
    //assert./*assertTrue(ptnList.size() == 3);

    HCatPartition ptn = client.getPartition(dbName, tableName, firstPtn);
    Assert.assertTrue(ptn != null);

    client.dropPartitions(dbName, tableName, firstPtn, true);
    ptnList = client.listPartitionsByFilter(dbName,
        tableName, null);
    Assert.assertTrue(ptnList.size() == 2);

    List<HCatPartition> ptnListTwo = client.listPartitionsByFilter(dbName,
        tableName, "country = \"argentina\"");
    Assert.assertTrue(ptnListTwo.size() == 1);

        client.markPartitionForEvent(dbName, tableName, thirdPtn,
      PartitionEventType.LOAD_DONE);
    boolean isMarked = client.isPartitionMarkedForEvent(dbName, tableName,
      thirdPtn, PartitionEventType.LOAD_DONE);
    assertTrue(isMarked); */
    client.close();
  }



}