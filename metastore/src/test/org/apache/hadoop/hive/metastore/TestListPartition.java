package org.apache.hadoop.hive.metastore;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Permission;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestListPartition {

  private static final Logger LOG = LoggerFactory.getLogger(TestHiveMetaStorePartitionSpecs.class);
  private static int msPort;
  private static HiveConf hiveConf;
  private IMetaStoreClient client;

  protected static final String DB_NAME = "testpartdb";
  protected static final String TABLE_NAME = "testparttable";

  public static class ReturnTable {
    public Table table;
    public List<List<String>> testValues;

    public ReturnTable(Table table, List<List<String>> testValues) {
      this.table = table;
      this.testValues = testValues;
    }
  }

  private static SecurityManager securityManager;

  public static class NoExitSecurityManager extends SecurityManager {

    @Override
    public void checkPermission(Permission perm) {
      // allow anything.
    }

    @Override
    public void checkPermission(Permission perm, Object context) {
      // allow anything.
    }

    @Override
    public void checkExit(int status) {

      super.checkExit(status);
      throw new RuntimeException("System.exit() was called. Raising exception. ");
    }
  }

  @Before
  public void setUp() throws Exception {
    // Get new client
    client = new HiveMetaStoreClient(hiveConf);

    // Clean up the database
    client.dropDatabase(DB_NAME, true, true, true);
    createDB(DB_NAME);
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (client != null) {
        try {
          client.close();
        } catch (Exception e) {
          // HIVE-19729: Shallow the exceptions based on the discussion in the Jira
        }
      }
    } finally {
      client = null;
    }
  }

  @BeforeClass
  public static void startMetaStoreServer() throws Exception {

    HiveConf metastoreConf = new HiveConf();
    metastoreConf.set(HiveConf.ConfVars.METASTORE_PRE_EVENT_LISTENERS.varname, "");
    metastoreConf.set(HiveConf.ConfVars.METASTORE_EVENT_LISTENERS.varname, "");
    metastoreConf.setClass(HiveConf.ConfVars.METASTORE_EXPRESSION_PROXY_CLASS.varname,
        MockPartitionExpressionForMetastore.class, PartitionExpressionProxy.class);
    msPort = MetaStoreUtils.startMetaStore(metastoreConf);
    securityManager = System.getSecurityManager();
    System.setSecurityManager(new NoExitSecurityManager());
    hiveConf = new HiveConf(TestHiveMetaStorePartitionSpecs.class);
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:"
        + msPort);
    hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
    hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname,
        "false");
    hiveConf.set(HiveConf.ConfVars.METASTORE_EXPRESSION_PROXY_CLASS.name(), MockPartitionExpressionForMetastore.class.getCanonicalName());
    System.setProperty(HiveConf.ConfVars.PREEXECHOOKS.varname, " ");
    System.setProperty(HiveConf.ConfVars.POSTEXECHOOKS.varname, " ");
  }

  @AfterClass
  public static void afterClass() throws Exception {
    LOG.info("Shutting down metastore.");
    System.setSecurityManager(securityManager);

    HiveMetaStoreClient hmsc = new HiveMetaStoreClient(hiveConf);
    hmsc.dropDatabase(DB_NAME, true, true, true);
  }

  @Test
  public void testListPartitionLocations() throws Exception {
    ReturnTable rt = createTable4PartColsParts(client);
    String tableLoc = client.getTable(DB_NAME, TABLE_NAME).getSd().getLocation();

    Map<String, String> partitionLocations = client.listPartitionLocations(DB_NAME, TABLE_NAME, (short)-1);
    Map<String, String> expectedPartitionLocations = getExpectedLocations(
        tableLoc, rt.testValues, Lists.newArrayList("yyyy", "mm", "dd"));
    assertEquals(expectedPartitionLocations, partitionLocations);

    partitionLocations = client.listPartitionLocations(DB_NAME, TABLE_NAME, (short)2);
    Map<String, String> expectedPartitionLocations2 = getExpectedLocations(tableLoc, rt.testValues.subList(0, 2),
        Lists.newArrayList("yyyy", "mm", "dd"));
    assertEquals(expectedPartitionLocations2, partitionLocations);

    partitionLocations = client.listPartitionLocations(DB_NAME, TABLE_NAME, (short)0);
    assertTrue(partitionLocations.isEmpty());

    //This method does not depend on MetastoreConf.LIMIT_PARTITION_REQUEST setting:
    partitionLocations = client.listPartitionLocations(DB_NAME, TABLE_NAME, (short)101);
    assertEquals(expectedPartitionLocations, partitionLocations);
  }

  @Test
  public void testListPartitionLocationsExternal() throws Exception {
    List<String> partCols = Lists.newArrayList("yyyy", "mm", "dd");
    List<List<String>> partVals = Lists.newArrayList();
    partVals.add(Lists.newArrayList("2020", "01", "01"));
    partVals.add(Lists.newArrayList("2020", "01", "02"));

    Table t = createTestTable(client, DB_NAME, TABLE_NAME, partCols);
    String tableLoc = client.getTable(DB_NAME, TABLE_NAME).getSd().getLocation();
    List<String> externalLocations = Lists.newArrayList("extern1", "extern2").stream().map(
        l -> tableLoc + "/" + l).collect(Collectors.toList());
    for (int i = 0; i < 2; i ++) {
      addPartitionWithLocation(client, t, partVals.get(i), externalLocations.get(i));
    }

    Map<String, String> expected = new HashMap<>();
    for (int i = 0; i < partVals.size(); i++) {
      List<String> currVals = partVals.get(i);
      String partName =
          IntStream.range(0, partCols.size()).mapToObj(j -> partCols.get(j) + "=" + currVals.get(j)).collect(joining("/"));
      expected.put(partName, externalLocations.get(i));
    }
    Map<String, String> locations = client.listPartitionLocations(DB_NAME, TABLE_NAME, (short)-1);
    assertEquals(expected, locations);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionLocationsNoDbName() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionLocations("", TABLE_NAME, (short)-1);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testListPartitionLocationsNoTblName() throws Exception {
    createTable4PartColsParts(client);
    client.listPartitionLocations(DB_NAME, "", (short)-1);
  }

  @Test(expected=NoSuchObjectException.class)
  public void testListPartitionLocationsNoTable() throws Exception {
    client.listPartitionLocations(DB_NAME, TABLE_NAME, (short)-1);
  }

  @Test(expected=NoSuchObjectException.class)
  public void testListPartitionLocationsNoDb() throws Exception {
    client.dropDatabase(DB_NAME);
    client.listPartitionLocations(DB_NAME, TABLE_NAME, (short)-1);
  }

  @Test
  public void testListPartitionLocationsNullDbName() throws Exception {
    try {
      createTable4PartColsParts(client);
      client.listPartitionLocations(null, TABLE_NAME, (short)-1);
      fail("Should have thrown exception");
    } catch (NullPointerException | TTransportException e) {
    }
  }

  @Test
  public void testListPartitionLocationsNullTblName() throws Exception {
    try {
      createTable4PartColsParts(client);
      client.listPartitionLocations(DB_NAME, (String)null, (short)-1);
      fail("Should have thrown exception");
    } catch (NullPointerException | TTransportException e) {
    }
  }

  private void createDB(String name) throws TException {
    Database db = new Database(name, null, null, null);
    client.createDatabase(db);
  }

  protected Table createTestTable(IMetaStoreClient client, String dbName, String tableName,
                                  List<String> partCols)
      throws TException {
    List<FieldSchema> partColumns = partCols.stream().map(
        col -> new FieldSchema(col, "string", "")).collect(toList());

    List<FieldSchema> columns = Lists.newArrayList(
        new FieldSchema("id", "int", ""),
        new FieldSchema("name", "string", ""));
    StorageDescriptor sd = createStorageDescriptor(columns);

    Table t = new Table(tableName, dbName, null, 0, 0, 0, sd, partColumns, new HashMap<String, String>(), null, null, null);
    client.createTable(t);
    return t;
  }

  protected StorageDescriptor createStorageDescriptor(List<FieldSchema> columns) {
    SerDeInfo serdeInfo = new SerDeInfo("LBCSerDe", LazyBinaryColumnarSerDe.class.getCanonicalName(), new HashMap<>());
    return new StorageDescriptor(columns, null,
        "org.apache.hadoop.hive.ql.io.RCFileInputFormat",
        "org.apache.hadoop.hive.ql.io.RCFileOutputFormat",
        false, 0, serdeInfo, null, null, null);
  }

  protected void addPartition(IMetaStoreClient client, Table table, List<String> values)
      throws TException {
    Partition p = new Partition();
    p.setValues(values);
    p.setTableName(table.getTableName());
    p.setDbName(table.getDbName());
    p.setSd(createStorageDescriptor(table.getSd().getCols()));
    client.add_partition(p);
  }

  protected void addPartitionWithLocation(IMetaStoreClient client, Table table,
                                          List<String> values, String location) throws TException {
    Partition p = new Partition();
    p.setValues(values);
    p.setTableName(table.getTableName());
    p.setDbName(table.getDbName());
    p.setSd(createStorageDescriptor(table.getSd().getCols()));
    p.getSd().setLocation(location);
    client.add_partition(p);
  }

  protected ReturnTable createTable4PartColsParts(IMetaStoreClient client) throws
      Exception {
    Table t = createTestTable(client, DB_NAME, TABLE_NAME, Lists.newArrayList("yyyy", "mm", "dd"));
    List<List<String>> testValues = Lists.newArrayList(
        Lists.newArrayList("1999", "01", "02"),
        Lists.newArrayList("2009", "02", "10"),
        Lists.newArrayList("2017", "10", "26"),
        Lists.newArrayList("2017", "11", "27"));

    for(List<String> vals : testValues) {
      addPartition(client, t, vals);
    }
    return new ReturnTable(t, testValues);
  }

  protected Map<String, String> getExpectedLocations(String tableLoc,
                                                     List<List<String>> values, List<String> partCols) {
    Map<String, String> results = new HashMap<>();
    for (List<String> value : values) {
      List<String> kvPair = new ArrayList<String>();
      for (int i = 0; i < partCols.size(); i++) {
        kvPair.add(partCols.get(i) + "=" + value.get(i));
      }
      String partLoc = tableLoc + "/" + kvPair.stream().collect(joining("/"));
      String partName = kvPair.stream().collect(joining("/"));
      results.put(partName, partLoc);
    }
    return results;
  }
}
