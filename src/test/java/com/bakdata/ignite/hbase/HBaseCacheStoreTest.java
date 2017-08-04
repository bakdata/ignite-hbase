package com.bakdata.ignite.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.bakdata.commons.serialization.ObjectSerializer;
import com.bakdata.commons.serialization.Serializer;
import com.bakdata.commons.serialization.StringSerializer;
import com.google.protobuf.ServiceException;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map.Entry;
import javax.cache.CacheException;
import javax.cache.configuration.FactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class HBaseCacheStoreTest {

  private static final byte[] QUALIFIER = "value".getBytes();
  private static final String TABLE_NAME = "TEST";
  private static final int TEST_DIRECTORY_MAX_LENGTH = 65;
  private static final String TEST_DIRECTORY_INVALID_MESSAGE =
      "HBase test directory name too long. Max. " + TEST_DIRECTORY_MAX_LENGTH
          + " characters allowed. Please set an appropriate directory using the system property "
          + HBaseTestingUtility.BASE_TEST_DIRECTORY_KEY;
  private static HBaseTestingUtility utility;
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @BeforeClass
  public static void setupClass() throws Exception {
    utility = new HBaseTestingUtility();
    Path dataTestDir = utility.getDataTestDir().getParent();
    int length = dataTestDir.toString().length();
    if (length > TEST_DIRECTORY_MAX_LENGTH) {
      System.err.println(TEST_DIRECTORY_INVALID_MESSAGE);
      System.err.println("Current HBase test directory: " + dataTestDir);
      throw new RuntimeException(TEST_DIRECTORY_INVALID_MESSAGE);
    }
    utility.startMiniCluster();
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    utility.shutdownMiniCluster();
  }

  private static void applyHBaseConfiguration(HBaseCacheStoreSessionListener cssl) {
    for (Entry<String, String> entry : utility.getConfiguration()) {
      cssl.addProperty(entry.getKey(), entry.getValue());
    }
  }

  private static void deleteTable(Admin admin, TableName tableName) throws IOException {
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
  }

  private static Connection getHBaseConnection() throws ServiceException, IOException {
    Configuration conf = utility.getConfiguration();
    HBaseAdmin.checkHBaseAvailable(conf);
    return ConnectionFactory.createConnection(conf);
  }

  private static IgniteConfiguration prepareConfig() {
    return prepareConfig(true);
  }

  private static IgniteConfiguration prepareConfig(boolean writeBehind) {
    HBaseCacheStoreSessionListener cssl = new HBaseCacheStoreSessionListener(TABLE_NAME);
    applyHBaseConfiguration(cssl);
    HBaseCacheStore<Object, Object> cs = create();
    return prepareConfig(cssl, cs, writeBehind);
  }

  private static <K, V> IgniteConfiguration prepareConfig(HBaseCacheStore<K, V> cs) {
    HBaseCacheStoreSessionListener cssl = new HBaseCacheStoreSessionListener(TABLE_NAME);
    applyHBaseConfiguration(cssl);
    return prepareConfig(cssl, cs, true);
  }

  private static IgniteConfiguration prepareConfig(HBaseCacheStoreSessionListener cssl) {
    HBaseCacheStore<Object, Object> cs = create();
    return prepareConfig(cssl, cs, true);
  }

  private static HBaseCacheStore<Object, Object> create() {
    return new HBaseCacheStore<Object, Object>(ObjectSerializer.INSTANCE,
        ObjectSerializer.INSTANCE);
  }

  @SuppressWarnings("unchecked")
  private static <K, V> IgniteConfiguration prepareConfig(HBaseCacheStoreSessionListener cssl,
      HBaseCacheStore<K, V> cs, boolean writeBehind) {
    IgniteConfiguration cfg = new IgniteConfiguration();
    CacheConfiguration<K, V> cacheCfg = new CacheConfiguration<>();
    cacheCfg.setWriteThrough(true);
    cacheCfg.setWriteBehindEnabled(writeBehind);
    cacheCfg.setReadThrough(true);
    cacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
    cacheCfg.setCacheStoreFactory(FactoryBuilder.factoryOf(cs));
    cacheCfg.setCacheStoreSessionListenerFactories(FactoryBuilder.factoryOf(cssl));
    cfg.setCacheConfiguration(cacheCfg);
    return cfg;
  }

  @Test
  public void testConfigurationFile() throws IOException {
    File file = folder.newFile();
    try (BufferedWriter out = new BufferedWriter(new FileWriter(file))) {
      utility.getConfiguration().writeXml(out);
    }
    HBaseCacheStoreSessionListener cssl = new HBaseCacheStoreSessionListener(TABLE_NAME);
    cssl.setConfPath(file.getPath());
    IgniteConfiguration cfg = prepareConfig(cssl);
    IgniteConfiguration cfg2 = new IgniteConfiguration(cfg);
    cfg.setGridName("first");
    cfg2.setGridName("second");
    try (Ignite ignite = Ignition.getOrStart(cfg); Ignite ignite2 = Ignition.getOrStart(cfg2)) {
      IgniteCache<String, String> cache = ignite.getOrCreateCache("myCache");
      cache.remove("Hello");
      assertNull(cache.get("Hello"));
      cache.put("Hello", "World");
      assertEquals("World", cache.get("Hello"));
    }

    try (Ignite ignite = Ignition.getOrStart(cfg); Ignite ignite2 = Ignition.getOrStart(cfg2)) {
      IgniteCache<String, String> cache = ignite.getOrCreateCache("myCache");
      assertEquals("World", cache.get("Hello"));
    }
  }

  @Test
  public void testDefaultSerializer() throws ServiceException, IOException {
    Serializer<Object> serializer = ObjectSerializer.INSTANCE;
    IgniteConfiguration cfg = prepareConfig(create());
    IgniteConfiguration cfg2 = new IgniteConfiguration(cfg);
    cfg.setGridName("first");
    cfg2.setGridName("second");
    String cacheName = "myCache";
    try (Ignite ignite = Ignition.getOrStart(cfg); Ignite ignite2 = Ignition.getOrStart(cfg2)) {
      IgniteCache<String, String> cache = ignite.getOrCreateCache(cacheName);
      cache.remove("Hello");
      assertNull(cache.get("Hello"));
      cache.put("Hello", "World");
      assertEquals("World", cache.get("Hello"));
    }
    try (Connection conn = getHBaseConnection()) {
      TableName tableName = TableName.valueOf(TABLE_NAME);
      Table table = conn.getTable(tableName);
      Get get = new Get(serializer.serialize("Hello"));
      get.addColumn(cacheName.getBytes(), QUALIFIER);
      Result result = table.get(get);
      byte[] serialized = result.getValue(cacheName.getBytes(), QUALIFIER);
      assertEquals("World", serializer.deserialize(serialized));
      assertTrue(Arrays.equals(serializer.serialize("World"), serialized));
    }
  }

  @Test
  public void testDelete() {
    IgniteConfiguration cfg = prepareConfig();
    IgniteConfiguration cfg2 = new IgniteConfiguration(cfg);
    cfg.setGridName("first");
    cfg2.setGridName("second");
    try (Ignite ignite = Ignition.getOrStart(cfg); Ignite ignite2 = Ignition.getOrStart(cfg2)) {
      IgniteCache<String, String> cache = ignite.getOrCreateCache("myCache");
      cache.put("Hello", "World");
      cache.put("Hello", "World");
      assertEquals("World", cache.get("Hello"));
      cache.remove("Hello");
      assertNull(cache.get("Hello"));
    }
  }

  @Test
  public void testDifferentCaches() {
    IgniteConfiguration cfg = prepareConfig();
    IgniteConfiguration cfg2 = new IgniteConfiguration(cfg);
    cfg.setGridName("first");
    cfg2.setGridName("second");
    try (Ignite ignite = Ignition.getOrStart(cfg); Ignite ignite2 = Ignition.getOrStart(cfg2)) {
      IgniteCache<String, String> cache = ignite.getOrCreateCache("myCache");
      IgniteCache<String, String> otherCache = ignite.getOrCreateCache("myOtherCache");
      otherCache.remove("Hello");
      assertNull(otherCache.get("Hello"));
      cache.put("Hello", "World");
      assertEquals("World", cache.get("Hello"));
    }

    try (Ignite ignite = Ignition.getOrStart(cfg); Ignite ignite2 = Ignition.getOrStart(cfg2)) {
      IgniteCache<String, String> otherCache = ignite.getOrCreateCache("myOtherCache");
      otherCache.loadCache(null);
      assertNull(otherCache.get("Hello"));
    }
  }

  @Test
  public void testLoad() throws IOException, ServiceException {
    IgniteConfiguration cfg = prepareConfig();
    IgniteConfiguration cfg2 = new IgniteConfiguration(cfg);
    cfg.setGridName("first");
    cfg2.setGridName("second");
    try (Connection conn = getHBaseConnection()) {
      deleteTable(conn.getAdmin(), TableName.valueOf(TABLE_NAME));
    }
    try (Ignite ignite = Ignition.getOrStart(cfg); Ignite ignite2 = Ignition.getOrStart(cfg2)) {
      IgniteCache<String, String> cache = ignite.getOrCreateCache("myCache");
      cache.put("Hello", "World");
      cache.put("Foo", "Bar");
      assertEquals("World", cache.get("Hello"));
      assertEquals("Bar", cache.get("Foo"));
    }

    try (Ignite ignite = Ignition.getOrStart(cfg); Ignite ignite2 = Ignition.getOrStart(cfg2)) {
      IgniteCache<String, String> cache = ignite.getOrCreateCache("myCache");
      assertEquals(0, cache.size());
      cache.loadCache(null);
      assertEquals(2, cache.size());
    }
  }

  @Test
  public void testLoadWithFilter() throws IOException, ServiceException {
    IgniteConfiguration cfg = prepareConfig();
    IgniteConfiguration cfg2 = new IgniteConfiguration(cfg);
    cfg.setGridName("first");
    cfg2.setGridName("second");
    try (Connection conn = getHBaseConnection()) {
      deleteTable(conn.getAdmin(), TableName.valueOf(TABLE_NAME));
    }
    try (Ignite ignite = Ignition.getOrStart(cfg); Ignite ignite2 = Ignition.getOrStart(cfg2)) {
      IgniteCache<String, String> cache = ignite.getOrCreateCache("myCache");
      cache.put("Hello", "World");
      cache.put("Foo", "Bar");
      assertEquals("World", cache.get("Hello"));
      assertEquals("Bar", cache.get("Foo"));
    }

    Serializer<Object> serializer = ObjectSerializer.INSTANCE;
    Filter filter = new RowFilter(CompareOp.EQUAL,
        new BinaryComparator(serializer.serialize("Hello")));
    try (Ignite ignite = Ignition.getOrStart(cfg); Ignite ignite2 = Ignition.getOrStart(cfg2)) {
      IgniteCache<String, String> cache = ignite.getOrCreateCache("myCache");
      assertEquals(0, cache.size());
      cache.loadCache(null, filter);
      assertEquals(1, cache.size());
    }
  }

  @Test
  public void testLoadWithMultipleFilters() throws IOException, ServiceException {
    IgniteConfiguration cfg = prepareConfig();
    IgniteConfiguration cfg2 = new IgniteConfiguration(cfg);
    cfg.setGridName("first");
    cfg2.setGridName("second");
    try (Connection conn = getHBaseConnection()) {
      deleteTable(conn.getAdmin(), TableName.valueOf(TABLE_NAME));
    }
    try (Ignite ignite = Ignition.getOrStart(cfg); Ignite ignite2 = Ignition.getOrStart(cfg2)) {
      IgniteCache<String, String> cache = ignite.getOrCreateCache("myCache");
      cache.put("Hello", "World");
      cache.put("Foo", "Bar");
      cache.put("Bar", "Baz");
      assertEquals("World", cache.get("Hello"));
      assertEquals("Bar", cache.get("Foo"));
      assertEquals("Baz", cache.get("Bar"));
    }

    Serializer<Object> serializer = ObjectSerializer.INSTANCE;
    Filter filter1 = new RowFilter(CompareOp.LESS_OR_EQUAL,
        new BinaryComparator(serializer.serialize("Foo")));
    Filter filter2 = new RowFilter(CompareOp.GREATER_OR_EQUAL,
        new BinaryComparator(serializer.serialize("Foo")));
    try (Ignite ignite = Ignition.getOrStart(cfg); Ignite ignite2 = Ignition.getOrStart(cfg2)) {
      IgniteCache<String, String> cache = ignite.getOrCreateCache("myCache");
      assertEquals(0, cache.size());
      cache.loadCache(null, filter1, filter2);
      assertEquals(1, cache.size());
    }
  }

  @Test(expected = CacheException.class)
  public void testLoadWithIllegalFilter() throws IOException, ServiceException {
    IgniteConfiguration cfg = prepareConfig();
    IgniteConfiguration cfg2 = new IgniteConfiguration(cfg);
    cfg.setGridName("first");
    cfg2.setGridName("second");
    try (Connection conn = getHBaseConnection()) {
      deleteTable(conn.getAdmin(), TableName.valueOf(TABLE_NAME));
    }
    try (Ignite ignite = Ignition.getOrStart(cfg); Ignite ignite2 = Ignition.getOrStart(cfg2)) {
      IgniteCache<String, String> cache = ignite.getOrCreateCache("myCache");
      cache.put("Hello", "World");
      cache.put("Foo", "Bar");
      assertEquals("World", cache.get("Hello"));
      assertEquals("Bar", cache.get("Foo"));
    }

    try (Ignite ignite = Ignition.getOrStart(cfg); Ignite ignite2 = Ignition.getOrStart(cfg2)) {
      IgniteCache<String, String> cache = ignite.getOrCreateCache("myCache");
      assertEquals(0, cache.size());
      cache.loadCache(null, "");
      fail();
    }
  }

  @Test
  public void testManualHBaseInsertion() throws ServiceException, IOException {
    IgniteConfiguration cfg = prepareConfig(false);
    IgniteConfiguration cfg2 = new IgniteConfiguration(cfg);
    cfg.setGridName("first");
    cfg2.setGridName("second");
    String cacheName = "myCache";
    try (Ignite ignite = Ignition.getOrStart(cfg); Ignite ignite2 = Ignition.getOrStart(cfg2)) {
      IgniteCache<String, String> cache = ignite.getOrCreateCache(cacheName);
      cache.remove("Hello");
      assertNull(cache.get("Hello"));
      try (Connection conn = getHBaseConnection()) {
        TableName tableName = TableName.valueOf(TABLE_NAME);
        Table table = conn.getTable(tableName);
        Serializer<Object> serializer = ObjectSerializer.INSTANCE;
        Put put = new Put(serializer.serialize("Hello"));
        put.addColumn(cacheName.getBytes(), QUALIFIER, serializer.serialize("World"));
        table.put(put);
      }
      assertEquals("World", cache.get("Hello"));
    }
  }

  @Test
  public void testManualHBaseUpdate() throws ServiceException, IOException {
    IgniteConfiguration cfg = prepareConfig(false);
    IgniteConfiguration cfg2 = new IgniteConfiguration(cfg);
    cfg.setGridName("first");
    cfg2.setGridName("second");
    String cacheName = "myCache";
    try (Ignite ignite = Ignition.getOrStart(cfg); Ignite ignite2 = Ignition.getOrStart(cfg2)) {
      IgniteCache<String, String> cache = ignite.getOrCreateCache(cacheName);
      cache.remove("Hello");
      assertNull(cache.get("Hello"));
      cache.put("Hello", "World");
      assertEquals("World", cache.get("Hello"));
      try (Connection conn = getHBaseConnection()) {
        TableName tableName = TableName.valueOf(TABLE_NAME);
        Table table = conn.getTable(tableName);
        Serializer<Object> serializer = ObjectSerializer.INSTANCE;
        Put put = new Put(serializer.serialize("Hello"));
        put.addColumn(cacheName.getBytes(), QUALIFIER, serializer.serialize("World2"));
        table.put(put);
      }
      assertEquals("World", cache.get("Hello"));
      cache.clear("Hello");
      assertEquals("World2", cache.get("Hello"));
    }
  }

  @Test
  public void testOtherSerializer() throws ServiceException, IOException {
    Serializer<String> serializer = StringSerializer.INSTANCE;
    HBaseCacheStore<String, String> csf = new HBaseCacheStore<>(serializer, serializer);
    IgniteConfiguration cfg = prepareConfig(csf);
    IgniteConfiguration cfg2 = new IgniteConfiguration(cfg);
    cfg.setGridName("first");
    cfg2.setGridName("second");
    String cacheName = "myCache";
    try (Ignite ignite = Ignition.getOrStart(cfg); Ignite ignite2 = Ignition.getOrStart(cfg2)) {
      IgniteCache<String, String> cache = ignite.getOrCreateCache(cacheName);
      cache.remove("Hello");
      assertNull(cache.get("Hello"));
      cache.put("Hello", "World");
      assertEquals("World", cache.get("Hello"));
    }
    try (Connection conn = getHBaseConnection()) {
      TableName tableName = TableName.valueOf(TABLE_NAME);
      Table table = conn.getTable(tableName);
      Get get = new Get(serializer.serialize("Hello"));
      get.addColumn(cacheName.getBytes(), QUALIFIER);
      Result result = table.get(get);
      byte[] serialized = result.getValue(cacheName.getBytes(), QUALIFIER);
      assertEquals("World", serializer.deserialize(serialized));
      assertTrue(Arrays.equals("World".getBytes(), serialized));
    }
  }

  @Test
  public void testPersistence() {
    IgniteConfiguration cfg = prepareConfig();
    IgniteConfiguration cfg2 = new IgniteConfiguration(cfg);
    cfg.setGridName("first");
    cfg2.setGridName("second");
    try (Ignite ignite = Ignition.getOrStart(cfg); Ignite ignite2 = Ignition.getOrStart(cfg2)) {
      IgniteCache<String, String> cache = ignite.getOrCreateCache("myCache");
      cache.remove("Hello");
      assertNull(cache.get("Hello"));
      cache.put("Hello", "World");
      assertEquals("World", cache.get("Hello"));
    }

    try (Ignite ignite = Ignition.getOrStart(cfg); Ignite ignite2 = Ignition.getOrStart(cfg2)) {
      IgniteCache<String, String> cache = ignite.getOrCreateCache("myCache");
      assertEquals("World", cache.get("Hello"));
    }
  }

  @Test
  public void testTableAndColumnFamilyCreation() throws IOException, ServiceException {
    try (Connection conn = getHBaseConnection()) {
      Admin admin = conn.getAdmin();
      TableName tableName = TableName.valueOf(TABLE_NAME);
      deleteTable(admin, tableName);
      assertFalse(admin.tableExists(tableName));
      IgniteConfiguration cfg = prepareConfig();
      IgniteConfiguration cfg2 = new IgniteConfiguration(cfg);
      cfg.setGridName("first");
      cfg2.setGridName("second");
      try (Ignite ignite = Ignition.getOrStart(cfg); Ignite ignite2 = Ignition
          .getOrStart(cfg2)) {
        String cacheName = "myCache";
        String otherCacheName = "myOtherCache";
        IgniteCache<String, String> cache = ignite.getOrCreateCache(cacheName);
        IgniteCache<String, String> otherCache = ignite.getOrCreateCache(otherCacheName);
        assertFalse(admin.tableExists(tableName));
        cache.put("Hello", "World");
        assertTrue(admin.tableExists(tableName));
        assertTrue(admin.getTableDescriptor(tableName).hasFamily(cacheName.getBytes()));
        assertFalse(
            admin.getTableDescriptor(tableName).hasFamily(otherCacheName.getBytes()));
        otherCache.put("Hello", "World");
        assertTrue(admin.tableExists(tableName));
        assertTrue(
            admin.getTableDescriptor(tableName).hasFamily(otherCacheName.getBytes()));
      }
    }
  }

  @Test(expected = CacheException.class)
  public void testTableNameNotNull() {
    HBaseCacheStoreSessionListener cssl = new HBaseCacheStoreSessionListener(null);
    applyHBaseConfiguration(cssl);
    IgniteConfiguration cfg = prepareConfig(cssl);
    IgniteConfiguration cfg2 = new IgniteConfiguration(cfg);
    cfg.setGridName("first");
    cfg2.setGridName("second");
    try (Ignite ignite = Ignition.getOrStart(cfg); Ignite ignite2 = Ignition.getOrStart(cfg2)) {
      IgniteCache<String, String> cache = ignite.getOrCreateCache("myCache");
      cache.put("Hello", "World");
    }
  }
}
