package com.bakdata.ignite.hbase;

import com.bakdata.commons.HBaseUtil;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import javax.cache.CacheException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.cache.store.CacheStoreSessionListener;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.resources.LoggerResource;

/**
 * CacheStoreSessionListener which injects {@link Table} into {@link HBaseCacheStore}.
 */
public class HBaseCacheStoreSessionListener implements CacheStoreSessionListener, LifecycleAware,
    Serializable {

  private static final long serialVersionUID = -4614910847286800963L;
  private final Collection<String> ensuredColumnFamilies = new HashSet<>();
  private transient Connection conn;

  // not final due to serialization
  private String tableName;
  private String confPath;
  private Properties properties;

  @LoggerResource
  private transient IgniteLogger logger;

  /**
   * @deprecated Do not use. Exists only to provide a no-arg constructor for Ignite and factories.
   */
  @Deprecated
  public HBaseCacheStoreSessionListener() {
  }

  /**
   * @param tableName HBase table to use for cache persistence
   */
  public HBaseCacheStoreSessionListener(String tableName) {
    this.tableName = tableName;
  }

  /**
   * Add property to establish connection to HBase.
   *
   * @param key valid HBase property
   * @param value value
   */
  public void addProperty(String key, String value) {
    if (properties == null) {
      properties = new Properties();
    }
    properties.setProperty(key, value);
  }

  @Override
  public void onSessionEnd(CacheStoreSession ses, boolean commit) {
    Table table = ses.attach(null);

    if (table != null) {
      try {
        table.close();
      } catch (IOException e) {
        String msg = "Failed to close table connection";
        logger.warning(msg, e);
        throw new CacheException(msg, e);
      }
    }

  }

  @Override
  public void onSessionStart(CacheStoreSession ses) {
    if (ses.attachment() == null) {
      String familyName = ses.cacheName();
      verifyIntegrity(familyName);
      try {
        Table table = conn.getTable(TableName.valueOf(tableName));
        ses.attach(table);
      } catch (IOException e) {
        logger.warning("Error preparing session", e);
        throw new CacheException(e);
      }
    }
  }

  /**
   * Set path to HBase configuration file to establish a connection to HBase. Path must be valid for
   * every worker. All valid hadoop paths are allowed.
   *
   * @param confPath path to XML configuration file
   */
  public void setConfPath(String confPath) {
    this.confPath = confPath;
  }

  /**
   * Set properties to establish a connection to HBase. All HBase properties are allowed.
   *
   * @param properties properties
   */
  public void setProperties(Properties properties) {
    this.properties = properties;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  @Override
  public void start() throws IgniteException {
    if (conn == null) {
      try {
        conn = HBaseUtil.createConnection(createConfig());
      } catch (IOException e) {
        throw new IgniteException(e);
      }
    }
  }

  @Override
  public void stop() throws IgniteException {
    try {
      if (conn != null && !conn.isClosed()) {
        conn.close();
      }
    } catch (IOException e) {
      logger.error("Failed to close HBase connection", e);
      throw new IgniteException(e);
    }
  }

  private Configuration createConfig() {
    Configuration conf = HBaseConfiguration.create();
    applyConfPath(conf);
    applyProperties(conf);
    return conf;
  }

  private void applyProperties(Configuration conf) {
    if (properties != null) {
      for (String propertyName : properties.stringPropertyNames()) {
        conf.set(propertyName, properties.getProperty(propertyName));
      }
    }
  }

  private void applyConfPath(Configuration conf) {
    if (confPath != null) {
      Path hbaseSitePath = new Path(confPath);
      conf.addResource(hbaseSitePath);
    }
  }

  private void verifyIntegrity(String familyName) throws CacheException {
    if (tableName == null) {
      String msg = "TableName must not be null";
      logger.warning(msg);
      throw new CacheException(msg);
    }
    if (!ensuredColumnFamilies.contains(familyName)) {
      try {
        HBaseUtil.ensureTableAndColumnFamilyExist(conn, tableName, familyName);
        ensuredColumnFamilies.add(familyName);
      } catch (IOException e) {
        logger.warning(
            "Error ensuring that HBase table '" + tableName + "' and column family '"
                + familyName + "' exists", e);
        throw new CacheException(e);
      }
    }
  }

}
