package com.bakdata.commons;

import com.google.protobuf.ServiceException;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class HBaseUtil {

  private static final Logger log = LoggerFactory.getLogger(HBaseUtil.class);

  //utility class
  private HBaseUtil() {

  }

  /**
   * Create HBase connection using the given configuration. Verifies whether HBase is available.
   *
   * @param config configuration to use
   * @return HBase connection
   * @throws IOException if an error occurs connecting to HBase
   */
  public static Connection createConnection(Configuration config) throws IOException {
    try {
      HBaseAdmin.checkHBaseAvailable(config);
    } catch (ServiceException e) {
      throw new IOException("No HBase connection", e);
    }
    Connection conn = ConnectionFactory.createConnection(config);
    log.info("Created HBase connection");
    return conn;
  }

  /**
   * Ensure that the given table and column family exist in an HBase instance. If they do not
   * exists, they are created
   *
   * @param conn HBase connection
   * @param tableName name of table to create
   * @param familyName name of column family to create
   * @throws IOException if an error occurs accessing HBase
   */
  public static void ensureTableAndColumnFamilyExist(Connection conn, String tableName,
      String familyName) throws IOException {
    try (Admin admin = conn.getAdmin()) {
      HColumnDescriptor family = new HColumnDescriptor(familyName);
      TableName table = TableName.valueOf(tableName);
      new AdminContext(admin).ensureTableAndColumnFamilyExists(family, table);
    }
  }

}
