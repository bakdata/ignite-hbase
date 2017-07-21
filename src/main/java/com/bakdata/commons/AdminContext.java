package com.bakdata.commons;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.InvalidFamilyOperationException;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AdminContext {

  private static final long TIMEOUT = 5000;
  private static final Logger log = LoggerFactory.getLogger(AdminContext.class);
  private final Admin admin;

  AdminContext(Admin admin) {
    this.admin = admin;
  }

  void ensureTableAndColumnFamilyExists(HColumnDescriptor family, TableName table)
      throws IOException {
    log.debug("Ensuring HBase table '{}' exists", table.getNameAsString());
    if (!admin.tableExists(table)) {
      createTable(family, table);
    } else {
      ensureColumnFamilyExists(family, table);
    }
  }

  private void ensureColumnFamilyExists(HColumnDescriptor family, TableName table)
      throws IOException {
    log.debug("Ensuring column family '{}' in HBase table '{}' exists",
        family.getNameAsString(), table.getNameAsString());
    if (!hasFamily(family, table)) {
      createColumnFamily(family, table);
    }
  }

  private boolean hasFamily(HColumnDescriptor family, TableName table) throws IOException {
    byte[] familyName = family.getName();
    HTableDescriptor tableDescriptor = admin.getTableDescriptor(table);
    return tableDescriptor.hasFamily(familyName);
  }

  private void createColumnFamily(HColumnDescriptor family, TableName table)
      throws IOException {
    try {
      admin.addColumn(table, family);
    } catch (InvalidFamilyOperationException e) {
      if (!hasFamily(family, table)) {
        //Schroedinger's cat: InvalidFamilyOperationException (cf exists) but does not exist at the same time
        throw new IllegalStateException("Column family should exist but does not", e);
      }
      //columnFamily was created in the meantime
      return;
    }
    waitForColumnFamilyCreation(family, table);
    log.info("Created column family '{}' in HBase table '{}'", family.getNameAsString(),
        table.getNameAsString());
  }

  private void waitForColumnFamilyCreation(HColumnDescriptor family, TableName table)
      throws IOException {
    try {
      StatusProvider provider = () -> hasFamily(family, table);
      if (!provider.waitForSuccess(TIMEOUT, TimeUnit.MILLISECONDS)) {
        throw new IOException("Timeout exceeded when waiting for column family creation");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Error waiting for column family creation", e);
    }
  }

  private void createTable(HColumnDescriptor family, TableName table) throws IOException {
    HTableDescriptor hbTable = new HTableDescriptor(table);
    hbTable.addFamily(family);
    try {
      admin.createTable(hbTable);
    } catch (TableExistsException e) {
      if (!admin.tableExists(table)) {
        //Schroedinger's cat: TableExistsException but does not exist at the same time
        throw new IllegalStateException("Table should exist but does not", e);
      }
      //table was created in the meantime
      return;
    }
    log.info("Created HBase table '{}'", table.getNameAsString());
  }
}
