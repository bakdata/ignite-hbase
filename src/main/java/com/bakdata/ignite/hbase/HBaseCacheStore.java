package com.bakdata.ignite.hbase;

import static com.google.common.base.Preconditions.checkState;

import com.bakdata.commons.serialization.SerializationException;
import com.bakdata.commons.serialization.Serializer;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.cache.Cache.Entry;
import javax.cache.CacheException;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.resources.CacheStoreSessionResource;
import org.apache.ignite.resources.LoggerResource;

/**
 * Provides persistence for Ignite cache in HBase. Connection is injected by {@link
 * HBaseCacheStoreSessionListener}.
 *
 * @param <K> type of keys stored in this cache
 * @param <V> type of values stored in this cache
 */
public class HBaseCacheStore<K, V> implements CacheStore<K, V>, Serializable {

  private static final long serialVersionUID = -3909649433524475L;
  private static final byte[] QUALIFIER = "value".getBytes();

  // not final due to serialization
  private Serializer<K> keySerializer;
  private Serializer<V> valueSerializer;

  @CacheStoreSessionResource
  private transient CacheStoreSession session;
  @LoggerResource
  private transient IgniteLogger logger;

  /**
   * @deprecated Do not use. Exists only to provide a no-arg constructor for Ignite and factories.
   */
  @Deprecated
  public HBaseCacheStore() {
  }

  /**
   * @param keySerializer serializer to use for writing and reading keys in HBase
   * @param valueSerializer serializer to use for writing and reading values in HBase
   */
  public HBaseCacheStore(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
  }

  private static <T> Stream<T> stream(Iterable<T> iterable) {
    return StreamSupport.stream(iterable.spliterator(), false);
  }

  /**
   * <p>Redirects to {@link #deleteAll(Collection)} as HBase expects batch operations anyways.</p>
   *
   * {@inheritDoc}
   */
  @Override
  public void delete(Object key) throws CacheWriterException {
    // HBase expects batches of operations
    deleteAll(Collections.singletonList(key));
  }

  @Override
  public void deleteAll(Collection<?> keys) throws CacheWriterException {
    List<Delete> deletes = createDeletes(keys);
    long start = System.currentTimeMillis();
    delete(deletes);
    long time = System.currentTimeMillis() - start;
    logger.debug("Deleted " + deletes.size() + " values in " + time + "ms");
  }

  /**
   * <p>Redirects to {@link #loadAll(Iterable)} as HBase expects batch operations anyways.</p>
   *
   * {@inheritDoc}
   */
  @Override
  public V load(K key) throws CacheLoaderException {
    // HBase expects batches of operations
    return loadAll(Collections.singletonList(key)).get(key);
  }

  @Override
  public Map<K, V> loadAll(Iterable<? extends K> keys) throws CacheLoaderException {
    List<Get> gets = createGets(keys);
    long start = System.currentTimeMillis();
    Result[] results = get(gets);
    long time = System.currentTimeMillis() - start;
    logger.debug("Got " + gets.size() + " values in " + time + "ms");
    return resultsToMap(Arrays.asList(results));
  }

  /**
   * @param args List of {@link Filter} to use for scan. The filters will be aggregated using {@link
   * FilterList}
   */
  @Override
  public void loadCache(IgniteBiInClosure<K, V> clo, Object... args) throws CacheLoaderException {
    Scan scan = createScan(args);
    Map<K, V> values = scan(scan);
    values.forEach(clo::apply);
  }


  /**
   * {@inheritDoc}
   */
  @Deprecated
  @Override
  public void sessionEnd(boolean commit) {
  /* No-op */
  }

  /**
   * Set {@link Serializer} to serialize and deserialize keys
   *
   * @param keySerializer serializer to use
   */
  public void setKeySerializer(Serializer<K> keySerializer) {
    this.keySerializer = keySerializer;
  }

  /**
   * Set {@link Serializer} to serialize and deserialize values
   *
   * @param valueSerializer serializer to use
   */
  public void setValueSerializer(Serializer<V> valueSerializer) {
    this.valueSerializer = valueSerializer;
  }

  /**
   * <p>Redirects to {@link #writeAll(Collection)} as HBase expects batch operations anyways.</p>
   *
   * {@inheritDoc}
   */
  @Override
  public void write(Entry<? extends K, ? extends V> entry) throws CacheWriterException {
    // HBase expects batches of operations
    writeAll(Collections.singletonList(entry));
  }

  @Override
  public void writeAll(Collection<Entry<? extends K, ? extends V>> entries)
      throws CacheWriterException {
    List<Put> puts = createPuts(entries);
    long start = System.currentTimeMillis();
    put(puts);
    long time = System.currentTimeMillis() - start;
    logger.debug("Put " + puts.size() + " values in " + time + "ms");
  }

  private void delete(List<Delete> deletes) {
    try {
      table().delete(deletes);
    } catch (IOException | IllegalStateException e) {
      throw new CacheWriterException("Failed to delete keys in HBase", e);
    }
  }

  private List<Delete> createDeletes(Collection<?> keys) {
    return keys.stream()
        .map(this::createDelete)
        .collect(Collectors.toList());
  }

  private Delete createDelete(Object key) {
    try {
      @SuppressWarnings("unchecked")
      Delete delete = new Delete(keySerializer.serialize((K) key));
      delete.addColumns(family(), QUALIFIER);
      return delete;
    } catch (ClassCastException | SerializationException e) {
      throw new CacheWriterException("Failed to create delete", e);
    }
  }

  private V extractValue(Result result) {
    byte[] bytes = result.getValue(family(), QUALIFIER);
    return extractValue(bytes);
  }

  private V extractValue(byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    try {
      return valueSerializer.deserialize(bytes);
    } catch (SerializationException e) {
      throw new CacheException("Error deserializing value", e);
    }
  }

  private byte[] family() {
    return session.cacheName().getBytes();
  }

  private Result[] get(List<Get> gets) {
    try {
      return table().get(gets);
    } catch (IOException | IllegalStateException e) {
      throw new CacheLoaderException("Failed to load keys from HBase", e);
    }
  }

  private List<Get> createGets(Iterable<? extends K> keys) {
    return stream(keys)
        .map(this::createGet)
        .collect(Collectors.toList());
  }

  private Get createGet(K key) {
    try {
      Get get = new Get(keySerializer.serialize(key));
      get.addColumn(family(), QUALIFIER);
      return get;
    } catch (SerializationException e) {
      throw new CacheLoaderException("Failed to create get", e);
    }
  }

  private Scan createScan(Object[] args) {
    Scan scan = new Scan();
    scan.addColumn(family(), QUALIFIER);
    Optional<Filter> filter = FilterParser.createFilter(args);
    filter.ifPresent(scan::setFilter);
    return scan;
  }

  private Map<K, V> scan(Scan scan) {
    try (ResultScanner scanner = table().getScanner(scan)) {
      return resultsToMap(scanner);
    } catch (IOException | IllegalStateException e) {
      throw new CacheLoaderException("Failed to load cache from HBase", e);
    }
  }

  private Map<K, V> resultsToMap(Iterable<Result> results) {
    return stream(results)
        .filter(result -> !result.isEmpty())
        .collect(Collectors.toMap(this::extractKey, this::extractValue));
  }

  private K extractKey(Result result) {
    try {
      byte[] row = result.getRow();
      return keySerializer.deserialize(row);
    } catch (SerializationException e) {
      throw new CacheException("Error deserializing key", e);
    }
  }

  private Table table() throws IllegalStateException {
    Table table = session.attachment();
    checkState(table != null, "Table must not be null");
    return table;
  }

  private void put(List<Put> puts) {
    try {
      table().put(puts);
    } catch (IOException | IllegalStateException e) {
      throw new CacheWriterException("Failed to write keys to HBase", e);
    }
  }

  private List<Put> createPuts(Collection<Entry<? extends K, ? extends V>> entries) {
    return entries.stream()
        .map(this::createPut)
        .collect(Collectors.toList());
  }

  private Put createPut(Entry<? extends K, ? extends V> entry) {
    try {
      Put put = new Put(keySerializer.serialize(entry.getKey()));
      put.addColumn(family(), QUALIFIER, valueSerializer.serialize(entry.getValue()));
      return put;
    } catch (SerializationException e) {
      throw new CacheWriterException("Failed to create put", e);
    }
  }

}
