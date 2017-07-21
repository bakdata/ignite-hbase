package com.bakdata.ignite.hbase;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Optional;
import javax.cache.integration.CacheLoaderException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;

final class FilterParser {

  //utility class
  private FilterParser() {
  }

  static Optional<Filter> createFilter(Object[] args) {
    if (args.length == 0) {
      return Optional.empty();
    }
    FilterList filters = new FilterList();
    for (int i = 0; i < args.length; i++) {
      Object filter = args[i];
      try {
        checkArgument(filter instanceof Filter,
            "Filter " + i + " must be of type " + Filter.class.getName()
                + " but is of type " + filter.getClass().getName());
      } catch (IllegalArgumentException e) {
        throw new CacheLoaderException(e);
      }
      filters.addFilter((Filter) filter);
    }
    return Optional.of(filters);
  }
}
