package com.bakdata.commons.serialization;

import java.util.Arrays;
import java.util.Collection;
import org.junit.runners.Parameterized.Parameters;

public class LongSerializerTest extends SerializerTest<Long> {

  @Parameters
  public static Collection<Long> data() {
    return Arrays.asList(0L, 2L, -3L);
  }

  @Override
  protected Serializer<Long> getSerializer() {
    return LongSerializer.INSTANCE;
  }
}
