package com.bakdata.commons.serialization;

import java.util.Arrays;
import java.util.Collection;
import org.junit.runners.Parameterized.Parameters;

public class ObjectSerializerTest extends SerializerTest<Object> {

  @Parameters
  public static Collection<Object> data() {
    return Arrays.asList("foo", "bar baz", "");
  }

  @Override
  protected Serializer<Object> getSerializer() {
    return ObjectSerializer.INSTANCE;
  }
}
