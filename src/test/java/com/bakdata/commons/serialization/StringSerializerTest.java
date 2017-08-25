package com.bakdata.commons.serialization;

import java.util.Arrays;
import java.util.Collection;
import org.junit.runners.Parameterized.Parameters;

public class StringSerializerTest extends SerializerTest<String> {

  @Parameters
  public static Collection<String> data() {
    return Arrays.asList("foo", "bar baz", "");
  }

  @Override
  protected Serializer<String> getSerializer() {
    return StringSerializer.INSTANCE;
  }
}
