package com.bakdata.commons.serialization;

import java.util.Arrays;
import java.util.Collection;
import org.junit.runners.Parameterized.Parameters;

public class IntSerializerTest extends SerializerTest<Integer> {

  @Parameters
  public static Collection<Integer> data() {
    return Arrays.asList(0, 2, -3);
  }

  @Override
  protected Serializer<Integer> getSerializer() {
    return IntSerializer.INSTANCE;
  }
}
