package com.bakdata.commons.serialization;

import java.util.Arrays;
import java.util.Collection;
import org.junit.runners.Parameterized.Parameters;

public class DoubleSerializerTest extends SerializerTest<Double> {

  @Parameters
  public static Collection<Double> data() {
    return Arrays.asList(0.0, 3.2, -10.1);
  }

  @Override
  protected Serializer<Double> getSerializer() {
    return DoubleSerializer.INSTANCE;
  }
}
