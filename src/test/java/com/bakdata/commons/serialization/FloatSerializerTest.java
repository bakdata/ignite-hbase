package com.bakdata.commons.serialization;

import java.util.Arrays;
import java.util.Collection;
import org.junit.runners.Parameterized.Parameters;

public class FloatSerializerTest extends SerializerTest<Float> {

  @Parameters
  public static Collection<Float> data() {
    return Arrays.asList(0.0f, 3.2f, -10.1f);
  }

  @Override
  protected Serializer<Float> getSerializer() {
    return FloatSerializer.INSTANCE;
  }
}
