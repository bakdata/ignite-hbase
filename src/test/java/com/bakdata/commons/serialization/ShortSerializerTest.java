package com.bakdata.commons.serialization;

import java.util.Arrays;
import java.util.Collection;
import org.junit.runners.Parameterized.Parameters;

public class ShortSerializerTest extends SerializerTest<Short> {

  @Parameters
  public static Collection<Short> data() {
    return Arrays.asList((short) 0, (short) 2, (short) -3);
  }

  @Override
  protected Serializer<Short> getSerializer() {
    return ShortSerializer.INSTANCE;
  }
}
