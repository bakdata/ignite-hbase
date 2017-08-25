package com.bakdata.commons.serialization;

import java.util.Arrays;
import java.util.Collection;
import org.junit.runners.Parameterized.Parameters;

public class ByteSerializerTest extends SerializerTest<Byte> {

  @Parameters
  public static Collection<Byte> data() {
    return Arrays.asList((byte) 0, (byte) 2, (byte) -3);
  }

  @Override
  protected Serializer<Byte> getSerializer() {
    return ByteSerializer.INSTANCE;
  }
}
