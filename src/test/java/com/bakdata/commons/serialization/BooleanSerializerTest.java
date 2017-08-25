package com.bakdata.commons.serialization;

import java.util.Arrays;
import java.util.Collection;
import org.junit.runners.Parameterized.Parameters;

public class BooleanSerializerTest extends SerializerTest<Boolean> {

  @Parameters
  public static Collection<Boolean> data() {
    return Arrays.asList(true, false);
  }

  @Override
  protected Serializer<Boolean> getSerializer() {
    return BooleanSerializer.INSTANCE;
  }
}
