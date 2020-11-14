package io.github.manuzhang.recommend.utils;

import org.apache.spark.sql.api.java.UDF4;

public class Encode4 implements UDF4<Integer, Number, Integer, Number, Long> {
  @Override
  public Long call(Integer prefix1, Number value1,
      Integer prefix2, Number value2) throws Exception {
    return HashEncoder.encode(prefix1, value1.longValue(), prefix2, value2.longValue());
  }
}
