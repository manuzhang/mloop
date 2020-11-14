package io.github.manuzhang.recommend.utils;

import org.apache.spark.sql.api.java.UDF5;

public class Encode5 implements UDF5<Integer, Number, Number, Integer, Number, Long> {

  @Override
  public Long call(Integer prefix1, Number value1, Number value2,
      Integer prefix2, Number value3) throws Exception {
    if (null == value1 || null == value2 || null == value3) {
      return null;
    } else {
      return HashEncoder.encode(prefix1,
          HashEncoder.unionAndParse(value1.longValue(), value2.longValue()),
          prefix2, value3.longValue());
    }
  }
}
