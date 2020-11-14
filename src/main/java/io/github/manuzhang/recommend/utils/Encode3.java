package io.github.manuzhang.recommend.utils;

import org.apache.spark.sql.api.java.UDF3;

public class Encode3 implements UDF3<Integer, Number, Number, Long> {
  @Override
  public Long call(Integer prefix, Number value1, Number value2) throws Exception {
    if (null == value1  || null == value2) {
      return null;
    } else {
      return HashEncoder.encode(prefix,
          HashEncoder.unionAndParse(value1.longValue(), value2.longValue()));
    }
  }
}
