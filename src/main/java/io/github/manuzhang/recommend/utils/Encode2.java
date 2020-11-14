package io.github.manuzhang.recommend.utils;

import org.apache.spark.sql.api.java.UDF2;

public class Encode2 implements UDF2<Integer, Number, Long> {

  @Override
  public Long call(Integer prefix, Number value) throws Exception {
    return HashEncoder.encode(prefix, value.longValue());
  }
}
