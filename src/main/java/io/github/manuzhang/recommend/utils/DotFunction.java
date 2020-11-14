package io.github.manuzhang.recommend.utils;

import org.apache.spark.ml.linalg.VectorUtils;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.ml.linalg.Vector;

public class DotFunction implements UDF2<Vector, Vector, Double> {
  @Override
  public Double call(Vector v1, Vector v2) throws Exception {
    if (v1 == null || v2 == null) {
      return -1.0;
    } else {
      return VectorUtils.dot(v1, v2);
    }
  }
}
