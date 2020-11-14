package io.github.manuzhang.recommend.utils;

import org.apache.spark.sql.api.java.UDF2;

public class PiecewiseSimilar implements UDF2<String, String, Long> {
  @Override
  public Long call(String value1, String value2) throws Exception {
    if (null == value1 || null == value2) {
      return null;
    } else {
      return (long) getSimilar(value1, value2);
    }
  }

  private int getSimilar(String v1, String v2)  {
    double similar = Utils.cosine(v1, v2);
/*    if (similar < -0.175)
      return 1;
    else if (similar < -0.08)
      return 2;
    else if (similar <-0.021)
      return 3;
    else if (similar < 0.03)
      return 4;
    else if (similar < 0.075)
      return 5;
    else if (similar < 0.12)
      return 6;
    else if (similar < 0.175)
      return 7;
    else if (similar < 0.248)
      return 8;
    else
      return 9;*/
    if (similar < -1.0)
      return 1;
    else if (similar < -0.377943)
      return 2;
    else if (similar < -0.230679)
      return 3;
    else if (similar < -0.18722)
      return 4;
    else if (similar < -0.158234)
      return 5;
    else if (similar < -0.135861)
      return 6;
    else if (similar < -0.11715)
      return 7;
    else if (similar < -0.100871)
      return 8;
    else if (similar < -0.0863382)
      return 9;
    else if (similar < -0.07305129)
      return 10;
    else if (similar < -0.0607377)
      return 11;
    else if (similar < -0.049179702)
      return 12;
    else if (similar < -0.038246108)
      return 13;
    else if (similar < -0.0278298)
      return 14;
    else if (similar < -0.0178302)
      return 15;
    else if (similar < -0.00817345)
      return 16;
    else if (similar < 0.00051)
      return 17;
    else if (similar < 0.00970741)
      return 18;
    else if (similar < 0.0186835)
      return 19;
    else if (similar < 0.0275652)
      return 20;
    else if (similar < 0.0363054)
      return 21;
    else if (similar < 0.0449607)
      return 22;
    else if (similar < 0.0535734)
      return 23;
    else if (similar < 0.0621456)
      return 24;
    else if (similar < 0.07072152)
      return 25;
    else if (similar < 0.0793494)
      return 26;
    else if (similar < 0.0880595)
      return 27;
    else if (similar < 0.0968779)
      return 28;
    else if (similar < 0.105835)
      return 29;
    else if (similar < 0.114963)
      return 30;
    else if (similar < 0.124312)
      return 31;
    else if (similar < 0.133941)
      return 32;
    else if (similar < 0.143869)
      return 33;
    else if (similar < 0.154199)
      return 34;
    else if (similar < 0.165004)
      return 35;
    else if (similar < 0.176447)
      return 36;
    else if (similar < 0.188607)
      return 37;
    else if (similar < 0.201776)
      return 38;
    else if (similar < 0.216213)
      return 39;
    else if (similar < 0.232308)
      return 40;
    else if (similar < 0.250823)
      return 41;
    else if (similar < 0.273066)
      return 42;
    else if (similar < 0.301835)
      return 43;
    else if (similar < 0.345467)
      return 44;
    else
      return 45;
  }
}
