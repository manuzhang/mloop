package io.github.manuzhang.recommend.utils;

public class Utils {

  public static double cosine(Double[] lhs, Double[] rhs) {
    if (lhs.length != rhs.length) {
      return -1.0;
    }
    double result = 0.0;
    for (int i = 0; i < lhs.length; i++) {
      result += lhs[i] * rhs[i];
    }
    return result;
  }

  public static double cosine(String[] lhs, String[] rhs) {
    if (lhs.length != rhs.length) {
      return -1.0;
    }
    double result = 0.0;
    for (int i = 0; i < lhs.length; i++) {
      result += Double.parseDouble(lhs[i]) * Double.parseDouble(rhs[i]);
    }
    return result;
  }

  public static Double cosine(String value1, String value2) {
    String[] vector1 = value1.split(",");
    String[] vector2 = value2.split(",");
    return cosine(vector1, vector2);
  }
}
