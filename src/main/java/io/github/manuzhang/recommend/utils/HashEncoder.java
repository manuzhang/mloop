package io.github.manuzhang.recommend.utils;

public class HashEncoder {
  private static int seed = 0x3c074a61;

  public static Long encode(int prefix, Long value) {
    if (value == null) {
      return null;
    } else {
      byte[] bytes = new byte[12];
      for (int i = 0; i < 4; i++) {
        bytes[i] = (byte) (prefix >>> (i * 8));
      }
      for (int i = 0; i < 8; i++) {
        bytes[4 + i] = (byte) (value >>> (i * 8));
      }
      return positiveMod(MurmurHash3.murmurhash3_x64_64(bytes, 12, seed), 1L << 60);
    }
  }

  public static Long encode(int prefix1, Long value1, int prefix2, Long value2) {
    if (value1 == null || value2 == null) {
      return null;
    } else {
      byte[] bytes = new byte[24];
      for (int i = 0; i < 4; i++) {
        bytes[i] = (byte) (prefix1 >>> (i * 8));
      }
      for (int i = 0; i < 8; i++) {
        bytes[4 + i] = (byte) (value1 >>> (i * 8));
      }
      for (int i = 0; i < 4; i++) {
        bytes[12 + i] = (byte) (prefix2 >>> (i * 8));
      }
      for (int i = 0; i < 8; i++) {
        bytes[16 + i] = (byte) (value2 >>> (i * 8));
      }
      return positiveMod(MurmurHash3.murmurhash3_x64_64(bytes, 24, seed), 1L << 60);
    }
  }

  public static long unionAndParse(long l1, long l2) {
    return Long.parseLong(String.format("%d%d", l1, l2));
  }

  private static long positiveMod(long div, long mod) {
    return ((div % mod) + mod) % mod;
  }
}
