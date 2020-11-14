package io.github.manuzhang.mlp.dag.utils

import com.google.protobuf.ByteString
import com.vip.mlp.pb.SoRtrsBrandInfo.BrandInfo.BrandGrade
import com.vip.mlp.pb.SoRtrsBrandInfo.{BrandInfo, GoodsInfo}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}
import org.mockito.Mockito._

import scala.collection.JavaConverters._

class TairParserTest extends FlatSpec with Matchers with MockitoSugar {

  "TairParser" should "parse type from proto class name" in {
    val parser = new TairParser

    val expected = Array(StructField("is_prepay", IntegerType, nullable = true),
      StructField("goods_name", StringType, nullable = true))
    parser.parseSchema("com.vip.mlp.pb.GoodsInfo",
      Array("is_prepay", "goods_name")) shouldEqual expected
  }

  "TairParser" should "parse pb result on one key request" in {
    val parser = new TairParser

    val tair = mock[SimpleTairClient]
    val area: Short = 2
    val key1 = "rtrs_goods_info_1"
    val key2 = "rtrs_goods_info_2"
    val key3 = "rtrs_goods_info_3"
    val key4 = "rtrs_goods_info_4"
    val keys = Array(key1, key2, key3, key4)

    val field1 = StructField("goods_name", StringType, nullable = true)
    val field2 = StructField("is_prepay", IntegerType, nullable = true)
    val field3 = StructField("tag_code_list", ArrayType(IntegerType), nullable = true)
    val fields = Array(field1.name, field2.name, field3.name)
    val pb = "com.vip.mlp.pb.GoodsInfo"

    val gname1 = "goods1"
    val prepay1 = 0
    val tagList1 = Vector(new Integer(0))
    val goods1 = GoodsInfo.newBuilder()
      .setGoodsName(ByteString.copyFromUtf8(gname1))
      .setIsPrepay(prepay1)
      .addAllTagCodeList(tagList1.asJava)
      .build()

    val gname2 = "goods2"
    val prepay2 = 1
    val tagList2 = Vector(new Integer(0), new Integer(1))
    val goods2 = GoodsInfo.newBuilder()
      .setGoodsName(ByteString.copyFromUtf8(gname2))
      .setIsPrepay(prepay2)
      .addAllTagCodeList(tagList2.asJava)
      .build()

    val gname3 = "goods3"
    val goods3 = GoodsInfo.newBuilder()
      .setGoodsName(ByteString.copyFromUtf8(gname3))
      .build()

    when(tair.getValue(area, key1)).thenReturn(goods1.toByteArray)
    when(tair.getValue(area, key2)).thenReturn(goods2.toByteArray)
    when(tair.getValue(area, key3)).thenReturn(goods3.toByteArray)
    when(tair.getValue(area, key4)).thenReturn(null)


    val (schema, data) = parser.parseOneKey(tair, area, keys, fields, pb)

    schema shouldEqual StructType(Array(StructField("key", StringType, nullable = false),
      field1, field2, field3))

    data shouldEqual List(
      Row(key1, gname1, prepay1, tagList1),
      Row(key2, gname2, prepay2, tagList2),
      Row(key3, gname3, null, null),
      Row(key4, null, null, null)
    )
  }

  "TairParser" should "parse nested fields" in {
    val parser = new TairParser
    val pb = "com.vip.mlp.pb.BrandInfo"

    val grade1 = BrandGrade.newBuilder()
      .setBrandSn(1)
      .build()
    val grade2 = BrandGrade.newBuilder()
      .setBrandSn(2)
      .build()
    val brand1 = BrandInfo.newBuilder()
      .setBrandId(1)
      .addBrandGrade(grade1)
      .addBrandGrade(grade2)
      .build()

    parser.parseValue(pb, Array("brand_id", "brand_grade.brand_sn"), brand1.toByteArray) shouldEqual
      Array(1, Array(1, 2))

    parser.parseSchema(pb, Array("brand_id", "brand_grade.brand_sn")) shouldEqual
      Array(
        StructField("brand_id", IntegerType, nullable = true),
        StructField("brand_grade.brand_sn", ArrayType(IntegerType), nullable = true)
      )
  }

}
