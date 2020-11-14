package org.apache.spark.ml.bundle.ops.feature

import ml.bundle.Socket
import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.{Model, NodeShape, Value}
import ml.combust.bundle.op.OpModel
import org.apache.spark.ml.bundle._
import org.apache.spark.ml.feature.OneHotEncoderModel

class OneHotEncoderOp23 extends SimpleSparkOp[OneHotEncoderModel] {
  override def sparkInputs(obj: OneHotEncoderModel): Seq[ParamSpec] = {
    Seq.empty
  }

  override def sparkOutputs(obj: OneHotEncoderModel): Seq[SimpleParamSpec] = {
    Seq.empty
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: OneHotEncoderModel): OneHotEncoderModel = {
    new OneHotEncoderModel(uid, model.categorySizes)
  }

  override val Model: OpModel[SparkBundleContext, OneHotEncoderModel] = new OpModel[SparkBundleContext, OneHotEncoderModel] {
    override val klazz: Class[OneHotEncoderModel] = classOf[OneHotEncoderModel]

    override def opName: String = "one_hot_encoder_23"

    override def store(model: Model, obj: OneHotEncoderModel)(implicit context: BundleContext[SparkBundleContext]): Model = {
      assert(obj.categorySizes.length == 1)

      model.withValue("categorySize", Value.int(obj.categorySizes.head))
        .withValue("droplast", Value.boolean(obj.getDropLast))
        .withValue("handleinvalid", Value.string(obj.getHandleInvalid))
    }

    override def load(model: Model)(implicit context: BundleContext[SparkBundleContext]): OneHotEncoderModel = {
      new OneHotEncoderModel("", Array(model.value("categorySizes").getInt))
        .setDropLast(model.value("droplast").getBoolean)
        .setHandleInvalid(model.value("handleinvalid").getString)
    }
  }

  override def shape(node: OneHotEncoderModel)
                    (implicit context: BundleContext[SparkBundleContext]): NodeShape = {
    assert(node.getInputCols.length == 1)

    val dataset = context.context.dataset.getOrElse {
      throw new IllegalArgumentException(
        """
          |Must provide a transformed data frame to MLeap for serializing a pipeline.
          |The transformed data frame is used to extract data types and other metadata
          |required for execution.
          |
          |Example usage:
          |```
          |val sparkTransformer: org.apache.spark.ml.Transformer
          |val transformedDataset = sparkTransformer.transform(trainingDataset)
          |
          |implicit val sbc = SparkBundleContext().withDataset(transformedDataset)
          |
          |for(bf <- managed(BundleFile(file))) {
          |  sparkTransformer.writeBundle.format(SerializationFormat.Json).save(bf).get
          |}
          |```
        """.stripMargin)
    }

    val is = Seq(Socket("input", node.getInputCols.head))
    val os = Seq(Socket("output", node.getOutputCols.head))

    NodeShape(inputs = is, outputs = os)
  }
}
