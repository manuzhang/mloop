package org.apache.spark.ml.bundle.ops.feature

import ml.bundle.{DataShape, Socket}
import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.feature.FillnaTransformer
import org.apache.spark.sql.mleap.TypeConverters.{sparkFieldToMleapField, sparkToMleapDataShape}

class FillNaOp extends SimpleSparkOp[FillnaTransformer] {
  override def sparkInputs(obj: FillnaTransformer): Seq[ParamSpec] = {
    Seq.empty
  }

  override def sparkOutputs(obj: FillnaTransformer): Seq[SimpleParamSpec] = {
    Seq.empty
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: FillnaTransformer): FillnaTransformer = {
    new FillnaTransformer(model.columnAndValues, uid)
  }

  override val Model: OpModel[SparkBundleContext, FillnaTransformer] = new OpModel[SparkBundleContext, FillnaTransformer] {
    override val klazz: Class[FillnaTransformer] = classOf[FillnaTransformer]

    override def opName: String = "fillna_transformer"

    override def store(model: Model, obj: FillnaTransformer)(implicit context: BundleContext[SparkBundleContext]): Model = {
      import ml.combust.mleap.runtime.types.BundleTypeConverters._

      val dataset = context.context.dataset.get
      val cols = obj.columnAndValues.keys.toArray
      val values = obj.columnAndValues.values.toArray
      val inputShapes = cols.map(i ⇒ sparkToMleapDataShape(dataset.schema(i), dataset): DataShape)
      val basicTypes = cols.map(i ⇒ sparkFieldToMleapField(dataset, dataset.schema(i)).dataType.base).map(mleapToBundleBasicType)

      model.withValue("keys", Value.stringList(cols))
        .withValue("values", Value.stringList(values))
        .withValue("input_shapes", Value.dataShapeList(inputShapes))
        .withValue("basic_types", Value.basicTypeList(basicTypes))
    }

    override def load(model: Model)(implicit context: BundleContext[SparkBundleContext]): FillnaTransformer = {
      val keys = model.value("keys").getStringList
      val values = model.value("values").getStringList
      new FillnaTransformer(keys.zip(values).toMap, "")
    }
  }

  override def shape(node: FillnaTransformer)
                    (implicit context: BundleContext[SparkBundleContext]): NodeShape = {
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

    val is = node.columnAndValues.keySet.zipWithIndex.map { case (col, index) =>
      Socket(s"input$index", col)
    }.toSeq
    val os = node.columnAndValues.keySet.zipWithIndex.map { case (col, index) =>
      Socket(s"output$index", col)
    }.toSeq

    NodeShape(inputs = is, outputs = os)
  }
}
