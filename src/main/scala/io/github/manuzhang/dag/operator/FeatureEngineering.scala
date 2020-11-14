package io.github.manuzhang.dag.operator

import io.github.manuzhang.dag.model.ParamsParser
import io.github.manuzhang.dag.utils.DataFrameUtils
import io.github.manuzhang.dag.utils.ParseUtils._
import io.github.manuzhang.dag.{Node, ParseContext, PipelineStageDescriptions, TransformerInfo}
import io.github.manuzhang.mlp.dag.model.ParamsAnnotation
import org.apache.spark.ml.feature._
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.{Estimator, PipelineStage, SparkMlUtils, Transformer}
import org.apache.spark.sql.types.StructType

import scala.util.{Success, Try}

trait FeatureEngineering[T <: PipelineStage] extends Transform {

  def getStageId(node: Node, inputCols: Array[String]): String = {
    node.id + inputCols.mkString(",")
  }

  /**
   * @return an empty DataFrame with the schema that is transformed from an input DataFrame
   */
  override def inferSchema(node: Node, context: ParseContext): StructType = {
    parseInternal(node, context)(transformSchema).schema
  }

  override def parse(node: Node, context: ParseContext): DataFrame = {
    parseInternal(node, context)(transformDf)
  }

  protected def transformSchema(node: Node, context: ParseContext, stage: T, df: DataFrame): DataFrame = {
    DataFrameUtils.createEmptyDF(context.sparkSession, stage.transformSchema(df.schema))
  }

  protected def transformDf(node: Node, context: ParseContext, stage: T, df: DataFrame): DataFrame = {
    val transformer: Transformer = stage match {
      case estimator: Estimator[_] =>
        estimator.fit(df).asInstanceOf[Transformer]
      case tf: Transformer =>
        tf
    }
    val inputs = SparkMlUtils.getInputCols(transformer)
    context.registerTransformerInfo(node, TransformerInfo(transformer, df, getStageId(node, inputs)))
    transformer.transform(df)
  }

  protected def parseInternal(node: Node, context: ParseContext)
    (transform: (Node, ParseContext, T, DataFrame) => DataFrame): DataFrame

  protected def getInputOutputCols(node: Node, keyword: String,
      postfix: String = "_TMP"): Map[String, String] = {
    val columns = parseArray(node, keyword)
    columns.map(col => (col, col + postfix)).toMap
  }

  override def getPipelineStageDescriptions(node: Node, context: ParseContext): List[PipelineStageDescriptions] = {
    val descriptions = scala.collection.mutable.ArrayBuffer.empty[PipelineStageDescriptions]

    def getDescriptions(node: Node, context: ParseContext, stage: T, df: DataFrame): DataFrame = {
      val inputs = SparkMlUtils.getInputCols(stage)
      descriptions += PipelineStageDescriptions(node.name, getStageId(node, inputs), inputs.mkString(","))
      DataFrameUtils.createEmptyDF(context.sparkSession, stage.transformSchema(df.schema))
    }

    parseInternal(node, context)(getDescriptions)
    context.getUpstreamStageDescs(node, DEFAULT_INPORT) ++ descriptions
  }
}

/**
  * A label indexer that maps a string column of labels to an ML column of label indices. If the input column
  * is numeric, we cast it to string and index the string values. The indices are in [0, numLabels), ordered
  * by label frequencies. So the most frequent label gets index 0.
  * params:
  *   columns
  */
class MLStringIndexer extends FeatureEngineering[StringIndexer] {
  val HANDLE_INVALID_KEYWORD = "handle_invalid"

  override protected def parseInternal(node: Node, context: ParseContext)
    (transform: (Node, ParseContext, StringIndexer, DataFrame) => DataFrame): DataFrame = {
    val inputDf = context.getInputDf(node, DEFAULT_INPORT)

    val handleInvalid = node.getParamValue(HANDLE_INVALID_KEYWORD)

    val cols = getInputOutputCols(node, DEFAULT_COLUMNS, "_IDX")
    cols.foldLeft(inputDf) {
      case (df, (inCol, outCol)) =>
        val indexer = new StringIndexer()
          .setInputCol(inCol)
          .setOutputCol(outCol)
          .setHandleInvalid(handleInvalid)

        transform(node, context, indexer, df)
    }
  }
}

/**
  * A feature transformer that merges multiple columns into a vector column.
  * params:
  *   from, to, keep_columns, output_column
  */
class MLVectorAssembler extends FeatureEngineering[VectorAssembler] {
  val OUTCOL_KEYWORD = "output_column"

  override protected def parseInternal(node: Node, context: ParseContext)
    (transform: (Node, ParseContext, VectorAssembler, DataFrame) => DataFrame): DataFrame = {
    val inputDf = context.getInputDf(node, DEFAULT_INPORT)

    val keepCols = parseBoolean(node, "keep_columns")
    val outColName = node.getParamValue(OUTCOL_KEYWORD)
    val cols = getInputOutputCols(node, DEFAULT_COLUMNS, "_A")
    val inputCols = cols.keys.toArray

    val assembler = new VectorAssembler()
      .setInputCols(inputCols)
      .setOutputCol(outColName)

    val outputDf = transform(node, context, assembler, inputDf)
    if (keepCols)  {
      outputDf
    } else {
      outputDf.drop(inputCols: _*)
    }
  }
}

/**
  * Binarize a column of continuous features given a threshold.
  */
class MLBinarizer extends FeatureEngineering[Binarizer] {

  override protected def parseInternal(node: Node, context: ParseContext)
    (transform: (Node, ParseContext, Binarizer, DataFrame) => DataFrame): DataFrame = {
    val inputDF = context.getInputDf(node, DEFAULT_INPORT)
    val columnsAndThresholds = parseMap(node, "columns")
    columnsAndThresholds.foldLeft(inputDF) {
      case (df, kv) =>
        val (column, threshold) = kv
        val binarizer = new Binarizer()
          .setInputCol(column)
          .setOutputCol(column + "_B")
          .setThreshold(threshold.toDouble)

        transform(node, context, binarizer, df)
    }
  }
}

/**
  * Support L1/L2/L`^`Inf normalizations.
  */
@deprecated("use NormalizeOp instead")
class MLNormalizer extends FeatureEngineering[Normalizer] {

  override protected def parseInternal(node: Node, context: ParseContext)
    (transform: (Node, ParseContext, Normalizer, DataFrame) => DataFrame): DataFrame = {
    val pNormOption = node.getParamValue("pNorm")
    val pNorm = Try(pNormOption.toDouble) match {
      case Success(result) => result
      case _ => Double.PositiveInfinity
    }

    val inputDf = context.getInputDf(node, DEFAULT_INPORT)
    val normalizedColumns = parseArray(node, DEFAULT_COLUMNS)
    val outputCol = normalizedColumns.mkString("_") + "_TMP"
    val assembler = new VectorAssembler().setInputCols(normalizedColumns).setOutputCol(outputCol)

    val vectorizedDF = assembler.transform(inputDf)
    val normalizer = new Normalizer()
      .setP(pNorm)
      .setInputCol(assembler.getOutputCol)
      .setOutputCol(assembler.getOutputCol + "_N")

    transform(node, context, normalizer, vectorizedDF)
  }
}

/**
  * alternative for MLNormalizer.
  */
class NormalizeOp extends FeatureEngineering[Normalizer] {

  override protected def parseInternal(node: Node, context: ParseContext)
                                      (transform: (Node, ParseContext, Normalizer, DataFrame) => DataFrame): DataFrame = {
    val pNormOption = node.getParamValue("pNorm")
    val pNorm = Try(pNormOption.toDouble) match {
      case Success(result) => result
      case _ => Double.PositiveInfinity
    }
    val outputCol = node.getParamValue("output_column")
    val inputDf = context.getInputDf(node, DEFAULT_INPORT)
    val normalizedColumn = parseHead(node, DEFAULT_COLUMNS)
    val normalizer = new Normalizer()
      .setP(pNorm)
      .setInputCol(normalizedColumn)
      .setOutputCol(outputCol)

    transform(node, context, normalizer, inputDf)
  }
}

/**
  * Maps a column of continuous features to a column of feature buckets.
  */
class MLBucketizer extends FeatureEngineering[Bucketizer] {
  val BUCKETIZER_KEYWORD = "buckets"
  val HANDLEINVALID_KEYWORD = "handle_invalid"

  override protected def parseInternal(node: Node, context: ParseContext)
    (transform: (Node, ParseContext, Bucketizer, DataFrame) => DataFrame): DataFrame = {
    val inputDf = context.getInputDf(node, DEFAULT_INPORT)
    val bucketColumns = parseArray(node, DEFAULT_COLUMNS)
    val buckets = parseCommaSepList(node, BUCKETIZER_KEYWORD).map {
      case "-INF" => Double.NegativeInfinity
      case "+INF" => Double.PositiveInfinity
      case v => v.toDouble
    }
    val handleInvalid = node.getParamValue(HANDLEINVALID_KEYWORD)

    bucketColumns.foldLeft(inputDf) {
      case (df, inCol) =>
        val bucketizer = new Bucketizer()
          .setInputCol(inCol)
          .setOutputCol(inCol + "_B")
          .setSplits(buckets)
          .setHandleInvalid(handleInvalid)

        transform(node, context, bucketizer, df)
    }
  }
}

/**
  * Imputation estimator for completing missing values, either using the mean or the median of the columns
  * in which the missing values are located. The input columns should be of DoubleType or FloatType.
  * Currently Imputer does not support categorical features and possibly creates incorrect values for a
  * categorical feature.
  */
class MLImputer extends FeatureEngineering[Imputer] {
  val STRATEGY_KEYWORD = "strategy"
  val INVALIDVALUE_KEYWORD = "invalid_keyword"

  override protected def parseInternal(node: Node, context: ParseContext)
    (transform: (Node, ParseContext, Imputer, DataFrame) => DataFrame): DataFrame = {
    val strategy = node.getParamValue(STRATEGY_KEYWORD) match {
      case "median" => "median"
      case _ => "mean"
    }

    val inputDf = context.getInputDf(node, DEFAULT_INPORT)
    val imputerColumns = parseArray(node, DEFAULT_COLUMNS)
    val imputer = new Imputer()
      .setInputCols(imputerColumns)
      .setOutputCols(imputerColumns.map(_ + "_I"))
      .setStrategy(strategy)

    transform(node, context, imputer, inputDf)
  }

}

/**
  * Transform multi columns to on one column.
  */
@deprecated("use MaxAbsScaleOp instead")
class MLMaxAbsScaler extends FeatureEngineering[MaxAbsScaler] {

  override protected def parseInternal(node: Node, context: ParseContext)
    (transform: (Node, ParseContext, MaxAbsScaler, DataFrame) => DataFrame): DataFrame = {
    val inputDf = context.getInputDf(node, DEFAULT_INPORT)
    val scaleColumns = parseArray(node, DEFAULT_COLUMNS)
    val outputCol =  scaleColumns.mkString("_") + "_TMP"
    val assembler = new VectorAssembler().setInputCols(scaleColumns).setOutputCol(outputCol)

    val vectorizedDF = assembler.transform(inputDf)
    val scaler = new MaxAbsScaler()
      .setInputCol(assembler.getOutputCol)
      .setOutputCol(assembler.getOutputCol + "_MAS")


    transform(node, context, scaler, vectorizedDF).drop(outputCol)
  }

}

/**
  * alternative for MLMaxAbsScaler.
  */
class MaxAbsScaleOp extends FeatureEngineering[MaxAbsScaler] {

  override protected def parseInternal(node: Node, context: ParseContext)
                                      (transform: (Node, ParseContext, MaxAbsScaler, DataFrame) => DataFrame): DataFrame = {
    val outputCol = node.getParamValue("output_column")
    val inputDf = context.getInputDf(node, DEFAULT_INPORT)
    val scaleColumn = parseHead(node, DEFAULT_COLUMNS)
    val scaler = new MaxAbsScaler()
      .setInputCol(scaleColumn)
      .setOutputCol(outputCol)


    transform(node, context, scaler, inputDf)
  }

}

/**
  * Rescale each feature individually to a common range [min, max] linearly using column summary statistics,
  * which is also known as min-max normalization or Rescaling
  */
@deprecated("use MinMaxScaleOp instead!")
class MLMinMaxScaler extends FeatureEngineering[MinMaxScaler] {

  override protected def parseInternal(node: Node, context: ParseContext)
    (transform: (Node, ParseContext, MinMaxScaler, DataFrame) => DataFrame): DataFrame = {
    val min = parseDouble(node, "min")
    val max = parseDouble(node, "max")

    val inputDf = context.getInputDf(node, DEFAULT_INPORT)
    val scaleColumns = parseArray(node, DEFAULT_COLUMNS)
    val tmpCol = scaleColumns.mkString("_") + "_TMP"
    val assembler = new VectorAssembler().setInputCols(scaleColumns).setOutputCol(tmpCol)

    val vectorizedDF = assembler.transform(inputDf)
    val scaler = new MinMaxScaler()
      .setMin(min)
      .setMax(max)
      .setInputCol(assembler.getOutputCol)
      .setOutputCol(assembler.getOutputCol + "_MMS")

    transform(node, context, scaler, vectorizedDF).drop(tmpCol)
  }

}

/**
  *alternative for MLMinMaxScaler.
  */
class MinMaxScaleOp extends FeatureEngineering[MinMaxScaler] {

  override protected def parseInternal(node: Node, context: ParseContext)
                                      (transform: (Node, ParseContext, MinMaxScaler, DataFrame) => DataFrame): DataFrame = {
    val min = parseDouble(node, "min")
    val max = parseDouble(node, "max")
    val outputCol = node.getParamValue("output_column")
    val inputDf = context.getInputDf(node, DEFAULT_INPORT)
    val scaleColumn = parseHead(node, DEFAULT_COLUMNS)
    val scaler = new MinMaxScaler()
      .setMin(min)
      .setMax(max)
      .setInputCol(scaleColumn)
      .setOutputCol(outputCol)

    transform(node, context, scaler, inputDf)
  }

}

/**
  * PCA trains a model to project vectors to a lower dimensional space of the top k principal components.
  */
@deprecated("use PCAOp instead!")
class MLPCA extends FeatureEngineering[PCA] {

  override protected def parseInternal(node: Node, context: ParseContext)
    (transform: (Node, ParseContext, PCA, DataFrame) => DataFrame): DataFrame = {
    val k = parseInt(node, "k")
    val inputDf = context.getInputDf(node, DEFAULT_INPORT)
    val pcaColumns = parseArray(node, DEFAULT_COLUMNS)
    val pcaTmp = pcaColumns.mkString("_") + "_TMP"
    val pcaOutput = pcaColumns.mkString("_") + "_P"
    val assembler = new VectorAssembler().setInputCols(pcaColumns).setOutputCol(pcaTmp)
    val vectorizedDF = assembler.transform(inputDf)
    val pca = new PCA()
      .setK(k)
      .setInputCol(pcaTmp)
      .setOutputCol(pcaOutput)

    transform(node, context, pca, vectorizedDF).drop(pcaTmp)
  }

}

/**
  * alternative for MLPCA.
  */
class PCAOp extends FeatureEngineering[PCA] {

  override protected def parseInternal(node: Node, context: ParseContext)
                                      (transform: (Node, ParseContext, PCA, DataFrame) => DataFrame): DataFrame = {
    val k = parseInt(node, "k")
    val outputCol = node.getParamValue("output_column")
    val inputDf = context.getInputDf(node, DEFAULT_INPORT)
    val pcaColumn = parseHead(node, DEFAULT_COLUMNS)
    val pca = new PCA()
      .setK(k)
      .setInputCol(pcaColumn)
      .setOutputCol(outputCol)

    transform(node, context, pca, inputDf)
  }

}

/**
  * A tokenizer that converts the input string to lowercase and then splits it by white spaces.
  */
class MLTokenizer extends FeatureEngineering[Tokenizer] {
  override protected def parseInternal(node: Node, context: ParseContext)(
    transform: (Node, ParseContext, Tokenizer, DataFrame) => DataFrame): DataFrame = {
    val inputDF = context.getInputDf(node, DEFAULT_INPORT)

    val cols = getInputOutputCols(node, DEFAULT_COLUMNS, "__output")
    cols.foldLeft(inputDF) {
      case (df, (inCol, outCol)) =>
        val indexer = new Tokenizer()
          .setInputCol(inCol)
          .setOutputCol(outCol)
        transform(node, context, indexer, df)
    }
  }
}

/**
  * Maps a sequence of terms to their term frequencies using the hashing trick.
  * Currently we use Austin Appleby's MurmurHash 3 algorithm (MurmurHash3_x86_32)
  * to calculate the hash code value for the term object.
  * Since a simple modulo is used to transform the hash function to a column index,
  * it is advisable to use a power of two as the numFeatures parameter;
  * otherwise the features will not be mapped evenly to the columns.
  */
class MLHashingTF extends FeatureEngineering[HashingTF] with ParamsParser {
  override protected def parseInternal(node: Node, context: ParseContext) (
    transform: (Node, ParseContext, HashingTF, DataFrame) => DataFrame): DataFrame = {
    val inputDF = context.getInputDf(node, DEFAULT_INPORT)
    val inputCol = parseHead(node, "inputCol")
    val indexer = initEstimatorParams(new HashingTF(), node).setInputCol(inputCol)
    transform(node, context, indexer, inputDF)
  }
}

/**
  * Word2Vec trains a model of `Map(String, Vector)`, i.e. transforms a word into a code for further
  * natural language processing or machine learning process.
  */
class MLWord2Vec extends FeatureEngineering[Word2Vec] with ParamsParser {
  override protected def parseInternal(node: Node, context: ParseContext)(
    transform: (Node, ParseContext, Word2Vec, DataFrame) => DataFrame): DataFrame = {
    val inputDF = context.getInputDf(node, DEFAULT_INPORT)
    val inputCol = parseHead(node, "inputCol")
    val word2Vec = initEstimatorParams(new Word2Vec(), node).setInputCol(inputCol)
    transform(node, context, word2Vec, inputDF)
  }
}

/**
  * Feature hashing projects a set of categorical or numerical features into a feature vector of
  * specified dimension (typically substantially smaller than that of the original feature
  * space). This is done using the hashing trick (https://en.wikipedia.org/wiki/Feature_hashing)
  * to map features to indices in the feature vector.
  */
class MLFeatureHasher extends FeatureEngineering[FeatureHasher] with ParamsParser {
  override protected def parseInternal(node: Node, context: ParseContext)
    (transform: (Node, ParseContext, FeatureHasher, DataFrame) => DataFrame): DataFrame = {
    val inputDF = context.getInputDf(node, DEFAULT_INPORT)
    val inputColumns = parseArray(node, "inputCols")
    val hasher = initEstimatorParams(new FeatureHasher(), node).setInputCols(inputColumns)
    val categoricalCols = parseArray(node, "categoricalCols")
    if (categoricalCols.length > 0) {
      hasher.setCategoricalCols(categoricalCols)
    }
    transform(node, context, hasher, inputDF)
  }
}

/**
 * A one-hot encoder that maps a column of category indices to a column of binary vectors, with
 * at most a single one-value per row that indicates the input category index.
 */
class MLOneHotEncoder extends FeatureEngineering[OneHotEncoderEstimator] with ParamsParser {
  override protected def parseInternal(node: Node, context: ParseContext)
    (transform: (Node, ParseContext, OneHotEncoderEstimator, DataFrame) => DataFrame): DataFrame = {
    val inputDF = context.getInputDf(node, DEFAULT_INPORT)
    val inputColumns = parseArray(node, "inputCols")
    inputColumns.foldLeft(inputDF) { case (df, col) =>
      val encoder = initEstimatorParams(new OneHotEncoderEstimator(), node)
        .setInputCols(Array(col))
        .setOutputCols(Array(s"${col}_E"))
      transform(node, context, encoder, df)
    }

  }
}

/**
  * A feature transformer that filters out stop words from input.
  *
  */
@ParamsAnnotation(paramsType = classOf[StopWordsRemover], name = "StopWordsRemover", id = "stopwordsremover", desc = "")
class MLStopWordsRemover extends FeatureEngineering[StopWordsRemover] {
  override protected def parseInternal(node: Node, context: ParseContext)(
    transform: (Node, ParseContext, StopWordsRemover, DataFrame) => DataFrame): DataFrame = {
    val inputDF = context.getInputDf(node, DEFAULT_INPORT)
    val caseSensitive = parseBoolean(node, "caseSensitive")
    val outputCol = node.getParamValue("outputCol")
    val inputCol = parseHead(node, "inputCol")
    val stopWords = parseArray(node, "stopWords")
    val stopWordsRemover = new StopWordsRemover()
      .setInputCol(inputCol).setStopWords(stopWords).setOutputCol(outputCol).setCaseSensitive(caseSensitive)
    transform(node, context, stopWordsRemover, inputDF)
  }
}

@ParamsAnnotation(paramsType = classOf[CountVectorizer], name = "CountVectorizer", id = "countvectorizer", desc = "")
class MLCountVectorizer extends FeatureEngineering[CountVectorizer] with ParamsParser {
  override protected def parseInternal(node: Node, context: ParseContext)(
    transform: (Node, ParseContext, CountVectorizer, DataFrame) => DataFrame): DataFrame = {
    val inputDF = context.getInputDf(node, DEFAULT_INPORT)
    val inputCol = parseHead(node, "inputCol")
    val vectorizer = initEstimatorParams(new CountVectorizer(), node).setInputCol(inputCol)
    transform(node, context, vectorizer, inputDF)
  }
}

@ParamsAnnotation(paramsType = classOf[StandardScaler], name = "StandardScaler", id = "standardScaler", desc = "")
class MLStandardScaler extends FeatureEngineering[StandardScaler] with ParamsParser {
  override protected def parseInternal(node: Node, context: ParseContext)(
    transform: (Node, ParseContext, StandardScaler, DataFrame) => DataFrame): DataFrame = {
    val inputDF = context.getInputDf(node, DEFAULT_INPORT)
    val inputCol = parseHead(node, "inputCol")
    val scaler = initEstimatorParams(new StandardScaler(), node).setInputCol(inputCol)
    transform(node, context, scaler, inputDF)
  }
}

@ParamsAnnotation(paramsType = classOf[VectorIndexer], name = "VectorIndexer", id = "vectorIndexer", desc = "")
class MLVectorIndexer extends FeatureEngineering[VectorIndexer] with ParamsParser {
  override protected def parseInternal(node: Node, context: ParseContext)(
    transform: (Node, ParseContext, VectorIndexer, DataFrame) => DataFrame): DataFrame = {
    // Without cache the same df will be scanned for multiple times
    val inputDF = context.getInputDf(node, DEFAULT_INPORT).cache()
    val inputCol = parseHead(node, "inputCol")
    val indexer = initEstimatorParams(new VectorIndexer(), node).setInputCol(inputCol)
    transform(node, context, indexer, inputDF)
  }
}

@ParamsAnnotation(paramsType = classOf[VectorSlicer], name = "VectorSlicer", id = "vectorSlicer", desc = "")
class MLVectorSlicer extends FeatureEngineering[VectorSlicer] with ParamsParser {
  override protected def parseInternal(node: Node, context: ParseContext)(
    transform: (Node, ParseContext, VectorSlicer, DataFrame) => DataFrame): DataFrame = {
    val inputDF = context.getInputDf(node, DEFAULT_INPORT)
    val inputCol = parseHead(node, "inputCol")
    val indices = parseCommaSepList(node, "indices").map(_.toInt)
    val names = parseCommaSepList(node, "names")
    val outputCol = node.getParamValue("outputCol")
    val indexer = new VectorSlicer().setInputCol(inputCol).setOutputCol(outputCol).setIndices(indices).setNames(names)
    transform(node, context, indexer, inputDF)
  }
}

@ParamsAnnotation(paramsType = classOf[ChiSqSelector], name = "ChiSqSelector", id = "chiSqSelector", desc = "")
class MLChiSqSelector extends FeatureEngineering[ChiSqSelector] with ParamsParser {
  override protected def parseInternal(node: Node, context: ParseContext)(
    transform: (Node, ParseContext, ChiSqSelector, DataFrame) => DataFrame): DataFrame = {
    val inputDF = context.getInputDf(node, DEFAULT_INPORT)
    val labelCol = parseHead(node, "labelCol")
    val featuresCol = parseHead(node, "featuresCol")
    val indexer = initEstimatorParams(new ChiSqSelector(), node, Array("labelCol", "featuresCol"))
      .setLabelCol(labelCol).setFeaturesCol(featuresCol)
    transform(node, context, indexer, inputDF)
  }
}
