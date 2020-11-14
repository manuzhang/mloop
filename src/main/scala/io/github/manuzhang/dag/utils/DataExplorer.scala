package io.github.manuzhang.dag.utils

import io.github.manuzhang.dag.DagParser
import io.github.manuzhang.dag.utils.DataExplorer.DataAndSchema
import io.github.manuzhang.dag.utils.SparkUtils.deserialize

object DataExplorer {

  case class EvaluateResultRequest(dagName: String, nodeId: String, outputPort: Int)

  case class Request(dagName: String, nodeId: String, outputPort: Int, rows: String, page: String)

  case class Response(data: Array[Array[String]], schema: String, records: Int, total: Int, page: Int)

  object Response {
    def apply(request: Request, dataAndSchema: DataAndSchema): Response = {
      val (data, schema) = dataAndSchema
      val curPage = request.page.toInt
      val rowsPerPage = request.rows.toInt
      val (start, end) = getRange(rowsPerPage, curPage)
      Response(data.slice(start, end), schema, data.length, getTotalPage(data.length, rowsPerPage), curPage)
    }


    private def getRange(rows: Int, page: Int): (Int, Int) = {
      val start = (page - 1) * rows
      val end = start + rows
      start -> end
    }

    private def getTotalPage(totalRows: Int, rowsPerPage: Int): Int = {
      math.ceil(totalRows.toDouble / rowsPerPage).toInt
    }
  }

  type DataAndSchema = (Array[Array[String]], String)

  val SCHEMA_NOT_FOUND: String = ""
}

class DataExplorer(collector: Collector) {

  /**
   * @param key data and schema key
   * @return
   */
  def explore(key: String): DataAndSchema = {
    getDataAndSchema(key)
  }

  /**
   * @param key schema key
   * @return
   */
  def exploreSchema(key: String): Option[String] = {
    Option(collector.load(key))
  }

  def exploreVis(key: String): String = {
    Option(collector.load(key)).getOrElse("{}")
  }

  private def getDataAndSchema(key: String): DataAndSchema = {
    val schema = exploreSchema(Collector.getSchemaKey(key)).getOrElse(DataExplorer.SCHEMA_NOT_FOUND)
    val values = collector.loadRange(Collector.getDataKey(key)).take(DagParser.SAMPLE_NUM_PER_PARTITION)

    val data = values.map(v => deserialize(v).toArray).toArray
    data -> schema
  }

}
