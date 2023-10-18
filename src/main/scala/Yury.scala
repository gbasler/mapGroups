import org.apache.spark.sql.SparkSession

import scala.annotation.tailrec

object Demo {

  def main(args: Array[String]): Unit = {

    // 1
    val session = SparkSession.builder()
      .appName("Blog_Demo")
      .config("spark.sql.parquet.writeLegacyFormat", value = true)
      .getOrCreate()

    session.sparkContext.setLogLevel("WARN")

    import session.implicits._
    import org.apache.spark.sql.functions._

    // 2
    val sales = session
      .read.option("delimiter", ",").option("header", "true").option("inferSchema", "true")
      .csv(s"/Users/pmishr43/dev/data/sales_records.csv")
      .select(
        $"cookie_id".as("cookieId"),
        to_date($"event_arrival_time").as("eventTime"),
        $"urn".as("urn")).as[Data]

    // 3
    val output = sales.groupByKey(item => item.cookieId).flatMapGroups(rollUpEvents)
    output.show(false)
  }

  private def rollUpEvents(cookieId: String, data: Iterator[Data]): Seq[RolledUpData] = {
    val sortedDataset = data.toSeq.sortBy(_.eventTime).zipWithIndex
    sortedDataset.map {
      case (data, index) =>
        val isLast = index == sortedDataset.size - 1
        RolledUpData(data.cookieId, index, isLast, s"$cookieId ${sortedDataset.head._1.eventTime} ", data.eventTime, data.urn)
    }
  }
}

case class Data(cookieId: String, eventTime: java.sql.Date, urn: String)

case class RolledUpData(cookieId: String, sessionRank: Int, sessionEnd: Boolean, sessionId: String, eventArrivalTime: java.sql.Date, urn: String)