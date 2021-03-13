import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

class TestSnowflake {

}
object TestSnowflake {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Alert handle")
      .getOrCreate()
    val sqlContext = spark.sqlContext

    var sfOptions = Map(
      "sfURL" -> "",  // <account>.<server>.snowflakecomputing.com
      "sfUser" -> "",   // Username, example kienhd1
      "sfAccount" -> "", // account, use account in sfURL, example zl8993
      "sfPassword" -> "", // edit password here
      "sfRole" ->    "SYSADMIN",  // Role
      "sfDatabase" -> "", // dbname
      "sfSchema" ->  "PUBLIC",
      "sfWarehouse" -> "COMPUTE_WH"
    )
    val SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

    val df: DataFrame = sqlContext.
      read.format(SNOWFLAKE_SOURCE_NAME).
      options(sfOptions).
      option("query", "SELECT ALERT_DATA:SrcIPv4, ALERT_DATA:DstIPv4, ALERT_DATA:ID, ALERT_DATA:EventTime FROM ALERT_PARQUET;") // edit query here
      .load()

    df.write.parquet("")  // edit path here
  }
}