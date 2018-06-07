import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

object ParseBuddyApp extends App {

  val conf = new SparkConf().setMaster("local[*]").setAppName("repl").set("spark.ui.enabled", "false")
  implicit val spark = SparkSession.builder().config(conf).appName("REPL").getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  def parse(query: String): LogicalPlan = {
    spark.sessionState.sqlParser.parsePlan(query)
  }

  val inputQuery = scala.io.StdIn.readLine("ParseBuddy> ")
  val lp = parse(inputQuery)
  println(lp)
  println(lp.prettyJson)


  spark.stop()
}
