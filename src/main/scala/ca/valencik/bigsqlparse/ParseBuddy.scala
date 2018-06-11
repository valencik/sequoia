import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import com.facebook.presto.sql.parser.SqlParser
import com.facebook.presto.sql.parser.ParsingOptions
import com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE
import com.facebook.presto.sql.tree.Statement

object PrestoBuddyApp extends App {

  private val exitCommands = Seq("exit", ":q", "q")
  def exitCommand(command: String): Boolean = exitCommands.contains(command.toLowerCase)

  private val prestoSqlParser = new SqlParser()

  def parsePrestoSql(query: String): Statement = {
    val parsingOptions = new ParsingOptions(AS_DOUBLE /* anything */)
    val statement = prestoSqlParser.createStatement(query, parsingOptions)
    statement
  }

  def prestoLoop(): Unit = {
    val inputQuery = scala.io.StdIn.readLine("ParseBuddy> ")
    if (!exitCommand(inputQuery)) {
      val lp = parsePrestoSql(inputQuery)
      println(lp)
      prestoLoop()
    }
  }

  prestoLoop()
}

object SparkBuddyApp extends App {

  val conf = new SparkConf().setMaster("local[*]").setAppName("repl").set("spark.ui.enabled", "false")
  implicit val spark = SparkSession.builder().config(conf).appName("REPL").getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._

  private val exitCommands = Seq("exit", ":q", "q")
  def exitCommand(command: String): Boolean = exitCommands.contains(command.toLowerCase)

  def parseSparkSql(query: String): LogicalPlan = {
    spark.sessionState.sqlParser.parsePlan(query)
  }

  def sparkLoop(): Unit = {
    val inputQuery = scala.io.StdIn.readLine("ParseBuddy> ")
    if (!exitCommand(inputQuery)) {
      val lp = parseSparkSql(inputQuery)
      println(lp)
      println(lp.prettyJson)
      sparkLoop()
    }
  }

  sparkLoop()
  spark.stop()
}
