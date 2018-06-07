import com.facebook.presto.sql.parser.SqlParser
import com.facebook.presto.sql.parser.ParsingOptions
import com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE
import com.facebook.presto.sql.tree.Statement

object PrestoBuddyApp extends App {

  private val exitCommands = Seq("exit", ":q", "q")
  def exitCommand(command: String): Boolean = exitCommands.contains(command.toLowerCase)

  private val prestSqlParser = new SqlParser()

  def parsePrestoSql(query: String): Statement = {
    val parsingOptions = new ParsingOptions(AS_DOUBLE /* anything */)
    val statement = prestSqlParser.createStatement(query, parsingOptions)
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
