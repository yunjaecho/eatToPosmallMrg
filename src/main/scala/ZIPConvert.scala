import java.io.InputStream
import java.sql.Connection

import scala.io.Source

/**
  * Created by USER on 2017-06-21.
  */
object ZIPConvert {
  case class ZipNoInfo(zipNo: String, userNo: Int, userType: String)

  var posmallConn: Connection = null

  def main(args: Array[String]): Unit = {
    val stream: InputStream = getClass.getResourceAsStream("DELIVERY_20171116_011524_성공.txt")
    val lines = Source.fromInputStream(stream, "ISO-8859-1").getLines().toList

    posmallConn = DatabaseConnection.getPrdPosmallDbConn()

    lines.foreach(line => {
      println(line)
      val arrLine = line.split('|')
      val insertSql = s"""
                        |INSERT INTO TMP_ZIP_CONVERT
                        |(COLUMN1,
                        | COLUMN2,
                        | ZIP_NO)
                        |VALUES
                        |('${arrLine(5)}',
                        | 'AA',
                        | '${arrLine(1)}')
                        | """.stripMargin
      val stmtUpdatePosmall = posmallConn.createStatement()
      stmtUpdatePosmall.execute(insertSql)
    })


  }
}
