import java.io.File
import java.sql.Connection

import scala.io.Source

/**
  * Created by USER on 2017-05-30.
  */
object PostConvert {
  var posmallConn: Connection = null

  def main(args: Array[String]): Unit = {
    posmallConn = DatabaseConnection.getPosmallDbConn()

    val folder = new File("D:\\Work\\zipcode_DB\\zip5")
    folder.listFiles().foreach(file => {
      val sido: String = file.getName.split('_')(1).replace(".txt", "")
      Source.fromFile(file, "ISO-8859-1").getLines().toList.tail.map(x=> (x.split('|')(0), sido)).distinct.sorted.foreach(
        zip => {
          posmallConn.createStatement().execute(s"INSERT INTO TB_OLD_ZIP (ZIP_NO, SIDO) VALUES('$zip._1', '$zip._2')")
        }
      )
    })



    //val lines: List[String] = Source.fromFile("D:\\Work\\20150710_경상남도.txt", "ISO-8859-1").getLines().toList
    //lines.tail.map(_.split('|')(0)).distinct.sorted.foreach(println)
  }

}
