import java.io.InputStream

import scala.io.Source

/**
  * Created by USER on 2017-03-30.
  */
class TableListRead {
  def getTableList(): List[String] = {
    val stream: InputStream = getClass.getResourceAsStream("table.txt")
    Source.fromInputStream(stream).getLines().toList;
  }
}
