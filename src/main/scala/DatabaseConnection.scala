import java.sql.{Connection, DriverManager}

/**
  * Created by USER on 2017-05-02.
  */
object DatabaseConnection {
  def getPosmallDbConn(): Connection = {
    val driver = "com.tmax.tibero.jdbc.TbDriver";
    val url = "jdbc:tibero:thin:@192.168.100.128:8629:tibero";
    val username = "ATEAT";
    val password = "atqwaszx12";

    var connection:Connection = null;
    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)
    return connection
  }

  def getPrdPosmallDbConn(): Connection = {
    val driver = "com.tmax.tibero.jdbc.TbDriver";
    val url = "jdbc:tibero:thin:@192.168.100.128:8629:tibero";
    val username = "posmall";
    val password = "posmall";

    var connection:Connection = null;
    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)
    return connection
  }


  def getEatDbConn(): Connection = {
    val driver = "oracle.jdbc.driver.OracleDriver"
    val url = "jdbc:oracle:thin:@//192.168.100.67:1521/afct"
    val username = "ateat"
    val password = "atqwaszx12"

    var connection:Connection = null
    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)
    return connection
  }

  def getFarmDbConn(): Connection = {
    val driver = "com.tmax.tibero.jdbc.TbDriver";
    val url = "jdbc:tibero:thin:@192.168.100.128:8629:tibero";
    val username = "farmdb"
    val password = "upload$0314"

    var connection:Connection = null
    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)
    return connection
  }

}
