import java.io.StringReader
import java.sql.{PreparedStatement, ResultSet, ResultSetMetaData, Statement}

/**
  * Created by USER on 2017-03-30.
  */
object Main {
  def main(args: Array[String]): Unit = {
    val tableListRead = new TableListRead()
    val tables = tableListRead.getTableList()

    val posmallConn = DatabaseConnection.getPosmallDbConn()
    val eatConn = DatabaseConnection.getEatDbConn()
    var selectSql = ""
    var insertSql:StringBuilder = null
    var insertHeaderSql:StringBuilder = null
    var insertHeaderStr:String = null

    var stmtEat: Statement = null
    var prstmtPosmall: PreparedStatement = null
    var stmtPosmall: Statement = null
    var resultSet:ResultSet = null
    var metaData: ResultSetMetaData = null

    tables.foreach(table => {
      println(table)

      if (table.equals("MBR_BANKACCT")) {
        selectSql = s"SELECT SEQ_NO, CUSTOMER_CD,BANK_CD,CRYPTO.DEC('normal', ACCT_NO) ACCT_NO,HOLDER,BASIC_ACCT_YN,BIGO,USE_GB,ACCT_IMG,REGR_ID,REG_DT,UPDR_ID,UPD_DT FROM ${table}"
      } else if (table.equals("MBR_CUSTOMER_TB")) {
        selectSql = s"""SELECT
                      | CUSTOMER_CD,
                      | UP_CUSTOMER_CD,
                      | CUSTOMER_GB,
                      | MAIN_BRANCH_GB,
                      | CUSTOMER_NM,
                      | CUSTOMER_SHORT_NM,
                      | CUSTOMER_ENG_NM,
                      | BIZ_LICENSE_NO,
                      | CRYPTO.DEC('normal', CORP_REG_NO) AS CORP_REG_NO,
                      | REP_JUMIN_NO,
                      | REP_NM,
                      | REP_ENG_NM,
                      | UPTAE,
                      | UPJONG,
                      | ZIP_CD,
                      | ADDR,
                      | DETAIL_ADDR,
                      | REP_TEL_NO,
                      | FAX_NO,
                      | MOBILE_NO,
                      | EMAIL_ADDR,
                      | HOMEPAGE,
                      | NATION_CD,
                      | FOREIGN_GB,
                      | FOREIGN_REG_NO,
                      | LIVING_YN,
                      | MGNT_BRANCH,
                      | INNER_CUSTOMER_CD,
                      | BIZ_START_DT,
                      | BIZ_END_DT,
                      | ASSCT_GB,
                      | BASE_RATE,
                      | GRP_USER_GB,
                      | USE_GB,
                      | REGR_ID,
                      | REG_DT,
                      | UPDR_ID,
                      | UPD_DT,
                      | BIZ_TYPE,
                      | FLOOR_SPACE,
                      | AREA_CD,
                      | MAIN_SALE_PRODUCT,
                      | RECENTLY_FOODPURCHASE_AMT,
                      | RECENTLY_SALE_AMT,
                      | RECENTLY_FOODSALE_AMT
                      | FROM MBR_CUSTOMER_TB
                      |""".stripMargin
      } else {
        selectSql = s"SELECT * FROM ${table}"
      }

      stmtEat = eatConn.createStatement()
      stmtPosmall = posmallConn.createStatement()
      resultSet = stmtEat.executeQuery(selectSql)
      metaData = resultSet.getMetaData

      println(s"metaData.getColumnCount : ${metaData.getColumnCount}")

      insertHeaderSql = new StringBuilder
      insertHeaderSql.append( s" INSERT INTO ${table} (");

      for (i <- 1 to metaData.getColumnCount) {
        val colName = metaData.getColumnName(i)

        insertHeaderSql.append(colName)

        if (i == metaData.getColumnCount) insertHeaderSql.append(" ) ")
        else insertHeaderSql.append(" , ")
      }

      insertHeaderSql.append(" VALUES ( ")
      insertHeaderStr = insertHeaderSql.toString()
      while(resultSet.next()) {
        insertSql = new StringBuilder(insertHeaderStr)

        for (i <- 1 to metaData.getColumnCount) {
          val colType = metaData.getColumnTypeName(i)
          var data = ""
          if (colType == "DATE") {
            if (resultSet.getTimestamp(i) == null)  data = "null"
            else data = s"TO_DATE('${resultSet.getTimestamp(i).toString.substring(0, 19)}', 'YYYY-MM-DD HH24:MI:SS')";
          } else if (colType == "CLOB") {
            data = "''"
          } else {
            data = resultSet.getString(i)
            if (data == null) data = "null"
            else data = s"'${data.replaceAll("'", "''")}'"
          }
          if (i == metaData.getColumnCount) insertSql.append(data + " )")
          else insertSql.append( data + " , ")
          //println(s"colType : ${colType}")
        }

        println(insertSql.toString())

        stmtPosmall.execute(insertSql.toString())

       if (table == "ITM_ITEM") {
         if (resultSet.getString("ITEM_DETAIL") != null) {
           prstmtPosmall = posmallConn.prepareStatement(s"UPDATE ITM_ITEM SET ITEM_DETAIL = ?  WHERE ITEM_CD = ${resultSet.getObject("ITEM_CD")}")
           val clob = resultSet.getString("ITEM_DETAIL")

           println("clob.length() : " + clob.length())

           prstmtPosmall.setCharacterStream(1 , new StringReader(clob), clob.length())
           prstmtPosmall.executeUpdate()
         }
        }

        /*
        for (i <- 1 to metaData.getColumnCount) {
          val colType = metaData.getColumnTypeName(i)
          val colName = metaData.getColumnName(i)

          if (colType == "CLOB") {
            prstmtPosmall = posmallConn.prepareStatement(s"INSERT INTO ${table} SET colName = ?")
          }
        }*/

      }


    });

    eatConn.close()
    if (prstmtPosmall != null)
      prstmtPosmall.close()
    posmallConn.close()
  }



}
