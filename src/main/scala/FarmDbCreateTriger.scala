import java.sql.{ResultSet, Statement}

/**
  * Created by USER on 2017-09-06.
  */
object FarmDbCreateTriger {
  val posmallConn = DatabaseConnection.getFarmDbConn();
  var stmtPosmall: Statement = null
  var resultSet:ResultSet = null
  var selectSql = ""
  var strTriggerStmt = ""
  var strTriggerHeaderSmt = ""
  var strTriggerDeclareSmt = ""
  var strTriggerConditionSmt = ""
  val strTableName = "AAT_FARM_PRODUCT_ITEM_STATUS";
  val strPkColumn  = "FARM_ID"
  val strPkColumn2  = "SEQ"
  val strPkColumn3  = ""

  def main(args: Array[String]): Unit = {
    selectSql =
      s"""
         |SELECT
         |      A.TABLE_NAME,
         |      A.COLUMN_NAME ,
         |      B.COMMENTS,
         |      (CASE
         |           WHEN A.DATA_TYPE = 'VARCHAR' THEN A.DATA_TYPE || '(' || A.DATA_LENGTH  || ')'
         |           ELSE A.DATA_TYPE
         |       END) DATA_TYPE
         |FROM  USER_TAB_COLUMNS A,
         |      USER_COL_COMMENTS B
         |WHERE A.TABLE_NAME = '${strTableName}'
         |  AND A.TABLE_NAME = B.TABLE_NAME
         |  AND A.COLUMN_NAME = B.COLUMN_NAME
         |  AND A.DATA_TYPE <> 'CLOB'
       """.stripMargin

    strTriggerHeaderSmt =
      s"""
         |CREATE OR REPLACE TRIGGER TR_${strTableName}
         |AFTER UPDATE
         |ON ${strTableName}
         |REFERENCING NEW AS NEW OLD AS OLD
         |FOR EACH ROW
         |DECLARE
         |  V_INSERT_SELECT_STMT VARCHAR(32000) := ' ';
         |  V_PK_VALUE VARCHAR(1000);
         |  V_CHG_CNT  NUMBER := 0;
       """.stripMargin

    stmtPosmall = posmallConn.createStatement()
    resultSet = stmtPosmall.executeQuery(selectSql)

    while(resultSet.next()) {
      //strTriggerDeclareSmt += s"\tOLD_${resultSet.getString("COLUMN_NAME")}    ${resultSet.getString("DATA_TYPE")};\n"
      var strPkColumn2Value = "";
      var strPkColumn3Value = "";
      if (strPkColumn2.isEmpty) strPkColumn2Value = "''" else strPkColumn2Value = s"TO_CHAR(:NEW.${strPkColumn2})"
      if (strPkColumn3.isEmpty) strPkColumn3Value = "''" else strPkColumn3Value = s"TO_CHAR(:NEW.${strPkColumn3})"

      strTriggerConditionSmt +=
        s"""
           |     /*     ${resultSet.getString("COMMENTS")}     */
           |	 IF (:OLD.${resultSet.getString("COLUMN_NAME")} IS NULL AND :NEW.${resultSet.getString("COLUMN_NAME")} IS NOT NULL) OR (:OLD.${resultSet.getString("COLUMN_NAME")} IS NOT NULL AND :NEW.${resultSet.getString("COLUMN_NAME")} IS NULL) OR (:OLD.${resultSet.getString("COLUMN_NAME")} <> :NEW.${resultSet.getString("COLUMN_NAME")}) THEN
           |      V_CHG_CNT := V_CHG_CNT + 1;
           |      V_INSERT_SELECT_STMT := V_INSERT_SELECT_STMT || ' SELECT ''${resultSet.getString("TABLE_NAME")}'' AS TABLE_NAME , ''${resultSet.getString("COLUMN_NAME")}'' AS COLUMN_NAME, ''' || TO_CHAR(:NEW.${strPkColumn})  || ''' AS PK_KEY, ''' || TO_CHAR(:OLD.${resultSet.getString("COLUMN_NAME")}) || ''' AS OLD_VALUE, '''|| TO_CHAR(:NEW.${resultSet.getString("COLUMN_NAME")}) || ''' AS NEW_VALUE,  '''|| ${strPkColumn2Value} || ''' AS PK_KEY2,  '''|| ${strPkColumn3Value} || ''' AS PK_KEY3 FROM DUAL UNION ALL';
           |	 END IF;
         """.stripMargin
    }

    //strTriggerStmt += strTriggerHeaderSmt + "\n" + strTriggerDeclareSmt + "\n" + "BEGIN" + "\n" + strTriggerConditionSmt + "\n" + "END;"
    strTriggerStmt = s"""
                        |${strTriggerHeaderSmt}
                        |BEGIN
                        |
         |${strTriggerConditionSmt}
                        |     IF V_CHG_CNT > 0 THEN
                        |        V_INSERT_SELECT_STMT := SUBSTR(V_INSERT_SELECT_STMT,1, LENGTH(V_INSERT_SELECT_STMT) -9);
                        |        V_INSERT_SELECT_STMT := 'INSERT INTO AAT_TABLE_UPDATE_HST SELECT AAT_TABLE_UPDATE_HST_SEQ.NEXTVAL, TABLE_NAME, COLUMN_NAME, PK_KEY, OLD_VALUE, NEW_VALUE, CURRENT_TIMESTAMP (2), PK_KEY2, PK_KEY3 FROM ( ' || V_INSERT_SELECT_STMT || ' )';
                        |        EXECUTE IMMEDIATE V_INSERT_SELECT_STMT;
                        |       END IF;
                        |END;
       """.stripMargin

    println(strTriggerStmt)

  }
}
