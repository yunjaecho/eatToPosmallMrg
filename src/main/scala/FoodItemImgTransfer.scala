import java.awt.image.BufferedImage
import java.io.{File, _}
import java.net.{HttpURLConnection, URL}
import java.sql.Connection
import javax.imageio.ImageIO

import com.sksamuel.scrimage.Image
import com.sksamuel.scrimage.ScaleMethod._
import com.sksamuel.scrimage.nio.JpegWriter

/**
  * Created by USER on 2017-05-02.
  */
object FoodItemImgTransfer {
  var posmallConn: Connection = null

  /**
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    posmallConn = DatabaseConnection.getPosmallDbConn()

    val stmtPosmall = posmallConn.createStatement()
    /*val resultSet   = stmtPosmall.executeQuery("""SELECT
                                                 |      FILE_DESCRIPTION,
                                                 |      SAVED_PATH,
                                                 |      SAVED_PATH AS SAVED_PATH_S,
                                                 |      CONTENT_FILE_ID,
                                                 |      CONTAINER_CATEGORY
                                                 |FROM  POSMALL.TB_CONTENT_FILE
                                                 |WHERE USER_ID = 'foodeat'
                                                 | AND CONTAINER_CATEGORY IN ('GOODS_L', 'GOODS_L_2', 'GOODS_L_3', 'GOODS_M')""".stripMargin)*/
    val resultSet   = stmtPosmall.executeQuery("""
                                                 |  SELECT
                                                 |        B.FILE_DESCRIPTION,
                                                 |        A.SAVED_PATH,
                                                 |        B.SAVED_PATH AS SAVED_PATH_S,
                                                 |        B.CONTENT_FILE_ID,
                                                 |        B.CONTAINER_CATEGORY
                                                 |  FROM  POSMALL.TB_CONTENT_FILE A,
                                                 |        POSMALL.TB_CONTENT_FILE B
                                                 |  WHERE A.USER_ID = 'foodeat'
                                                 |    AND A.CONTAINER_CATEGORY = 'GOODS_L'
                                                 |    AND B.USER_ID = 'foodeat'
                                                 |    AND B.CONTAINER_CATEGORY = 'GOODS_S'
                                                 |    AND A.CONTAINER_ID = B.CONTAINER_ID
                                                 """.stripMargin)

    while (resultSet.next()) {
      val fileUrl = resultSet.getString("FILE_DESCRIPTION")
      val savePath = resultSet.getString("SAVED_PATH")
      val savePathS = resultSet.getString("SAVED_PATH_S")
      val fileId = resultSet.getInt("CONTENT_FILE_ID")
      val category = resultSet.getString("CONTAINER_CATEGORY")
      downloadFile(fileUrl, savePath, fileId, category, savePathS)
    }

  }

  /**
    *
    * @param fileUrl
    * @param savePath
    * @param fileId
    * @param category
    */
  def downloadFile(fileUrl: String, savePath: String, fileId: Int, category: String, savePathS: String): Unit = {
    var connection: HttpURLConnection = null
    var in: InputStream = null
    var out: OutputStream = null

    try {
      val url = new URL(fileUrl)
      connection = url.openConnection().asInstanceOf[HttpURLConnection]
      connection.setRequestMethod("GET")

      val contentType = connection.getHeaderField("Content-Type")
      // Content-Type image 인 경우만 처리
      if (contentType.indexOf("image") >= 0) {
        in = connection.getInputStream
        var fileSize:Long = 0

        if (category == "GOODS_S") {
          val fromFile = new java.io.File(s"D:/FOOD_DB/Images/$savePath")
          val toFile = new java.io.File(s"D:/FOOD_DB/Images/$savePathS")
          fileSize = createThumbnail(fromFile, toFile)
        } else {
          val fileToDownloadAs = new java.io.File(s"D:/FOOD_DB/Images/$savePath")

          val out: OutputStream = new BufferedOutputStream(new FileOutputStream(fileToDownloadAs, true))
          val byteArray = Stream.continually(in.read).takeWhile(-1 !=).map(_.toByte).toArray
          out.write(byteArray)
          fileSize = byteArray.length
        }

        // File Size UPDATE
        val updateSql = s"UPDATE POSMALL.TB_CONTENT_FILE SET FILE_SIZE = $fileSize WHERE CONTENT_FILE_ID = $fileId"
        val stmtUpdatePosmall = posmallConn.createStatement()
        stmtUpdatePosmall.execute(updateSql)
      }



    } catch {
      case err: Exception => err.printStackTrace
    } finally {
      if (connection != null) connection.disconnect()
      if (in != null) in.close()
      if (out != null) out.close()
    }

    /**
      *
      * @param inFile
      * @param outFile
      */
    def createThumbnail(inFile:File, outFile:File): Long = {
      //val inStream = new FileInputStream(inFile)
      val image = Image.fromFile(inFile).scaleTo(300, 300, FastScale).output(outFile)(JpegWriter())
      image.length()
    }

  }


}
