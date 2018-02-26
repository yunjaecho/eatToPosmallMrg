import java.io.{File, InputStream}
import java.net.{HttpURLConnection, URL}

import scala.io.Source

/**
  * Created by USER on 2017-05-19.
  */
object ImageTest {
  def main(args: Array[String]): Unit = {
    var imgUrl = "http://image.eatmart.co.kr/ateatout/Image/2016/04/05/1459821115593.png"
    val url = new URL(imgUrl)
    val connection: HttpURLConnection = url.openConnection().asInstanceOf[HttpURLConnection]
    connection.setRequestMethod("GET")

    val contentType = connection.getHeaderField("Content-Type")

    println("contentType : " + contentType)

    //in = connection.getInputStream


    /*var dir = new File("D:\\FOOD_DB\\Images\\GOODS_L")
    for (file <- dir.listFiles()) {

    }*/
  }

}
