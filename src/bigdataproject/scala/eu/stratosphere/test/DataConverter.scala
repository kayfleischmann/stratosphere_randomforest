package bigdataproject.scala.eu.stratosphere.test

import scala.io.Source
import scala.Array
import java.io.PrintWriter
// import following package
import scala.util.control._

/**
 * Created by kay on 2/9/14.
 */
object DataConverter {
  def main(args: Array[String]) {
    convertfromSVMlibtoOwn( "/home/kay/Desktop/mnist8m.raw",
                            "/home/kay/Desktop/mnist8m.dataset"
                            )
  }

  def convertfromSVMlibtoOwn(input : String, output : String){
    var counter=0
    val out = new PrintWriter(output);
    val loop = new Breaks;
    loop.breakable{
      for(  line <- Source.fromFile(input).getLines ){
        val values = line.split(" ")
        val label = values.head
        val randomCount = 784
        var arr: Array[Double] = Array()
        arr = Array(784)
        arr = Array.fill(randomCount)(0)
        for( v <- values.tail ){
          arr( v.split(":")(0).toInt-1 ) = v.split(":")(1).toDouble / 255.0
        }
        if (true ){
          var text = ""
          if (counter > 0)
            text += "\r\n"
          text += counter + " " + label + " " + arr.mkString(" ")
          out.print(text);
          counter = counter +1
            if(counter % 100000 == 0 ){
            System.out.println(counter)
          }//if
        }
        else {
          loop.break;
        }

      }
    }
    out.close()
  }
}
