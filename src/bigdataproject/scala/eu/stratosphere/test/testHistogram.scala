package bigdataproject.scala.eu.stratosphere.test


import bigdataproject.scala.eu.stratosphere.ml.randomforest.Histogram
import org.junit.Test
import org.junit.Assert._
import org.scalatest.junit.JUnitSuite

class testHistogram extends JUnitSuite  {


  @Test
  def testHistogram_paper = {
	 val h = new Histogram(2,5)
	 h.update(23.0)
	 h.update(19.0)
	 h.update(10.0)
	 h.update(16.0)
	 h.update(36.0)

   assertTrue(  h.getBins.toList == List( (10.0,1), (16.0,1), (19.0,1), (23.0,1), (36.0,1) ) )
	 System.out.println( h.getBins.toList.toString )
	 h.update(2.0)
	 System.out.println( h.getBins.toList.toString )
	 h.update(9.0)
	 System.out.println( h.getBins.toList.toString )
	 
	 h.update(32.0)
	 System.out.println( h.getBins.toList.toString )
	 h.update(30.0)
	 System.out.println( h.getBins.toList.toString )
	 h.update(45.0)
	 h.update(45.0)
	 h.update(45.0)
	 System.out.println( h.getBins.toString )
	 System.out.println( h.getBins.toString )
	 
	 
	 System.out.println("uniform");
	 System.out.println( h.uniform(5)  ) 
  }
  
  def testHistogram_serialization = {
	 val strhistogram ="346;10;0.001636151149201403 302,0.09193899782135079 18,0.16823529411764707 10,0.28590102707749765 21,0.40620915032679733 12,0.5273725490196078 25,0.6425180598555209 19,0.7720965309200606 26,0.8930856553147575 38,0.991001880204138 146"

	 System.out.println(strhistogram)
	 val hh = Histogram.fromString(strhistogram)

	 System.out.println(hh.toString.equals(strhistogram))
	 System.out.println( hh.uniform(10) )
  }
}