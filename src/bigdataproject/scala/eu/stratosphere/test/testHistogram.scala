package bigdataproject.scala.eu.stratosphere.test

import bigdataproject.scala.eu.stratosphere.ml.randomforest.Histogram
import scala.util.Random 

object testHistogram {
 
  def main(args: Array[String]) { 
    testHistogram_randomValues
  }

  
   def testHistogram_randomValues = {
	 val h = new Histogram(2,10)
	 
	 (0 until 20000).foreach({ x =>
		 val max =Random.nextDouble * (Random.nextInt(60) - 30)
		 System.out.println(max)
		 h.update(max)
	 })

	 h.print
     System.out.println( h.toString );
     System.out.println( h.uniform(10) );
     System.out.println( h.getNormalSum);

  }

  
  def testHistogram_60k_failed = {
	 val str="162;10;3.4508287104401825E-4 54980,0.11762102073598131 856,0.23971795550847458 590,0.3457754629629628 405,0.44886001275510207 392,0.5411847014925373 335,0.634043560606061 330,0.7468771462912088 364,0.867412109375 400,0.9826160283753709 1348"
		  
	 val hh = Histogram.fromString(str)
	 hh.print
	 System.out.println( hh.uniform(10) );

  }
  
  def testHistogram_vlaues =  {
	 val strhistogram ="346;10;0.001636151149201403 302,0.09193899782135079 18,0.16823529411764707 10,0.28590102707749765 21,0.40620915032679733 12,0.5273725490196078 25,0.6425180598555209 19,0.7720965309200606 26,0.8930856553147575 38,0.991001880204138 146"

	 System.out.println(strhistogram)
	 val hh = Histogram.fromString(strhistogram)

	 System.out.println( hh.uniform(10) );
  }
  
  def testHistogram_loaddata = {
	val h = new Histogram(0,10);
	val values = List(0.1, 0.5, 0.5, 0.5, 0.1, 0.10, 1, 1, 0.002, 0.44, 0.55, 0.22, 0.444, 0.003, 0.002, 0.001, 0.0003, 0.0010, 0.2, 0.3, 0.003, 0.007,
				0.8, 0.997, 0.002, 0.003, 0.002);
	
	for( v <- values ) {
	  h.update( v )
	}//for	
  
	h.print
	System.out.println( h.uniform(10) )
	
  }
  
  def testHistogram_paper = {
	 val h = new Histogram(2,5)
	 h.update(23.0/100)
	 h.update(19.0/100)
	 h.update(10.0/100)
	 h.update(16.0/100)
	 h.update(36.0/100)
	 System.out.println( h.getBins.toString )
	 h.update(2.0/100)
	 System.out.println( h.getBins.toString )
	 h.update(9.0/100)
	 System.out.println( h.getBins.toString )
	 
	 h.update(32.0/100)
	 System.out.println( h.getBins.toString )
	 h.update(30.0/100)
	 System.out.println( h.getBins.toString )
	 h.update(45.0/100)
	 h.update(45.0/100)
	 h.update(45.0/100)
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