package bigdataproject.scala.eu.stratosphere.test

import bigdataproject.scala.eu.stratosphere.ml.randomforest.Histogram

object testHistogram {
 
  def main(args: Array[String]) { 
    testHistogram_loaddata
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
	 h.update(23)
	 h.update(19)
	 h.update(10)
	 h.update(16)
	 h.update(36)
	 System.out.println( h.getBins.toString )
	 h.update(2)
	 System.out.println( h.getBins.toString )
	 h.update(9)
	 System.out.println( h.getBins.toString )
	 
	 h.update(32)
	 System.out.println( h.getBins.toString )
	 h.update(30)
	 System.out.println( h.getBins.toString )
	 h.update(45)
	 System.out.println( h.getBins.toString )
	 System.out.println( h.getBins.toString )
	 
	 
	 System.out.println( "sum:"+h.sum(1)  ) 
	 System.out.println( "sum:"+h.sum(2)  ) 
	 System.out.println( "sum:"+h.sum(33)  ) 
	 System.out.println( "sum:"+h.sum(44)  ) 
	 System.out.println( "sum:"+h.sum(45)  ) 

	 System.out.println("uniform");
	 System.out.println( h.uniform(6)  ) 
  }
  
  def testHistogram_serialization = {
	 val strhistogram ="346;10;0.001636151149201403 302,0.09193899782135079 18,0.16823529411764707 10,0.28590102707749765 21,0.40620915032679733 12,0.5273725490196078 25,0.6425180598555209 19,0.7720965309200606 26,0.8930856553147575 38,0.991001880204138 146"

	 System.out.println(strhistogram)
	 val hh = Histogram.fromString(strhistogram)

	 System.out.println(hh.toString.equals(strhistogram))
	 System.out.println( hh.uniform(10) )
  }
}