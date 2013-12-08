package bigdataproject.scala.eu.stratosphere.test

import eu.stratosphere.pact.client.LocalExecutor
import bigdataproject.scala.eu.stratosphere.ml.randomforest.DecisionTreeBuilder
import bigdataproject.scala.eu.stratosphere.ml.randomforest.RandomForestBuilder
import bigdataproject.scala.eu.stratosphere.ml.randomforest.Histogram

object testRandomForestOnStratosphere {
  
  def main(args: Array[String]) { 
	//new RandomForestBuilder().build	
    testHistogram
  }
  def testHistogram = {
	 val h = new Histogram(5)
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
	 
	 
	 System.out.println( h.sum(15)  ) 

	 /*
	 h.merge( h )
	 val h2 = h.merge( h )
	 System.out.println( h)

	 System.out.println( h2.sum(3)  ) 
	 
	 val serial = h2.toString
	 System.out.println(serial)
	 
	 val hFromString = Histogram.fromString(serial)
	 System.out.println(hFromString.toString)*/
  }
}