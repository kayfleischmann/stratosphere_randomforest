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
	 h.update(11)
	 h.update(0.1)
	 h.update(0.1)
	 h.update(0.1)
	 h.update(0.1)
	 h.update(0.1)
	 System.out.println( h.toString )
	 h.update(0.12)
	 System.out.println( h.toString )
	 
	 h.update(3)
	 System.out.println( h.toString )
	 h.update(4.5)
	 System.out.println( h.toString )
	 h.update(4.6)
	 System.out.println( h.toString )
	 h.update(4.9)
	 System.out.println( h.toString )
	 
	 
	 h.merge( h )
	 System.out.println(  h.merge( h ).toString ) 
  }
}