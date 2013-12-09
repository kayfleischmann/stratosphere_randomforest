package bigdataproject.scala.eu.stratosphere.test

import eu.stratosphere.pact.client.LocalExecutor
import bigdataproject.scala.eu.stratosphere.ml.randomforest.DecisionTreeBuilder
import bigdataproject.scala.eu.stratosphere.ml.randomforest.RandomForestBuilder
import bigdataproject.scala.eu.stratosphere.ml.randomforest.Histogram

object testRandomForestOnStratosphere {
  
  def main(args: Array[String]) { 
	new RandomForestBuilder().build	
    //testHistogram
  }
  def testHistogram = {
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

	
	 System.out.println( h.uniform(6)  ) 
	 
	 val strhistogram ="728;10;0.0 641"
	
	
	 val hh = Histogram.fromString(strhistogram)
	 
	 System.out.println( hh.uniform(10) )
	 //System.out.println(hh.toString)

	 /*
	 val hh =Histogram.fromString("10;0.0011463046757164404 260,0.08466566113624936 39,0.1627450980392157 2,0.24248366013071898 30,0.3640866873065016 38,0.5039848197343454 31,0.6289592760180995 13,0.7274509803921568 26,0.8631127450980391 32,0.988489666136725 185")
	 System.out.println(hh.getBins.length)
	 System.out.println( hh.uniform(10) )
	 */
	 
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