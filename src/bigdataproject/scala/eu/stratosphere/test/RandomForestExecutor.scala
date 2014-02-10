package bigdataproject.scala.eu.stratosphere.test

import bigdataproject.scala.eu.stratosphere.ml.randomforest.DecisionTreeBuilder
import bigdataproject.scala.eu.stratosphere.ml.randomforest.RandomForestBuilder
import bigdataproject.scala.eu.stratosphere.ml.randomforest.Histogram

object RandomForestExecutor {
 
  def main(args: Array[String]) { 
    
    val path=args(0);
    val data=args(1)
    var remoteJar : String =null
    var remoteHost : String =null
    var remotePort : Int =0
    
    if(args.length > 4)
    	remoteJar=args(3)
    if(args.length > 5)
    	remoteHost=args(4)
    if(args.length > 6)
    	remoteHost=args(5)
    
	new RandomForestBuilder(remoteJar,remoteHost,remotePort).build(
	    path,
	    data,
	    args(2).toInt
	    )
  }
 
}