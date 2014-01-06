package bigdataproject.scala.eu.stratosphere.test

import eu.stratosphere.pact.client.LocalExecutor
import bigdataproject.scala.eu.stratosphere.ml.randomforest.DecisionTreeBuilder
import bigdataproject.scala.eu.stratosphere.ml.randomforest.RandomForestBuilder
import bigdataproject.scala.eu.stratosphere.ml.randomforest.Histogram

object testRandomForest {
 
  def main(args: Array[String]) {
    
    val input = "/home/kay/Dropbox/kay-rep/Uni-Berlin/MA_INF_Sem3_WS13/BigDataAnalytics/datasets/normalized.txt"
	val outputFile = "/home/kay/rf_output"
	val outputTreeFile = "/home/kay/rf_output_tree"
      
    new RandomForestBuilder().build(input, outputFile, outputTreeFile, 10 )
     
    System.exit(0)
  }
 
}