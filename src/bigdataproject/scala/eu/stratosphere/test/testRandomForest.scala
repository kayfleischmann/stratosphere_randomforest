package bigdataproject.scala.eu.stratosphere.test

import bigdataproject.scala.eu.stratosphere.ml.randomforest.DecisionTreeBuilder
import bigdataproject.scala.eu.stratosphere.ml.randomforest.RandomForestBuilder
import bigdataproject.scala.eu.stratosphere.ml.randomforest.Histogram

object testRandomForest {
 
  def main(args: Array[String]) {
    val output = "/home/kay/rf/"
    val input = "/home/kay/Dropbox/kay-rep/Uni-Berlin/MA_INF_Sem3_WS13/BigDataAnalytics/datasets/normalized_0to2full_test.txt"
	val outputFile = "/home/kay/rf_output"
	val outputTreeFile = "/home/kay/rf_output_tree"
	val outputNodeQueueFile = "/home/kay/rf_output_queue"
      
    new RandomForestBuilder().build( output, outputNodeQueueFile, input, outputFile, outputTreeFile, 10 )
     
    System.exit(0)
  }
 
}