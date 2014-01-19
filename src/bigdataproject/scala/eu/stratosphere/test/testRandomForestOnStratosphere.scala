package bigdataproject.scala.eu.stratosphere.test

import bigdataproject.scala.eu.stratosphere.ml.randomforest.DecisionTreeBuilder
import bigdataproject.scala.eu.stratosphere.ml.randomforest.RandomForestBuilder
import bigdataproject.scala.eu.stratosphere.ml.randomforest.Histogram

object testRandomForestOnStratosphere {
 
  def main(args: Array[String]) { 
	new RandomForestBuilder().build(
	    "/home/kay/rf/",
	    "/home/kay/Dropbox/kay-rep/Uni-Berlin/MA_INF_Sem3_WS13/BigDataAnalytics/datasets/normalized_full.txt",
	    "/home/kay/rf/rf_input_nodequeue",
	    "/home/kay/rf/rf_output",
	    "/home/kay/rf/rf_output_tree",
	    1
	    )
  }
 
}