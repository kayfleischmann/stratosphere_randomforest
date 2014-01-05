package bigdataproject.scala.eu.stratosphere.test

import eu.stratosphere.pact.client.LocalExecutor
import bigdataproject.scala.eu.stratosphere.ml.randomforest.DecisionTreeBuilder
import bigdataproject.scala.eu.stratosphere.ml.randomforest.RandomForestBuilder
import bigdataproject.scala.eu.stratosphere.ml.randomforest.Histogram

object testEvaluationOnStratosphere {
 
  def main(args: Array[String]) { 
	new RandomForestBuilder().eval(
	    "/home/kay/Dropbox/kay-rep/Uni-Berlin/MA_INF_Sem3_WS13/BigDataAnalytics/datasets/normalized_test.txt",
	    "/home/kay/rf_output_tree",
	    "/home/kay/rf_output_evaluation"
	    )
  }
 
}