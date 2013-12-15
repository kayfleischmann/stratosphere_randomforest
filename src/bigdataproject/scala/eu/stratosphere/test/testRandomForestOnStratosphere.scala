package bigdataproject.scala.eu.stratosphere.test

import eu.stratosphere.pact.client.LocalExecutor
import bigdataproject.scala.eu.stratosphere.ml.randomforest.DecisionTreeBuilder
import bigdataproject.scala.eu.stratosphere.ml.randomforest.RandomForestBuilder
import bigdataproject.scala.eu.stratosphere.ml.randomforest.Histogram

object testRandomForestOnStratosphere {
 
  def main(args: Array[String]) { 
	new RandomForestBuilder().build	
  }
 
}