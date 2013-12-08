package bigdataproject.scala.eu.stratosphere.test

import eu.stratosphere.pact.client.LocalExecutor
import bigdataproject.scala.eu.stratosphere.ml.randomforest.DecisionTreeBuilder
import bigdataproject.scala.eu.stratosphere.ml.randomforest.RandomForestBuilder

object testRandomForestOnStratosphere {
  
  def main(args: Array[String]) { 
	  System.out.println("start building random forest");
	  
	  new RandomForestBuilder().build	  
  }
}