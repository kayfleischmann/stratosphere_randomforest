package bigdataproject.scala.eu.stratosphere.test

import bigdataproject.scala.eu.stratosphere.ml.randomforest.DecisionTreeBuilder
import bigdataproject.scala.eu.stratosphere.ml.randomforest.RandomForestBuilder
import bigdataproject.scala.eu.stratosphere.ml.randomforest.Histogram

object testRandomForest {
 
  def main(args: Array[String]) {

    new RandomForestBuilder().build(
      "/home/kay/rf/",
      "/home/kay/Desktop/mnist8m.dataset",
      "/home/kay/rf/rf_input_nodequeue",
      "/home/kay/rf/rf_output",
      "/home/kay/rf/rf_output_tree",
      1
    )

  }
 
}