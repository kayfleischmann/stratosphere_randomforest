package bigdataproject.scala.eu.stratosphere.test

import eu.stratosphere.pact.client.LocalExecutor
import bigdataproject.scala.eu.stratosphere.ml.randomforest.buildDecisionTree

object testRandomForestOnStratosphere {
  
  def main(args: Array[String]) { 

    // Write test input to temporary directory
    val inputPath = "/home/kay/Desktop/small-trainingset"
    		
    // Output
    val outputPath = "file:///home/kay/decisionTree_output"

    val number_trees = ""+10

    println("Reading input from " + inputPath)
    println("Writing output to " + outputPath)

    val plan = new buildDecisionTree().getPlan(inputPath, outputPath, number_trees )
    val ex = new LocalExecutor()
    ex.start()
    val runtime = ex.executePlan(plan)
    println("runtime:  " + runtime)
    ex.stop();
    
    System.exit(0)
  }
}