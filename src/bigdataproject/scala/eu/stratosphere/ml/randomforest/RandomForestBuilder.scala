package bigdataproject.scala.eu.stratosphere.ml.randomforest

import scala.util.Random
import eu.stratosphere.pact.client.LocalExecutor


class RandomForestBuilder {
  def getSampleCount() = 10000
  
  def generateRandomBaggingTable(number : Int)  = {
    Array.fill(number) { Random.nextInt(number-1) }
  }

  
  def build = {
    val numTrees = 100
    val nodesQueue = new Array[TreeNode](numTrees)
    val totalFeatureCount = 784 

    // add node to build for each tree
    for (tree <- 0 to numTrees-1 ){
      nodesQueue(tree)=( new TreeNode(tree,0,generateRandomBaggingTable(getSampleCount), (0 until totalFeatureCount).toSet, null, -1, false ) )
    }//for
    
    // if next level, read from file which node has to be split
    // each line treeId,nodeId, featuresIndicies, baggingTable
          
    // Write test input to temporary directory
    val inputPath = "/home/kay/Desktop/small-trainingset"
      
    // Output
    val outputPath = "file:///home/kay/decisionTree_output"


    println("Reading input from " + inputPath)
    println("Writing output to " + outputPath)

    // distribute the nodesQueue 
    val plan = new DecisionTreeBuilder(nodesQueue).getPlan(inputPath, outputPath, ""+numTrees )

    val ex = new LocalExecutor()
    ex.start()
    
    val runtime = ex.executePlan(plan)
    println("runtime:  " + runtime)
    ex.stop();
    
    System.exit(0)
    
  }
}