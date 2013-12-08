package bigdataproject.scala.eu.stratosphere.ml.randomforest

import scala.util.Random
import eu.stratosphere.pact.client.LocalExecutor
import java.util.ArrayList

class RandomForestBuilder {
  def getSampleCount() = 10000
  
  def generateRandomBaggingTable(number : Int)  = {
    Array.fill(number) { Random.nextInt(number-1) }
  }

  def generateFeatureSubspace(randomCount : Int, maxRandomNumber : Int) : Array[Int] = {
    var arrayList = new ArrayList[Int]();
    // Generate an arrayList of all Integers
    for(i <- 0 until maxRandomNumber){
        arrayList.add(i);
    }
    var arr : Array[Int] = Array()
    arr = Array(randomCount)
    arr = Array.fill(randomCount)(0)
    for(i <- 0 until randomCount)
    {
        var random = new Random().nextInt(arrayList.size());
        arr(i)=arrayList.remove(random);
    }
    arr;
  }
  def build = {
    val numTrees = 5
    val nodesQueue = scala.collection.mutable.Buffer[TreeNode]()
    
    val totalFeatureCount = 784 
    val numberSplitCondidates = 2
    var featureSubspaceCount = math.round(math.log(totalFeatureCount).toFloat + 1);
    
    // add node to build for each tree
    for (tree <- 0 to numTrees-1 ){
      var features = (0 until totalFeatureCount).toSet
      var featureSubspace = generateFeatureSubspace(featureSubspaceCount, totalFeatureCount)

      nodesQueue +=( new TreeNode(tree,0,generateRandomBaggingTable(getSampleCount), features, null, featureSubspace,-1, false ) )
    }//for
    
    // if next level, read from file which node has to be split
    // each line treeId,nodeId, featuresIndicies, baggingTable
          
    // Write test input to temporary directory
    val inputPath = "/home/kay/Desktop/small-trainingset"
      
    // Output
    val outputNodeQueuePath = "file:///home/kay/stratosphere_rf_node_queue"

    // Output
    val outputTreePath = "file:///home/kay/stratosphere_rf_tree"

      
    println("Reading input from " + inputPath)
    println("Writing node-queue output to " + outputNodeQueuePath)
    println("Writing trees output to " + outputTreePath)

    // distribute the nodesQueue 
    val plan = new DecisionTreeBuilder(nodesQueue.toList).getPlan(inputPath, outputNodeQueuePath, outputTreePath, ""+numTrees )

    val ex = new LocalExecutor()
    ex.start()
    
    val runtime = ex.executePlan(plan)
    println("runtime:  " + runtime)
    ex.stop();
    
    System.exit(0)
    
  }
}