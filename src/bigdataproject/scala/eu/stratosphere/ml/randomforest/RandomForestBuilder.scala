package bigdataproject.scala.eu.stratosphere.ml.randomforest

import scala.util.Random
import eu.stratosphere.pact.client.LocalExecutor
import java.util.ArrayList
import java.io.File
import scala.io.Source
import scala.collection.mutable.Buffer
import java.io.FileWriter

class RandomForestBuilder {
  def getSampleCount() = 10000
  
  def generateRandomBaggingTable(number : Int)  = {
	Array.fill(number) { Random.nextInt(number) }
  }

  def generateFeatureSubspace(randomCount : Int, maxRandomNumber : Int) : Array[Int] = {
	var features = Buffer[Int]();
	// Generate an arrayList of all Integers
	for(i <- 0 until maxRandomNumber){
	    features += i;
	}
	generateFeatureSubspace(randomCount, features)
  }
  
  def generateFeatureSubspace(randomCount : Int, features : Buffer[Int]) : Array[Int] = {
    var arr : Array[Int] = Array()
	arr = Array(randomCount)
	arr = Array.fill(randomCount)(0)
	for(i <- 0 until randomCount)
	{
	    var random = new Random().nextInt(features.length);
	    arr(i)=features.remove(random);
	}
	arr;
  }
  
  def build = {
    val numTrees = 10
    var nodesQueue = Buffer[TreeNode]()
    val totalFeatureCount = 784 //TODO: find the amount of number of feature automatically
    var featureSubspaceCount = Math.round(Math.log(totalFeatureCount).toFloat + 1);

    // add node to build for each tree
    for (treeId <- 0 until numTrees ){
      // TODO: the features left is the whole set minus still used best-splits
      var features = (0 until totalFeatureCount).toArray
      
      // randomized
      var featureSubspace = generateFeatureSubspace(featureSubspaceCount, totalFeatureCount)
      
      nodesQueue += new TreeNode(treeId, 0, generateRandomBaggingTable(getSampleCount), features, featureSubspace, -1, -1, -1 )
    }//for
    
    // if next level, read from file which node has to be split
    // each line treeId,nodeId, featuresIndicies, baggingTable
          
    // Write test input to temporary directory
    val inputPath = new File("C:\\Projects\\StratosphereRandomForest\\Prototype1\\normalized.txt").toURI().toString()
      
    // Output
    val outputFile = "C:\\Users\\Silver\\AppData\\Local\\Temp\\output"
    val outputNodeQueuePath = new File(outputFile).toURI().toString()
    // read from the file and build the TreeNode List

    // Output
    val outputTreeFile = "C:\\Users\\Silver\\AppData\\Local\\Temp\\outputTree"
    val outputTreePath = new File(outputTreeFile).toURI().toString()

    println("Reading input from " + inputPath)
    println("Writing node-queue output to " + outputNodeQueuePath)
    println("Writing trees output to " + outputTreePath)

    // generate plan with a distributed nodesQueue 
    
    val ex = new LocalExecutor()
    ex.start()
    
	val fw = new FileWriter(outputTreeFile, false)
    val newLine = System.getProperty("line.separator");
	try 
	{
	    do {
	    	val plan = new DecisionTreeBuilder(nodesQueue.toList, 70).getPlan(inputPath, outputNodeQueuePath, outputTreePath, numTrees.toString )
		    val runtime = ex.executePlan(plan)
		    println("runtime: " + runtime)
		    
		    val previousNodeQueue = nodesQueue
		    nodesQueue = Buffer[TreeNode]()
		    for(line <- Source.fromFile(outputFile).getLines())
		    {
		    	fw.write(line + newLine)
		    	val lineData = line.split(",")
		    	val treeId = lineData(0).toInt
		    	val nodeId = lineData(1).toInt
		    	val featureIndex = lineData(2).toInt
		    	val featureValue = lineData(3).toDouble
		    	val label = lineData(4).toInt
		    	val leftBaggingTable = lineData(5).split(" ").map(_.toInt).toArray
		    	val rightBaggingTable = lineData(5).split(" ").map(_.toInt).toArray
		    	val parentNode = previousNodeQueue.find(x => x.treeId == treeId && x.nodeId == nodeId).get
		    	
		    	// if not a leaf node, add new nodes to split
		    	if (label == -1)
		    	{
		    		val features = parentNode.features.filter(x => x != featureIndex)
			    	nodesQueue += new TreeNode(treeId, ((nodeId + 1) * 2) - 1, leftBaggingTable, features, generateFeatureSubspace(featureSubspaceCount, features.toBuffer), -1, -1, -1 )
			    	nodesQueue += new TreeNode(treeId, ((nodeId + 1) * 2), rightBaggingTable, features, generateFeatureSubspace(featureSubspaceCount, features.toBuffer), -1, -1, -1 )
		    	}	    	
		    }
	    } while (!nodesQueue.isEmpty)    
	}
	finally {
	  fw.close()
	}
    
    ex.stop();
    
    System.exit(0)
    
  }
}