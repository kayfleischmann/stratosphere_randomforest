package bigdataproject.scala.eu.stratosphere.ml.randomforest

import scala.util.Random
import eu.stratosphere.pact.client.LocalExecutor
import java.util.ArrayList
import java.io.File
import scala.io.Source
import scala.collection.mutable.Buffer
import java.io.FileWriter
import java.io.BufferedInputStream
import java.io.FileInputStream

class RandomForestBuilder {
  def getSampleCount(filename : String) : Int = {
	val src = io.Source.fromFile(filename)
	try {
		src.getLines.size.toInt
	} finally {
		src.close()
	}
  }
  def getFeatureCount(filename : String) : Int = {
	val src = io.Source.fromFile(filename)
	try {
		src.getLines.find(_ => true).orNull.split(" ").tail.tail.size
	} finally {
		src.close()
	}
  }
  
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
  
  def eval(inputFile : String, treeFile : String, outputFile : String) = {
    val inputPath = new File(inputFile).toURI().toString()
    val treePath = new File(treeFile).toURI().toString()
    val outputPath = new File(outputFile).toURI().toString()
    
    val ex = new LocalExecutor()
    ex.start()
	val plan = new DecisionTreeEvaluator().getPlan(inputPath, treePath, outputPath)
    val runtime = ex.executePlan(plan)
    System.exit(0)
  }
  
  def build(inputFile : String, outputFile : String, outputTreeFile : String, numTrees : Int) = {
    var nodesQueue = Buffer[TreeNode]()
    val totalFeatureCount = getFeatureCount(inputFile)
    var featureSubspaceCount = Math.round(Math.log(totalFeatureCount).toFloat + 1);
          
    // Write test input to temporary directory
    val inputPath = new File(inputFile).toURI().toString()
      
    // Output
    val outputNodeQueuePath = new File(outputFile).toURI().toString()
    // read from the file and build the TreeNode List

    // Output
    val outputTreePath = new File(outputTreeFile).toURI().toString()
    
    // add node to build for each tree
    val sampleCount = getSampleCount(inputFile)
    for (treeId <- 0 until numTrees ){
      // TODO: the features left is the whole set minus still used best-splits
      var features = (0 until totalFeatureCount).toArray
      
      // randomized
      var featureSubspace = generateFeatureSubspace(featureSubspaceCount, totalFeatureCount)
      val randomSamples = generateRandomBaggingTable(sampleCount)
      nodesQueue += new TreeNode(treeId, 0, new BaggingTable().create(randomSamples), features, featureSubspace, -1, -1, -1 )
    }//for
    
    // if next level, read from file which node has to be split
    // each line treeId,nodeId, featuresIndicies, baggingTable

    println("Reading input from " + inputPath)
    println("Writing node-queue output to " + outputNodeQueuePath)
    println("Writing trees output to " + outputTreePath)

    // generate plan with a distributed nodesQueue
    val ex = new LocalExecutor()
    ex.start()
    
    var randomForestTrees : Array[String] = Array.fill(numTrees)("")
    
    do {
    	val plan = new DecisionTreeBuilder(nodesQueue.toList, 70).getPlan(inputPath, outputNodeQueuePath, outputTreePath, numTrees.toString )
    	//plan.setDefaultParallelism(2)
    	//plan.setMaxNumberMachines(2)
    	val runtime = ex.executePlan(plan)
	    println("runtime: " + runtime)
	    
	    val previousNodeQueue = nodesQueue
	    nodesQueue = Buffer[TreeNode]()
	    for(line <- Source.fromFile(outputFile).getLines())
	    {
	    	val lineData = line.split(",")
		    val treeId = lineData(0).toInt
	    	val label = lineData(4).toInt
	    	
	    	if (randomForestTrees(treeId).length > 0)
    			randomForestTrees(treeId) += ";"
	    	randomForestTrees(treeId) += lineData.take(5).mkString(",")
	    	
	    	// if not a leaf node, add new nodes to split
	    	if (label == -1)
	    	{
		    	val nodeId = lineData(1).toInt
		    	val featureIndex = lineData(2).toInt
		    	val featureValue = lineData(3).toDouble
	    		val parentNode = previousNodeQueue.find(x => x.treeId == treeId && x.nodeId == nodeId).get
		    	val leftBaggingTable = lineData(5).split(" ").map(_.toInt).toArray
		    	val rightBaggingTable = lineData(6).split(" ").map(_.toInt).toArray
	    		val features = parentNode.features.filter(x => x != featureIndex)

	    		nodesQueue += new TreeNode(treeId, ((nodeId + 1) * 2) - 1,  new BaggingTable().create(leftBaggingTable), features, generateFeatureSubspace(featureSubspaceCount, features.toBuffer), -1, -1, -1 )
		    	nodesQueue += new TreeNode(treeId, ((nodeId + 1) * 2),  new BaggingTable().create(rightBaggingTable), features, generateFeatureSubspace(featureSubspaceCount, features.toBuffer), -1, -1, -1 )
	    	}
	    }
    } while (!nodesQueue.isEmpty)    
	      
	val fw = new FileWriter(outputTreeFile, false)
    val newLine = System.getProperty("line.separator");
	try 
	{
		for (i <- 0 until numTrees)
		{
			fw.write(randomForestTrees(i) + newLine)
		}
	}
	finally {
	  fw.close()
	}
    ex.stop();
    
    System.exit(0)
  }
}