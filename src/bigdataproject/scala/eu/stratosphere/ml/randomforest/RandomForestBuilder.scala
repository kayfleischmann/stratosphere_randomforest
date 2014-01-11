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
    var baggingTable = new BaggingTable
    for( i <- 0 until number ){
      baggingTable.add( Random.nextInt(number) )
    }
    baggingTable
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
  
  def build( outputPath : String, inputFile : String, inputNodeQueue : String, outputFile : String, outputTreeFile : String, numTrees : Int) = {
    System.out.println("build started");
    var nodesQueue = Buffer[TreeNode]()
    val totalFeatureCount = getFeatureCount(inputFile)
    var featureSubspaceCount = Math.round(Math.log(totalFeatureCount).toFloat + 1);
          
    // Write test input to temporary directory
    val inputPath = inputFile; //new File(inputFile).toURI().toString()
      
    // Write test input to temporary directory
    val inputNodeQueuePath = inputNodeQueue; //new File(inputNodeQueue).toURI().toString()

    // Output
    val outputNodeQueuePath = outputFile; //new File(outputFile).toURI().toString()
    // read from the file and build the TreeNode List

    // Output
    val outputTreePath = outputTreeFile; //new File(outputTreeFile).toURI().toString()
    
    // add node to build for each tree
    val sampleCount = getSampleCount(inputFile)
    for (treeId <- 0 until numTrees ){
      // TODO: the features left is the whole set minus still used best-splits
      var features = (0 until totalFeatureCount).toArray
      
      // randomized
      var featureSubspace = DecisionTreeUtils.generateFeatureSubspace(featureSubspaceCount, totalFeatureCount)
      val randomSamples = generateRandomBaggingTable(sampleCount)
      nodesQueue += new TreeNode(treeId, 0, randomSamples, features, featureSubspace, -1, -1, -1 )
    }//for

    // write the initial nodes to file to join in the iteration
    writeNodes(nodesQueue, inputNodeQueuePath);

    // if next level, read from file which node has to be split
    // each line treeId,nodeId, featuresIndicies, baggingTable

    println("Reading input from " + inputPath)
    println("Writing node-queue output to " + outputNodeQueuePath)
    println("Writing trees output to " + outputTreePath)

    // generate plan with a distributed nodesQueue
    val ex = new LocalExecutor()
    ex.start()
    
    var nodeQueueSize=0
    var level=0
    
    // cleanup
    new File(outputTreePath).delete
    
    do {
    	val level_outputTreePath=outputTreePath+"_"+level
    	val plan = new DecisionTreeBuilder(70,featureSubspaceCount).getPlan(inputPath /*samples*/, inputNodeQueuePath, outputNodeQueuePath , level_outputTreePath, numTrees.toString, level.toString )
    	val runtime = ex.executePlan(plan)
    	println("runtime: " + runtime)

    	// delete old input node queue
    	new File(inputNodeQueuePath).delete()    	

    	// change output nodequeue to input queue
    	new File(outputNodeQueuePath).renameTo( new File(inputNodeQueuePath) )

    	
    	// check how many nodes to build
    	nodeQueueSize = Source.fromFile(inputNodeQueuePath).getLines().length    	
    	// increment for next level
    	level = level +1;
    	
   } while (nodeQueueSize>0) 
	
	// build final tree  
	// concatenate tree to single file
	val files = new File(outputPath).listFiles
				.filter(_.getName.startsWith( new File(outputTreePath).getName))
				.map({ file => (file, Source.fromFile(file).getLines())  })
				.filter( x=> !x._1.getName.equals(new File(outputTreePath).getName))
				.sortBy({x=>x._1})
				
	val fw = new FileWriter( new File(outputTreePath), false )
	for( file <- files ) {
	  fw.write( file._2.mkString("\n"))
	  fw.write( "\n" )
	}//for
	fw.close()
	
	new File(outputPath).listFiles
			.filter(_.getName.startsWith( new File(outputTreePath).getName))
			.filter( x=> !x.getName.equals(new File(outputTreePath).getName))
			.foreach( x => x.delete() )


    ex.stop();

    System.exit(0)
  }
  
  // write node-queue efficiently to file
  // line format:
  // treeID, nodeId, baggingTable, featureSpace, features
  def writeNodes( nodes : Buffer[TreeNode], outputPath : String ) {
	val fw = new FileWriter( outputPath, false)
    val newLine = System.getProperty("line.separator");
	try {
		for( i <- 0 until nodes.length ) {
		  var node = nodes(i)
		  fw.write( node.treeId+",")
		  fw.write( node.nodeId+",")
		  fw.write( node.splitFeatureIndex+",")
		  fw.write( node.splitFeatureValue+",")
		  fw.write( node.label+",")

		  node.baggingTable.getBaggingTable.foreach{ case (feature,count) => {
			  fw.write( List.fill(count)(feature).mkString(" ")+" " );
		  }}		  
		  fw.write( ",")
		  fw.write( node.featureSpace.mkString(" ")+"," )
		  fw.write( node.features.mkString(" "));
		  fw.write( newLine )
		}
	}
	finally {
	  fw.close()
	}
  }
  
}