package bigdataproject.scala.eu.stratosphere.ml.randomforest

import eu.stratosphere.pact.common.plan.PlanAssembler
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription
import eu.stratosphere.scala._
import eu.stratosphere.scala.operators._
import scala.util.matching.Regex
import eu.stratosphere.pact.client.LocalExecutor

import util.Random
  
class DecisionTreeBuilder( var nodeQueue : Array[TreeNode]) extends PlanAssembler with PlanAssemblerDescription with Serializable {

  
  override def getDescription() = {
	  "Usage: [inputPath] [outputPath] ([number_trees])"
  }
  
  override def getPlan(args: String*) = {
    val inputPath = args(0)
    val outputPath = args(1)
    val number_trees = args(2)
    val outputNewNodes = args(3)
    
    
    val trainingSet = TextFile(inputPath) 
    val newNodes = TextFile(outputNewNodes)
    
    // prepare string input for next operations
    val samples = trainingSet map { line => 
	  val values = line.split(" ")
	  val index = values.head.trim().toInt
	  val label = values.tail.head.trim().toInt
	  val features = values.tail.tail.mkString(" ")
	  	(index,label,features)
    }
    

    // for each sample and each tree and node, create an histogram tuple 
    val treenode_samples = samples  
    				.flatMap { case (index,label, features ) => 
    					nodeQueue
    							.filter { node => node.baggingTable.contains(index) }
		    					.map( node => 
		    					  		(	node.treeId+"_"+node.nodeId+"_"+label, 
		    					  			node.baggingTable.count( _ == index), 
		    					  			features.split(" ").zipWithIndex.filter( x => node.featureSpace.contains(x._2) ).map( f => f._1 ).mkString(" "),
		    					  			label) )
    					}
    
    val nodeClassHistograms = treenode_samples
      .groupBy { _._1 }
      .reduceGroup { values =>
        		val buckets = 10
      			val buffered = values.buffered
      			val treeAndNode = buffered.head._1
      			val treeId = treeAndNode.split("_")(0).toInt
      			val nodeId = treeAndNode.split("_")(1).toInt        
      			val label =  treeAndNode.split("_")(2).toInt
      			
      			
      			// generate class histogram
      			val classHistograms = values
      				.map ( x => {
      					// extract feature vector
      					val vec = x._3.split(" ").map( _.toDouble )
      					// generate a histogram for each vector
      					vec.map( new Histogram(buckets).update(_) )
      				})
      				.reduceLeft( (s1,s2) => s1.zip(s2).map( h => h._1.merge(h._2) ) )
      			//classHistograms.map( classHistogram => (treeId+"_"+nodeId, label, classHistogram.toString ) )
      			
      			val out = scala.collection.mutable.Buffer.empty[(String,Int, String)]
      			for(i <- 0 until classHistograms.length ) {
      			  out +=( (treeId+"_"+nodeId, label, "test" ) )
      			}
        		out
      			
      	}
      

   val nodeHistograms = nodeClassHistograms
      .groupBy { _._1  }
      .reduceGroup { values =>
      			val buffered = values.buffered
      			val treeAndNode = buffered.head._1
      			val treeId = treeAndNode.split("_")(0).toInt
      			val nodeId = treeAndNode.split("_")(1).toInt 

      			// find the best split
      			// store this in separate file
      			
      			// generate new nodes if necessary

      			// store new node
      			(nodeId,treeId,values.length)
      		}*/
      
    val sink = nodeClassHistograms.write( outputPath, CsvOutputFormat("\n",","))
    
    //val sink2 = nodeHistograms.write( outputPath, CsvOutputFormat("\n",","))

    
    new ScalaPlan(Seq(sink))
  }
  
}
