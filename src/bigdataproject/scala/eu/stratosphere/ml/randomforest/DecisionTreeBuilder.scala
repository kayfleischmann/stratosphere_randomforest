package bigdataproject.scala.eu.stratosphere.ml.randomforest

import eu.stratosphere.pact.common.plan.PlanAssembler
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription
import eu.stratosphere.scala._
import eu.stratosphere.scala.operators._
import scala.util.matching.Regex
import eu.stratosphere.pact.client.LocalExecutor

import util.Random
  
class DecisionTreeBuilder( var nodeQueue : List[TreeNode]) extends PlanAssembler with PlanAssemblerDescription with Serializable {

  
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
		    					.flatMap( node => 
		    					  // for each feature in node-featureSpace and 
		    					  	features.split(" ").zipWithIndex.filter( f => node.featureSpace.contains(f._2) ).map( feature =>  
		    					  	  	(	node.treeId+"_"+node.nodeId+"_"+label+"_"+feature._2, 
		    					  			node.baggingTable.count( _ == index), 
		    					  			feature._1, // feature value
		    					  			feature._2, // feature index
		    					  			label )
		    					  			) )	
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
      			val feature =  treeAndNode.split("_")(3).toInt
      			
      			
      			// generate class histogram
      			val classFeatureHistograms = buffered
      				.map ( x => new Histogram(buckets).update( x._3.toDouble ) )
      				.reduceLeft( (h1,h2) => h1.merge(h2) )
      				
      			(treeId+"_"+nodeId+"", label, feature, classFeatureHistograms.toString )
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
      		}
      
    val sink = nodeClassHistograms.write( outputPath, CsvOutputFormat("\n",","))
    
    //val sink2 = nodeHistograms.write( outputPath, CsvOutputFormat("\n",","))

    
    new ScalaPlan(Seq(sink))
  }
  
}
