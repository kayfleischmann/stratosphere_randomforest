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
	  	(index,label,features.split(" "))
    }
    
    // for each sample and each tree and node, create an histogram tuple 
    val treenode_samples = samples  
    				.flatMap { case (index,label, features ) => 
    					nodeQueue
    							.filter { node => node.baggingTable.contains(index) }
		    					.flatMap( node => 
		    					    // filter feature space for this node, and ignore the other features
		    					   	node.featureSpace.map( feature =>
		    					  	  	(	node.treeId+"_"+node.nodeId,
		    					  			node.baggingTable.count( _ == index), 
		    					  			feature, // feature attribute index
		    					  			features(feature), // feature value
		    					  			label )
		    					  			) )	
    					}
    
    val newQueueNodesHistograms = treenode_samples
      .groupBy { _._1 }
      .reduceGroup { values =>
        		val buckets = 10
      			val buffered = values.buffered
      			val keyValues = buffered.head._1
      			val treeId = keyValues.split("_")(0).toInt
      			val nodeId = keyValues.split("_")(1).toInt        

      			val groupedFeatureTuples = buffered.toArray.groupBy( _._3 )
      			val featureHistograms = groupedFeatureTuples.map( x => x._2.map ( t => new Histogram(t._3,buckets).update( t._4.toDouble ) ) )
      			
      			// generate class histogram
      			val mergedHistogram = featureHistograms
      				.map( x => x.reduceLeft( (h1,h2) => h1.merge(h2) ) )
      	
      			// now do the splitting
      			val leftChild = ""
      			val rightChild = ""
      			  
  				// prob that sample reaches v
  				val tau = 1
  				
  				// the probabilities of label j in the left
  				val qLj = 1

  				// the probabilities of label j in the Right
  				val qRj = 1

  				// emit new node for nodeQueue
      			(treeId, nodeId, leftChild, rightChild )
      	}
      
      
    val sink = newQueueNodesHistograms.write( outputPath, CsvOutputFormat("\n",","))

    new ScalaPlan(Seq(sink))
  }

  def isSplitNode(){
  }
}
