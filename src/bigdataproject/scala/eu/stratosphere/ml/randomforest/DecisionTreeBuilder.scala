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
		    					  			features(feature).toDouble, // feature value
		    					  			label,
		    					  			index)
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

      			val tupleList = buffered.toArray
      			
      			// group by feature results in a List[(feature,List[inputTuple])]
      			val groupedFeatureTuples = tupleList.groupBy( _._3 )
 
      			val featureHistograms = groupedFeatureTuples.map( x => x._2.map ( t => new Histogram(t._3,buckets).update( t._4.toDouble ) ) )
      			
      			// merged histogram h(i) (merge all label histpgrams to one total hostogram)
      			val mergedHistogram = featureHistograms
      				.map( x => x.reduceLeft( (h1,h2) => h1.merge(h2) ) )
      			
      			// compute split candidate for each feature
      			// List[Histogram(feature)] => [(feature,List[a])]
      			val splitCandidates = mergedHistogram.map( x => Histogram.fromString(x.toString).uniform(buckets) )
      			
      			// compute the best split
      			val bestSplit = (3 /*feature*/,  22 /*a*/)

  				// prob that sample reaches v
  				val tau = 1
  				
  				// the probabilities of label j in the left
  				val qLj = 1

  				// the probabilities of label j in the Right
  				val qRj = 1
  				
      			
      			val label = tupleList.groupBy( _._5 ).maxBy(x=>x._2.length )
      			// decide if there is a stopping condition
      			
      			// if yes, assign label to the node.
      			// group by label
      			
      			// cerate new bagging tables for the next level
      			val leftNode = buffered.filter( x => x._3 == bestSplit._1 && x._4 <= bestSplit._2).map( x => x._6)
      			val rightNode = buffered.filter( x => x._3 == bestSplit._1 && x._4 > bestSplit._2).map( x => x._6)
      		
      			// now do the splitting
      			val leftChild =  leftNode.mkString(" ")
      			val rightChild = rightNode.mkString(" ")

  				// emit new node for nodeQueue
  				(treeId, nodeId, bestSplit._1, bestSplit._2, label, leftChild, rightChild )
      	}
      
      
    val sink = newQueueNodesHistograms.write( outputPath, CsvOutputFormat("\n",","))

    new ScalaPlan(Seq(sink))
  }

  def isStoppingCriterion(){
  }
}
