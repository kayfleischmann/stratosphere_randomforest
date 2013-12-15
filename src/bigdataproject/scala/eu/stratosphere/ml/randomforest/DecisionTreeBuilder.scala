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
      			val total_samples = tupleList.length
      			
      			// group by feature results in a List[(feature,List[inputTuple])]
      			val groupedFeatureTuples = tupleList.groupBy( _._3 )
 
      			val featureHistograms = groupedFeatureTuples.map( x => x._2.map ( t => new Histogram(t._3,buckets).update( t._4.toDouble ) ) )
      			
      			// merged histogram h(i) (merge all label histpgrams to one total hostogram)
      			val mergedHistogram = featureHistograms
      				.map( x => x.reduceLeft( (h1,h2) => h1.merge(h2) ) )
      			
      			// compute split candidate for each feature
      			// List[Histogram(feature)] => [(feature,List[a])]
      			val splitCandidates = mergedHistogram.map( x => (x.feature,x,x.uniform(buckets)) )
      			
      			//val splitting_qualities = splitCandidates.map( x => split_quality(x)  )
      			val qj = groupedFeatureTuples.map( f => (f._1, f._2.length.toDouble / total_samples ) )
      			

      			// compute all possible split qualities to make the best decision
      			val splitQualities = splitCandidates.flatMap( featureCandidates =>
      			  							featureCandidates._3.map( candidate => split_quality( featureCandidates._1, candidate, featureCandidates._2) ) )
      			  					
      			  					
      			// compute the best split, find max by quality
      			// OUTPUT
      			// (feature,candidate,quality)
      			val bestSplit = splitQualities.maxBy( x=> x._3  )
      			  				
      			// compute the label by max count (uniform distribution)
      			val label =  tupleList.groupBy( _._5 ).maxBy(x=>x._2.length )._1
      			
      			// decide if there is a stopping condition
      			
      			// if yes, assign label to the node.
      			// group by label
      			
      			// create new bagging tables for the next level
      			val leftNode = tupleList.filter( x => x._3 == bestSplit._1 && x._4 <= bestSplit._2).map( x => x._6)
      			val rightNode = tupleList.filter( x => x._3 == bestSplit._1 && x._4 > bestSplit._2).map( x => x._6)
      		
      			// now do the splitting
      			val leftChild =  leftNode.mkString(" ")
      			val rightChild = rightNode.mkString(" ")

  				// emit new node for nodeQueue
  				(treeId, nodeId, bestSplit._1, bestSplit._2, label, leftChild, rightChild )
      	}
      
      
    val sink = newQueueNodesHistograms.write( outputPath, CsvOutputFormat("\n",","))

    new ScalaPlan(Seq(sink))
  }

  def isStoppingCriterion (groupedFeatureTuples : List[Int] ) ={
1 }
  
  def impurity_gini( q:List[Double] )={Double
    1-q.map( x=>x*x).sum
  }
  
  def entropy ( q:List[Double] )={
    - q.map( x=>x*Math.log(x)).sum
  }
  
  def quality_function(tau:Double, q : List[Double], qL : List[Double], qR : List[Double]) ={
    impurity_gini(q) -tau*impurity_gini(qL)-(1-tau)*impurity_gini(qR);
  }

  // INPUT
  // Integer => feature
  // Double => split candidate
  // Histogram histogram distribution
  // OUTPUT
  // (feature,candidate,quality)
  
  def split_quality( feature : Integer,  candidate : Double, histogram : Histogram ) = {  
	val quality=0
	
    (feature,candidate,quality)
  }
  
}
