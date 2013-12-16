package bigdataproject.scala.eu.stratosphere.ml.randomforest

import eu.stratosphere.pact.common.plan.PlanAssembler
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription
import eu.stratosphere.scala._
import eu.stratosphere.scala.operators._
import scala.util.matching.Regex
import eu.stratosphere.pact.client.LocalExecutor

import util.Random
  
class DecisionTreeBuilder(var nodeQueue : List[TreeNode], var minNrOfItems : Int) extends PlanAssembler with PlanAssemblerDescription with Serializable {

  
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
						(node.treeId+"_"+node.nodeId,
			  			node.baggingTable.count( _ == index), 
			  			feature, // feature attribute index
			  			features(feature).toDouble, // feature value
			  			label,
			  			index)
			  			)
			  		)
			}
    
    val newQueueNodesHistograms = treenode_samples
      .groupBy { _._1 }
      .reduceGroup { values =>
    		val buckets = 10
  			val buffered = values.buffered
  			val keyValues = buffered.head._1
  			val treeId = keyValues.split("_")(0).toInt
  			val nodeId = keyValues.split("_")(1).toInt        


      		// account for EACH occurance in bagging table
  			val tupleList = buffered.flatMap(x => (0 until x._2).map( _ => x )).toList
  			
  			
      		// group by feature results in a List[(feature,List[inputTuple])]
      		val groupedFeatureTuples = tupleList.groupBy( _._3 ).toList
  			val totalSamples = groupedFeatureTuples.head._2.length

  			val featureHistograms = groupedFeatureTuples map {
    		  	case (_, featureHistogram) => featureHistogram map {
					case (_, _, featureIndex, featureValue, _, _) => new Histogram(featureIndex, buckets).update(featureValue.toDouble)
  					}
    			}
      			
      			// merged histogram h(i) (merge all label histograms to one total histogram)
  			val mergedHistogram = featureHistograms.map(x => x.reduceLeft((h1, h2) => h1.merge(h2)))
  			
  			// compute split candidate for each feature
  			// List[Histogram(feature)] => [(feature,List[Tuples])]
  			// filter Histogram uniform results which are not valid
  			val splitCandidates = mergedHistogram.map(x => (x.feature, x.uniform(buckets))).filter(_._2.length > 0)
  			
  			// group by label, then compute the probability for each label
  			val qj = tupleList.groupBy( _._5 ).map {
  				case (label, dataItemsForLabel) => (label, dataItemsForLabel.length.toDouble / totalSamples)
  				}.toList
  			
  			// compute all possible split qualities to make the best decision
  			val splitQualities = splitCandidates.flatMap { case (featureIndex, featureBuckets) =>
  				featureBuckets.map(bucket => split_quality( qj, groupedFeatureTuples, featureIndex, bucket, totalSamples))
  				}  
  				
  			System.out.println(impurity_gini(qj.map(_._2)))
  	
  			// compute the best split, find max by quality
  			// OUTPUT
  			// (feature,candidate,quality)
  			val bestSplit = splitQualities.maxBy(_._3)
  			var label = -1
  			  				
  			// create new bagging tables for the next level
  			val leftNode = tupleList.filter(x => x._3 == bestSplit._1 && x._4 <= bestSplit._2).map(x => x._6)
  			val rightNode = tupleList.filter(x => x._3 == bestSplit._1 && x._4 > bestSplit._2).map(x => x._6)
  			
  			// decide if there is a stopping condition
  			val stoppingCondition = leftNode.isEmpty || rightNode.isEmpty || leftNode.length < minNrOfItems || rightNode.length < minNrOfItems;
  			
  			// if yes, 
  			if (stoppingCondition)
  			{
  				// compute the label by max count (uniform distribution)
  				label = tupleList.groupBy(_._5).maxBy(x => x._2.length)._1
  			}

  			System.out.println(bestSplit)
  			System.out.println("leftnode: "+leftNode.length)
  			System.out.println("rightnode: "+rightNode.length)
  			
  			// now do the splitting
  			val leftChild = leftNode.mkString(" ")
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
    1.0-q.map( x=>x*x).sum.toDouble
  }
  
  def entropy ( q:List[Double] )={
    - q.map( x=>x*Math.log(x)).sum
  }
  
  def quality_function(tau:Double, q : List[Double], qL : List[Double], qR : List[Double]) = {
    impurity_gini(q) -tau*impurity_gini(qL)-(1-tau)*impurity_gini(qR);
  }

  // INPUT
  // qj => probability for each label occurrence in the node
  // groupedFeatureTuples List[(feature,List[input-data])]
  // Int => feature
  // Double => split candidate
  // Histogram histogram distribution
  // OUTPUT
  // (feature,candidate,quality)
  
  
  def split_quality( 	qj:List[(Int,Double)], 
		  				groupedFeatureTuples : List[(Int,List[(String,Int,Int,Double,Int,Int)])], 
		  				feature : Int,  
		  				candidate : Double, 
		  				totalSamples : Int) = {  
    
	// compute probability distribution for each child (Left,Right) and the current candidate with the specific label
    val qLj = groupedFeatureTuples.filter(x=>x._1 == feature).map({ case (feature, tuples) => (feature, tuples.filter( x=> x._4 <= candidate ).groupBy(_._2).map(x => (x._1, x._2.length.toDouble / totalSamples )) ) })
    val qRj = groupedFeatureTuples.filter(x=>x._1 == feature).map({ case (feature, tuples) => (feature, tuples.filter( x=> x._4 > candidate ).groupBy(_._2).map(x => (x._1, x._2.length.toDouble / totalSamples )) ) })

	val tau=0.5
	val quality=quality_function( tau, qj.map(_._2), qLj.flatMap(_._2).map( _._2), qRj.flatMap(_._2).map( _._2) );
	
    (feature,candidate,quality)
  }
  
}
