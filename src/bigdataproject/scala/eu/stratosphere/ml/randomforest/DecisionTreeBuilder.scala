package bigdataproject.scala.eu.stratosphere.ml.randomforest

import eu.stratosphere.pact.common.plan.PlanAssembler
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription
import eu.stratosphere.scala._
import eu.stratosphere.scala.operators._
import scala.util.matching.Regex
import eu.stratosphere.pact.client.LocalExecutor
import util.Random
import scala.collection.mutable.Buffer
  
class DecisionTreeBuilder(var minNrOfItems : Int, var featureSubspaceCount : Int) extends PlanAssembler with PlanAssemblerDescription with Serializable {

  
  override def getDescription() = {
	  "Usage: [inputPath] [outputPath] ([number_trees])"
  }
  
  override def getPlan(args: String*) = {
    val inputPath = args(0)
    val inputNodeQueuePath = args(1)
    val outputNodeQueuePath = args(2)
    val outputTreePath = args(3)
    val number_trees = args(4)
    val level = args(5).toInt
    
    val trainingSet = TextFile(inputPath) 
    val inputNodeQueue= TextFile(inputNodeQueuePath)

    // INPUT: nodequeue to build
    // treeID, nodeId, baggingTable, featureSpace, features
    
    val baggingTable_nodequeue = inputNodeQueue flatMap { line =>
      val values = line.split(",")      
      val treeId = values(0).toInt
      val nodeId = values(1).toInt
      val bestSplitIndex = values(2)
      val bestSplitValue = values(3)
      val label = values(4)
      val baggingTable = values(5)
      val featureSpace = values(6)
      //val features = values(6).split(" ")
      
      //(treeId,nodeId,baggingTable,featureSpace)
      baggingTable.split(" ").map( sampleIndex => 
      		(treeId,nodeId,sampleIndex.toInt,featureSpace /*all features may missing for next round. maybe read this from tree*/ )
       )
    }
    
    // prepare the treeId,nodeId and featureList for next round
    // map to new potential nodeIds
    val nodeFeatures = inputNodeQueue flatMap { line =>
      val values = line.split(",")      
      val treeId = values(0).toInt
      val nodeId = values(1).toInt
      val features = values(7)        
      
      val leftNodeId = ((nodeId + 1) * 2) - 1
      val rightNodeId = ((nodeId + 1) * 2) 
      
      List( (treeId,leftNodeId,features), (treeId,rightNodeId,features) )
    }
    
    // prepare string input for next operations
    val samples = trainingSet map { line => 
	  val values = line.split(" ")
	  val sampleIndex = values.head.trim().toInt
	  val label = values.tail.head.trim().toInt
	  val features = values.tail.tail	  
	  	(sampleIndex, label, features.mkString(" ") )
    }

    // join
    //  bagging table
    //  with samples
    val joined = baggingTable_nodequeue
    				.join(samples)
    				.where { x => x._3 }
    				.isEqualTo { x => x._1 }
    				.map{ (nodeSample, sample) => 
    				  	(	
    				  	    nodeSample._1+"_"+nodeSample._2, /*group key: treeId_nodeId*/
    				  	    nodeSample._3 /*sampleIndex*/, 
    						sample._2, /*label*/
    						sample._3 .split(" ")
    							.zipWithIndex
    							.filter( x => nodeSample._4.split(" ").map({_.toInt}).contains(x._2.toInt)  )
    							.map({x=>""+x._2+":"+x._1}).mkString(" ")
    					) }

    val nodeResults = joined
    			.groupBy( _._1 )
    			.reduceGroup( { values =>
		    		val buckets = 10
		  			val buffered = values.buffered
		  			val keyValues = buffered.head._1
		  			val treeId = keyValues.split("_")(0).toInt
		  			val nodeId = keyValues.split("_")(1).toInt

		  			// pre process incoming data
		  			val sampleList = buffered.toList
		  						.map({ case (treeIdnodeId,sampleIndex,label,features) => (label, sampleIndex, features.split(" ").map( f => (f.split(":")(0).toInt, f.split(":")(1).toDouble) )) })
		  			
		  			// compute total sample count in this node
		  			val totalSamples = sampleList.length
  			
		  			// compute the histogram for each feature
		  			val mergedHistogram = sampleList
		  						.map({ case (label, sampleIndex, features) => features.map({ case(featureIndex,featureValue) => new Histogram(featureIndex, buckets).update(featureValue) }) })
		  						.reduceLeft( (s1, s2) => s1.zip(s2).map( x => x._1.merge(x._2) )) 
	
		  			// find some split candidates to make further evaluation
		  			val splitCandidates = mergedHistogram.map(x => (x.feature, x.uniform(buckets), x)).filter(_._2.length > 0)
		  					  			
		  			// compute split-quality for each candidate
		  			val splitQualities = splitCandidates.flatMap { 
	  			  		case (featureIndex, featureBuckets, x) =>
	  			  			featureBuckets.map( bucket => split_quality(sampleList, featureIndex, bucket, x, totalSamples))
	  				}
		    		
		    		// check the array with split qualities is not empty
		  			if(splitQualities.length > 0){
		  			  
			  			val bestSplit = splitQualities.maxBy(_._3)
			  			
			  			// create new bagging tables for the next level
			  			val leftNode = sampleList
			  							.flatMap({ case(label,sampleIndex,features) => 
			  							  				features.filter({ case (feature,value) => feature == bestSplit._1 && value <= bestSplit._2 })
			  							  				.map(x=>(sampleIndex,x._1,x._2) ) })
			  			val rightNode = sampleList
			  							.flatMap({ case(label,sampleIndex,features) => 
			  							  			features.filter({ case (feature,value) => feature == bestSplit._1 && value > bestSplit._2 })
			  							  			.map(x=>(sampleIndex,x._1,x._2)) })

			  			// decide if there is a stopping condition
			  			val stoppingCondition = leftNode.isEmpty || rightNode.isEmpty || leftNode.length < minNrOfItems || rightNode.length < minNrOfItems;
			  			
			  			System.out.println(bestSplit)
			  			System.out.println("right:"+rightNode.length)
			  			System.out.println("left:"+leftNode.length)
						
			  			// serialize based on stopping condition
			  			if (stoppingCondition)
			  			{
			  				// compute the label by max count (uniform distribution)
				  			val label = sampleList
				  					.map({ case(label,sampleIndex,sample) => (label) })
				  					.groupBy(x=>x).maxBy(x => x._2.length)._1
				  					
				  			List( (treeId, nodeId, -1 /* feature*/, 0.0 /*split*/, label, "", "", "" ) )
				  			
			  			}else{
				  			var left_nodeId   = ((nodeId + 1) * 2) - 1
				  			var right_nodeId  = ((nodeId + 1) * 2) 
	
				  			var left_baggingTable  = leftNode.map({x=>x._1}).mkString(" ")
				  			var right_baggingTable = rightNode.map({x=>x._1}).mkString(" ")
				  					
				  			
				  			//System.out.println(bestSplit)
				  			//System.out.println("leftnode: " + leftNode.length)
				  			//System.out.println("rightnode: " + rightNode.length)
				  			
							List(
								// emit the tree node
							    (treeId, nodeId, bestSplit._1, bestSplit._2, -1, "", "", "" ),
						    
								// emit new nodes for the level node queue
								(treeId, left_nodeId, -1, 0.0, -1, left_baggingTable, bestSplit._1.toString, "" ),
								(treeId, right_nodeId, -1, 0.0, -1, right_baggingTable, bestSplit._1.toString, "" ) 
							)
			  			}
			  			
					} 
		  			else {
		  				// compute the label by max count (uniform distribution)
			  			val label = sampleList
			  					.map({ case(label,sampleIndex,sample) => (label) })
			  					.groupBy(x=>x).maxBy(x => x._2.length)._1
			  			
			  			System.out.println(label)
						
			  			// emit the final tree node
						List( (treeId, nodeId, -1 /* feature*/, 0.0 /*split*/, label, "", "", "" ) )
		  			}
    			})
    			.flatMap(x=>x)
    
    // filter nodes to build, join the last featureList from node and remove :q
    val nodeResultsWithFeatures = nodeResults
								.filter({ case(treeId,nodeId,featureIndex,splitValue,label,_,_,_) => 
								  	featureIndex == -1 && label == -1 })
			    				.join(nodeFeatures)
			    				.where({ x => (x._1, x._2) })
			    				.isEqualTo { y => (y._1,y._2) }
			    				.map({ (nodeResult, nodeFeatures) => 
			    				    val features = nodeFeatures._3.split(" ").filter(x=>x.toInt != nodeResult._7.toInt ).map({_.toInt})
		    				  		val featureSpace = generateFeatureSubspace(featureSubspaceCount, features.toBuffer )

		    				  		(nodeResult._1, nodeResult._2, nodeResult._3, nodeResult._4, nodeResult._5, nodeResult._6, featureSpace.mkString(" "), features.mkString(" ") )			    				
			    				})
        
    // output to tree file if featureIndex != -1 (node) or leaf (label detected)  
	val finaTreeNodesSink = nodeResults
							.filter({ case(treeId,nodeId,featureIndex,splitValue,label,baggingTable,_,_) => 
							  	featureIndex != -1 || label != -1 })
							.write( outputTreePath, CsvOutputFormat("\n",","))

	// output nodes to build if 
	val nodeQueueSink = nodeResultsWithFeatures
							.write( outputNodeQueuePath, CsvOutputFormat("\n",","))
    	
    new ScalaPlan(Seq(finaTreeNodesSink, nodeQueueSink))
  }
  // INPUT
  // List[(Int,Array[(Int,Double)])] => sampleList with featureIndex and value 
  //							  List( (label, List(s1f1,s1f2,s1f3,..s1fN)), ... )
  // Int => feature
  // Double => split candidate
  // Histogram histogram distribution
  // OUTPUT
  // (feature,candidate,quality)
  
  def split_quality(  	sampleList : List[(Int,Int,Array[(Int,Double)])], 
		  				feature : Int,  
		  				candidate : Double, 
		  				h : Histogram,
		  				totalSamples : Int) = {  

      h.print
	  //System.out.println( sampleList .map({ case(label,sampleIndex,sample) => (label,sampleIndex,sample.filter( x => x._1 == feature).toList ) }).toList )
			  				
	  // filter feature from all samples
	  val featureList = sampleList
			  				.map({ case(label,sampleIndex,sample) => (label,sample.filter( x => x._1 == feature).head) })
	  
	  // probability for each label occurrence in the node
	  val qj = featureList
  				.groupBy( _._1) /*group by label */
  				.map( x=> (x._2.length.toDouble / totalSamples ))
	  
  	  // compute probability distribution for each child (Left,Right) and the current candidate with the specific label
  	  val qLj = featureList
  				.filter({ case(label,sample) => sample._2 <= candidate})
  				.groupBy( _._1) /*group by label */
  	  			.map( x=> (x._2.length.toDouble / totalSamples ))
  	  			
  	  
  	  val qRj = featureList
  				.filter({ case(label,sample) => sample._2 > candidate})
  				.groupBy( _._1) /*group by label */
  				.map( x=> (x._2.length.toDouble / totalSamples ))

  	  
  	  // TODO: quality_function do not use the qj List, instead use the qLj list two times
	  val tau=0.2
	  val quality=quality_function( tau, qj.toList, qLj.toList, qRj.toList );
	  
	  (feature,candidate, quality)
  }

  def impurity( q:List[Double] )={
    gini(q)
  }
  
  def gini( q:List[Double] )={
    1.0-q.map( x=>x*x).sum.toDouble
  }
  
  def entropy ( q:List[Double] )={
    - q.map( x=>x*Math.log(x)).sum
  }
  
  def quality_function(tau:Double, q : List[Double], qL : List[Double], qR : List[Double]) = {
    impurity(qL)-tau*impurity(qL)-(1-tau)*impurity(qR);
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
  
}
