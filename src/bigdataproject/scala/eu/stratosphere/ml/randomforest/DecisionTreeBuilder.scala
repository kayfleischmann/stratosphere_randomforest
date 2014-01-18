package bigdataproject.scala.eu.stratosphere.ml.randomforest

import eu.stratosphere.client.LocalExecutor
import eu.stratosphere.api.common.Plan
import eu.stratosphere.api.common.Program
import eu.stratosphere.api.common.ProgramDescription
import eu.stratosphere.api.scala._
import eu.stratosphere.api.scala.operators._

import scala.util.matching.Regex
import util.Random
import scala.collection.mutable.Buffer


class DecisionTreeBuilder(var minNrOfItems: Int, var featureSubspaceCount: Int, var numClasses : Int, var treeLevel : Int ) extends Program with ProgramDescription with Serializable {

	override def getDescription() = {
		"Usage: [inputPath] [outputPath] ([number_trees])"
	}

	override def getPlan(args: String*) = {
		val inputPath = args(0)
		val inputNodeQueuePath = args(1)
		val outputNodeQueuePath = args(2)
		val outputTreePath = args(3)
		val number_trees = args(4)

		val trainingSet = TextFile(inputPath)
		val inputNodeQueue = TextFile(inputNodeQueuePath)

		val newLine = System.getProperty("line.separator");

		

		val nodequeue = inputNodeQueue map { line =>
			val values = line.split(",")
			val treeId = values(0).toInt
			val nodeId = values(1).toInt
			val bestSplitIndex = values(2)
			val bestSplitValue = values(3)
			val label = values(4)
			val baggingTable = values(5)
			val featureSpace = values(6)
			
			(treeId, nodeId, baggingTable.trim, featureSpace.trim )
		}

		val samples = trainingSet map { line =>
			val values = line.split(" ")
			val sampleIndex = values.head.trim().toInt
			val label = values.tail.head.trim().toInt
			val features = values.tail.tail
			(sampleIndex, label, features.map(_.toDouble))
		}

		val nodesAndSamples = nodequeue.flatMap { case(treeId,nodeId,baggingTable,featureSpace) =>
				baggingTable
					.split(" ")
					.map(sampleIndex => (treeId, nodeId, sampleIndex.toInt, featureSpace.split(" ").map(_.toInt), 1))
			}
			.join(samples)
			.where { x => x._3 }
			.isEqualTo { x => x._1 }
			.map {(node, sample) =>
				(
				node._1, //treeId
				node._2, //nodeid
				sample._1, //sampleIndex
				sample._2, //label
				sample._3.zipWithIndex.filter(x => node._4.contains(x._2)), //features
				node._5 //count
				)
			}

	
		val nodeSampleFeatures  = nodesAndSamples
				.flatMap  { case(treeId,nodeId,sampleIndex,label,features,count) =>
					features.map({ case(featureValue,featureIndex) => 
					  
					  	( treeId,nodeId, featureIndex, sampleIndex, label, featureValue, 1 )})
				}
										
		val nodeSampleFeatureHistograms = nodeSampleFeatures
							.map({ case ( treeId,nodeId,featureIndex,sampleIndex,label,featureValue, count) =>
									( treeId,nodeId,featureIndex,new Histogram(featureIndex, 10).update(featureValue) ) 
									})
								

		val  nodeHistograms = nodeSampleFeatureHistograms
							.groupBy({ x => (x._1,x._2,x._3)})
							.reduce( {(left,right) => (left._1, left._2, left._3, left._4.merge(right._4)) } )
									
							
		val nodestoBuildFeatures = nodequeue
										.flatMap({ case(treeId,nodeId,baggingTable,featureSpace) => 
										  			featureSpace.split(" ").map({ feature=> 
										  			  		(treeId, nodeId, feature.toInt ) }) })
		
									
		val nodeFeatureDistributions = nodeHistograms
									.flatMap( { nodeHistogram => 
											nodeHistogram._4.uniform(10)
															.map({ splitCandidate => 
															  		( 	nodeHistogram._1, /*treeId */
															  			nodeHistogram._2, /*nodeId*/
															  			nodeHistogram._3, /*featureId*/
															  			splitCandidate) }) 
									})
									.join(nodeSampleFeatures)
									.where( { x => (x._1,x._2,x._3) })
									.isEqualTo { x => (x._1,x._2,x._3) }
									.map({ (nodeHistogram, nodeSampleFeature) =>
									  	(
									  		nodeHistogram._1, /*treeId */
									  		nodeHistogram._2, /*nodeId */
									  		nodeHistogram._3, /*featureId */
									  		nodeHistogram._4  /*splitCandidate*/,
									  	    nodeSampleFeature._4 /*sampleIndex*/,
									  		nodeSampleFeature._6 /*featureValue*/, 
									  		nodeSampleFeature._5 /*label*/, 
									  		nodeSampleFeature._7 /*sampleCount*/  )
									})
									

		// compute node distributions in a distributed fashion
									
		val nodeFeatureDistributions_qj = nodeSampleFeatures
  									.map({ x => (x._1, x._2, x._3, createLabelArray(numClasses, List( (x._5/*label*/, x._7 /*count*/ )))) })  									
 									.groupBy({x=>(x._1,x._2,x._3)})
									.reduce({(left,right)=>(left._1,left._2,left._3, left._4.zip(right._4).map(x=>x._1+x._2)) })
									.map({ x => (x._1, x._2, x._3, x._4.toList.mkString(" ") ) })

  		val nodeFeatureDistributions_qjL_samples = nodeFeatureDistributions
  									.filter({x=> x._6 <= x._4})
									
  		val nodeFeatureDistributions_qjR_samples = nodeFeatureDistributions
  									.filter({x=> x._6 > x._4})
  									
  									
  									
  									
  		val nodeFeatureDistributions_qjL = nodeFeatureDistributions
  									.filter({x=> x._6 <= x._4})
  									.map({ x => (x._1, x._2, x._3, x._4, createLabelArray(numClasses, List( (x._7/*label*/, x._8 /*count*/ )))) })
 									.groupBy({x=>(x._1,x._2,x._3,x._4)})
									.reduce({(left,right)=>(left._1,left._2,left._3,left._4, left._5.zip(right._5).map(x=>x._1+x._2)) })
									.map({ x => (x._1, x._2, x._3,x._4, x._5.toList.mkString(" ") ) })

  		val nodeFeatureDistributions_qjR = nodeFeatureDistributions
  									.filter({x=> x._6 > x._4})
  									.map({ x => (x._1, x._2, x._3, x._4, createLabelArray(numClasses, List( (x._7/*label*/, x._8 /*count*/ )))) })
 									.groupBy({x=>(x._1,x._2,x._3,x._4)})
									.reduce({(left,right)=>(left._1,left._2,left._3,left._4, left._5.zip(right._5).map(x=>x._1+x._2)) })
									.map({ x => (x._1, x._2, x._3,x._4, x._5.toList.mkString(" ") ) })

									
		// join all nodes in the queue with the corresponding tree-node-feature distributions
		// compute split qualities:q
		val nodeDistributions = nodeFeatureDistributions_qj 
		  						.cogroup(nodeFeatureDistributions_qjL)
								.where( { x => (x._1,x._2, x._3) })
								.isEqualTo { x =>  (x._1,x._2, x._3)}
								.flatMap({ (qj, qjL) => 
								      var node1 = qj.next
									  var empty_node : (Int,Int,Int,Double,String) =  (0, 0, 0, 0.0, "null")
									  if( qjL.hasNext ){
									    qjL.map({ node  =>
									      	(node1._1, node1._2, node1._3, node._4,  node1,  node )
									      })
									  }
									  else
										List( (node1._1, node1._2, node1._3, -1.0,  node1, empty_node ) )
									})	
		  						.cogroup(nodeFeatureDistributions_qjR)
								.where( { x => (x._1,x._2, x._3, x._4) })
								.isEqualTo { x =>  (x._1,x._2, x._3, x._4)}
								.map({ (qjqjL, qjR) => 
								  var node12 = qjqjL.next
								  var node3 : (Int,Int,Int,Double,String) = (0, 0, 0, 0.0, "null")
								  if( qjR.hasNext )
								     node3 = qjR.next
								  (node12._1, node12._2, node12._3, node12._4,  node12._5, node12._6, node3 )
								})
								.map({ case (treeId,nodeId,featureIndex,splitCandidate, qj, qjL, qjR ) =>
									System.out.println("------------------------------------")
								  
									var p_qj = qj._4.split(" ").map(_.toDouble )
									var p_qjL = Array[Double]()
									var p_qjR = Array[Double]()

									if(qjL._5!="null")
										p_qjL = qjL._5.split(" ").map(_.toDouble)
									if(qjR._5!="null")
										p_qjR = qjR._5.split(" ").map(_.toDouble)
									
									val tau = 0.5
									
									System.out.println("p_qj:"+p_qj.toList)
									System.out.println("p_qjL:"+p_qjL.toList)
									System.out.println("p_qjR:"+p_qjR.toList)
									
									val totalSamples = p_qj.sum.toInt
									val totalSamplesLeft = p_qjL.sum.toInt
									val totalSamplesRight = p_qjR.sum.toInt
									val bestLabel = p_qj.zipWithIndex.maxBy(_._1)._2
									val bestLabelProbability = p_qj(bestLabel) / totalSamples.toDouble;
									
									var quality = quality_function( tau, 
																	p_qj.map( _ /totalSamples).toList, 
																	p_qjL.map( _ /totalSamples).toList, 
																	p_qjR.map(_/totalSamples).toList);
									
									System.out.println("totalsamples:"+totalSamples)
									System.out.println("totalsamplesleft:"+totalSamplesLeft)
									System.out.println("totalsamplesright:"+totalSamplesRight)
									System.out.println("bestlabel:"+bestLabel)
									System.out.println("bestlabelprobability:"+bestLabelProbability)
									System.out.println("quality:"+quality)
									System.out.println("featureIndex:"+featureIndex)
									System.out.println("splitCandidate:"+splitCandidate)
									
									(treeId,nodeId,(featureIndex,splitCandidate, quality, totalSamplesLeft, totalSamplesRight, bestLabel, bestLabelProbability) ) 									
								});
				
		// compute the best split for each tree-node
		val bestTreeNodeSplits = nodeDistributions
								// group by treeIdnodeIdFeatureIndex and compute the max (best quality)
								.groupBy({x=>(x._1, x._2)})
								.reduce({ (left,right) =>  
								  				System.out.println(left)
								  				System.out.println(right)
								  				val bestSplit = 
								  				  		// if right one has a bigger quality, an is not a unvalid quality (determined by featureId!=-1 and splitcandidate != -1 )
								  				  		if(right._3._3 > left._3._3 && (right._3._1 != -1 && right._3._2 != -1.0 ))  
								  				  		  right._3
								  				  		// if left one has invalid values, just assign right one
								  				  		else if (left._3._1 == -1 || left._3._2 == -1.0 )
								  				  		  right._3
								  				  		 // otherwise assign left
							  				  		     else
								  				  		  left._3
								  				
								  				 /* treeIdnodeId,(featureIndex,splitCandidate,quality, totalSamplesLeft, totalSamplesRight, bestLabel, bestLabelProbability)*/
								  				( left._1, left._2 /*treeId_nodeId*/,  bestSplit )
											})	
											
		// decide whether the split is a good or a bad one, by evaluating the stopping criterion
		// good ones go into the next level
	    // bad ones are final nodes (leaves) with a label
		val finalnodes = bestTreeNodeSplits
								.map({ x =>
									  	val treeId = x._1
									  	val nodeId = x._2
									  	val label = x._3._6.toInt
									  	
									  	System.out.println("final:"+x)
									  	
									  	// TREE-LEAF: if stopping criterion create node in output tree as a labeled one, but without a featureId
								  		if(isStoppingCriterion(x))
								  			(treeId, nodeId, -1/*featureId*/, 0.0 /*split*/, label, ""/*baggingTable*/, "" /*featureList*/) 
								  		// TREE-NODE: otherwise we have a nice split feature without a label
								  		else
								  			(treeId, nodeId, x._3._1.toInt/*featureId*/, x._3._2.toDouble/*split*/, -1, ""/*baggingTable*/, "" /*featureList*/) 
									})
								
		val nodestobuild = bestTreeNodeSplits.filter({ z  => !isStoppingCriterion(z) })
  						
		// compute new nodes to build
		val nodeWithBaggingTable = nodestobuild
							.join( nodesAndSamples )
							.where( x => (x._1,x._2 ) )
							.isEqualTo { x => (x._1,x._2) }
							.map({ (bestSplits, nodeAndSamples) =>

							  		(	bestSplits._1 /*treeId*/, 
							  			bestSplits._2 /*nodeId*/, 
							  			bestSplits._3._1 /*featureIndex*/, 
							  			bestSplits._3._2 /*splitCandidate*/, 
							  			nodeAndSamples._3 /*sampleIndex*/,  
							  			nodeAndSamples._4 /*label*/, 
							  			nodeAndSamples._5.find(x=>x._2==bestSplits._3._1).get._1.toDouble /*featureValue*/, 
							  			bestSplits._3._1 /*featureIndex*/, 
							  			nodeAndSamples._6 /*sampleCount*/ )
							})
							
							
		val leftNodesWithBaggingTables = nodeWithBaggingTable
							
							.filter({x=>x._7 <= x._4})
							
							.map({ case(treeId,nodeId, featureIndex, _, sampleIndex, _,_, _, count)=> 
							  				(treeId,nodeId, featureIndex, (0 until count).toList.map(x=>sampleIndex).mkString(" ")) })
							.groupBy({x=>(x._1,x._2)})
							.reduce({ (left,right)=> (left._1,left._2, left._3,left._4+" "+right._4) })
							.map({ x=> 
							  	val treeId = x._1
							  	val parentNodeId = x._2
							  	val nodeId = ((parentNodeId + 1) * 2) - 1
							  	val featureIndex = x._3
							  	System.out.println( "left:"+ (treeId,nodeId, x._4.split(" ").length ) )
							  	
							  	(treeId,nodeId, featureIndex/*featureId*/, 0.0 /*split*/, -1, x._4 /*baggingTable*/, "" /*featureList*/) 
							  })
							
		val rightNodesWithBaggingTables = nodeWithBaggingTable
							.filter({x=>x._7 > x._4})		
							.map({ case(treeId,nodeId, featureIndex, _, sampleIndex, _,_, _, count)=> 
							  				(treeId,nodeId, featureIndex, (0 until count).toList.map(x=>sampleIndex).mkString(" ")) })
							.groupBy({x=>(x._1,x._2)})
							.reduce({ (left,right)=> (left._1,left._2, left._3,left._4+" "+right._4) })
							.map({ x=> 
							  	val treeId = x._1
							  	val parentNodeId = x._2
							  	val nodeId = ((parentNodeId + 1) * 2)
							  	val featureIndex = x._3
							  	System.out.println( "left:"+ (treeId,nodeId, x._4.split(" ").length ) )
							  	
							  	(treeId,nodeId, featureIndex/*featureId*/, 0.0 /*split*/, -1, x._4 /*baggingTable*/, "" /*featureList*/) 
							  })
		
		// output to tree file if featureIndex != -1 (node) or leaf (label detected)  
		val finaTreeNodesSink = finalnodes
									.write(outputTreePath, CsvOutputFormat(newLine, ","))
		
									
		// prepare the treeId,nodeId and featureList for next round
		// map to new potential nodeIds
		val nodeFeatures = inputNodeQueue flatMap { line =>
			val values = line.trim.split(",")
			val treeId = values(0).toInt
			val nodeId = values(1).toInt
			val features = values(7)

			val leftNodeId = ((nodeId + 1) * 2) - 1
			val rightNodeId = ((nodeId + 1) * 2)

			List((treeId, leftNodeId, features), (treeId, rightNodeId, features))
		}

		// filter nodes to build, join the last featureList from node and remove :q
		val nodeResultsWithFeatures =  rightNodesWithBaggingTables
											.union(leftNodesWithBaggingTables)
											.join(nodeFeatures)
											.where({ x => (x._1, x._2) })	/*join by treeId,nodeId */
											.isEqualTo { y => (y._1, y._2) }
											.map({ (nodeResult, nodeFeatures) => 
											    val selectedFeatureForNode = nodeResult._3.toInt
												val features = nodeFeatures._3.split(" ").map({ _.toInt }).filter(x => x != selectedFeatureForNode)
												val featureSpace = generateFeatureSubspace(featureSubspaceCount, features.toBuffer)
												(	nodeResult._1, 
												    nodeResult._2, 
												    -1, 
												    -1, 
												    -1,
												    nodeResult._6, 
												    featureSpace.mkString(" "), 
												    features.mkString(" "))
											})
								
		// output to tree file if featureIndex != -1 (node) or leaf (label detected)  
		val treeLevelSink = finalnodes .write(outputTreePath, CsvOutputFormat(newLine, ","))
									
		// output nodes to build if 
		val nodeQueueSink = nodeResultsWithFeatures .write(outputNodeQueuePath, CsvOutputFormat(newLine, ","))
						
		new ScalaPlan(Seq(treeLevelSink,nodeQueueSink ))
	}


	def impurity(q: List[Double]) = {
		gini(q)
	}

	def gini(q: List[Double]) = {
		1.0 - q.map(x => x * x).sum.toDouble
	}

	def entropy(q: List[Double]) = {
		-q.map(x => x * Math.log(x)).sum
	}

	def quality_function(tau: Double, q: List[Double], qL: List[Double], qR: List[Double]) = {
		impurity(qL) - tau * impurity(qL) - (1 - tau) * impurity(qR);
	}

	def generateFeatureSubspace(randomCount: Int, features: Buffer[Int]): Array[Int] = {
		var arr: Array[Int] = Array()
		arr = Array(randomCount)
		arr = Array.fill(randomCount)(0)
		for (i <- 0 until randomCount) {
			var random = new Random().nextInt(features.length);
			arr(i) = features.remove(random);
		}
		arr;
	}
	
	def createLabelArray( labels : Integer, values : List[(Integer,Integer)])={
	  val a = new Array[Int](labels)
	  values.foreach(f=>a(f._1)=f._2)
	  a
	}

	def isStoppingCriterion( x : (Int,Int, (Int/*featureIndex*/, Double /*splitCandidate*/, Double /*quality*/, Int /*totalSamplesLeft*/, Int /*totalSamplesRight*/,  Int /*bestLabel*/, Double /*bestLabelProbability*/ ) ) ) = {
	  System.out.println("check for stopping: "+x);
	  
	  if( x._3._4 == 0 ||  x._3._5 == 0 || x._3._4 < minNrOfItems || x._3._5 < minNrOfItems  ){
	    System.out.println("stop!");
	    true
	  }
	  else
		  false
	}
}
