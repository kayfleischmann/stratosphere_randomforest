package bigdataproject.scala.eu.stratosphere.ml.randomforest

import eu.stratosphere.pact.common.plan.PlanAssembler
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription
import eu.stratosphere.scala._
import eu.stratosphere.scala.operators._
import scala.util.matching.Regex
import eu.stratosphere.pact.client.LocalExecutor
import util.Random
import scala.collection.mutable.Buffer

class DecisionTreeBuilder(var minNrOfItems: Int, var featureSubspaceCount: Int, var numClasses : Int) extends PlanAssembler with PlanAssemblerDescription with Serializable {

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

		val nodequeue = inputNodeQueue map { line =>
			val values = line.split(",")
			val treeId = values(0).toInt
			val nodeId = values(1).toInt
			val bestSplitIndex = values(2)
			val bestSplitValue = values(3)
			val label = values(4)
			val baggingTable = values(5)
			val featureSpace = values(6)

			(treeId, nodeId, baggingTable, featureSpace)
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
					.groupBy(x => x)
					.map(sampleIndex => (treeId, nodeId, sampleIndex._1.toInt, featureSpace.split(" ").map(_.toInt), sampleIndex._2.length))
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
					  	( treeId,nodeId,featureIndex, sampleIndex, label, featureValue, count )})
				}

			
		val nodeSampleFeatureHistograms = nodeSampleFeatures
							.map({ case ( treeId,nodeId,featureIndex,sampleIndex,label,featureValue, count) => 
										( treeId,nodeId,featureIndex,new Histogram(featureIndex, 10).update(featureValue,count).toString ) })

		val  nodeHistograms = nodeSampleFeatureHistograms
							.groupBy({ x => (x._1,x._2,x._3)})
							.reduce( {(left,right) => (left._1, left._2, left._3, Histogram.fromString(left._4).merge(Histogram.fromString(right._4)).toString ) } )
						

									
		val nodeFeatureDistributions = nodeHistograms
									.flatMap( { nodeHistogram => 
												Histogram.fromString(nodeHistogram._4).uniform(10)
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
									
									
  		val nodeFeatureDistributions_qj = nodeFeatureDistributions
  									.map({ x => (x._1, x._2, x._3, x._4, createLabelArray(numClasses, List( (x._7/*label*/, x._8 /*count*/ )))) })
 									.groupBy({x=>(x._1,x._2,x._3,x._4)})
									.reduce({(left,right)=>(left._1,left._2,left._3,  left._4, left._5.zip(right._5).map(x=>x._1+x._2)) })
									.map({ x => (x._1, x._2, x._3, x._4, x._5.toList.mkString(" ") ) })

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

									
		val nodeDistributions = nodeFeatureDistributions_qj
		  						.join(nodeFeatureDistributions_qjL)	
								.where( { x => (x._1,x._2,x._3,x._4) })
								.isEqualTo { x =>  (x._1,x._2,x._3,x._4) }
									
								.map({ (qj, qjL) => (	qj._1, /*treeId*/
														qj._2, /*nodeId*/
														qj._3, /*featureId*/
														qj._4, /*splitCandidate*/
														qj._5, /*histogram qj*/
														qjL._5 /*histogram qjL */
														) })
		  						.join(nodeFeatureDistributions_qjR)	
								.where( { x =>  (x._1,x._2,x._3,x._4)  })
								.isEqualTo { x => (x._1,x._2,x._3,x._4)  }	
								.map({ (qjqjL, qjR) => 
								    val treeId = qjqjL._1
								    val nodeId = qjqjL._2
								    val featureIndex = qjqjL._3
								    val splitCandidate = qjqjL._4
									val tau = 0.5
									
									val p_qj = qjqjL._5.split(" ").map(_.toDouble )
									val p_qjL = qjqjL._6.split(" ").map(_.toDouble)
									val p_qjR = qjR._5.split(" ").map(_.toDouble)

									val totalSamples = p_qj.sum.toInt
									val totalSamplesLeft = p_qjL.sum.toInt
									val totalSamplesRight = p_qjR.sum.toInt
									val bestLabel = p_qj.zipWithIndex.maxBy(_._1)._2
									val bestLabelProbability = p_qj(bestLabel) / totalSamples.toDouble;
									var quality = quality_function( tau, 
																	p_qj.map( _ /totalSamples).toList, 
																	p_qjL.map( _ /totalSamples).toList, 
																	p_qjR.map(_/totalSamples).toList);			
								    
									//quality = Math.round(quality*100.0)/100.0;
									/*System.out.println("-----------")
									System.out.println(splitCandidate);
									System.out.println(quality);
									System.out.println(totalSamplesLeft);
									System.out.println(totalSamplesRight);
									System.out.println(p_qj.toList);
									System.out.println(p_qjL.toList);
									System.out.println(p_qjR.toList);*/
									
									(treeId,nodeId, (featureIndex,splitCandidate, quality, totalSamplesLeft, totalSamplesRight, bestLabel, bestLabelProbability) ) 
								})
								
		val bestSplit = nodeDistributions
								// group by treeIdnodeIdFeatureIndex and compute the max (best quality)
								.groupBy({x=>(x._1, x._2)})
								.reduce({ (left,right) =>  
								  			val bestSplit = if(left._3._3>right._3._3) left._3 else right._3 
								  				( left._1, left._2 /*treeId_nodeId*/,  bestSplit ) /* treeIdnodeId,(featureIndex,splitCandidate,quality, totalSamplesLeft, totalSamplesRight, bestLabel, bestLabelProbability)*/
											})	
								
		val final_OutputTreeNodes = bestSplit
								.map({ x =>
									  	val treeId = x._1
									  	val nodeId = x._2
									  	val label = x._3._6.toInt
								  		if(isStoppingCriterion(x))
								  			(treeId, nodeId, -1/*featureId*/, 0.0 /*split*/, label, ""/*baggingTable*/, "" /*featureList*/) 
								  		else
								  			(treeId, nodeId, x._3._1.toInt/*featureId*/, x._3._2.toDouble/*split*/, -1, ""/*baggingTable*/, "" /*featureList*/) 
								  		  
									})
								
								
		val nodestobuild = bestSplit.filter({ z  => ! isStoppingCriterion(z) })
		  						
		  						
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
							.filter({x=>x._4 <= x._7})		
							.map({ case(treeId,nodeId, featureIndex, _, sampleIndex, _,_, _, count)=> 
							  				(treeId,nodeId, featureIndex, (0 until count).toList.map(x=>sampleIndex).mkString(" ")) })
							.groupBy({x=>(x._1,x._2)})
							.reduce({ (left,right)=> (left._1,left._2, left._3,left._4+" "+right._4) })
							.map({ x=> 
							  	val treeId = x._1
							  	val parentNodeId = x._2
							  	val nodeId = ((parentNodeId + 1) * 2) - 1
							  	val featureIndex = x._3
							  	(treeId,nodeId, featureIndex/*featureId*/, 0.0 /*split*/, -1, x._4 /*baggingTable*/, "" /*featureList*/) 
							  })
							
		val rightNodesWithBaggingTables = nodeWithBaggingTable
							.filter({x=>x._4 > x._7})		
							.map({ case(treeId,nodeId, featureIndex, _, sampleIndex, _,_, _, count)=> 
							  				(treeId,nodeId, featureIndex, (0 until count).toList.map(x=>sampleIndex).mkString(" ")) })
							.groupBy({x=>(x._1,x._2)})
							.reduce({ (left,right)=> (left._1,left._2, left._3,left._4+" "+right._4) })
							.map({ x=> 
							  	val treeId = x._1
							  	val parentNodeId = x._2
							  	val nodeId = ((parentNodeId + 1) * 2)
							  	val featureIndex = x._3
							  	(treeId,nodeId, featureIndex/*featureId*/, 0.0 /*split*/, -1, x._4 /*baggingTable*/, "" /*featureList*/) 
							  })
		
			
		val newLine = System.getProperty("line.separator");

		
		// output to tree file if featureIndex != -1 (node) or leaf (label detected)  
		val finaTreeNodesSink = final_OutputTreeNodes
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
						// output nodes to build if 
		val nodeQueueSink = nodeResultsWithFeatures
						.write(outputNodeQueuePath, CsvOutputFormat(newLine, ","))
						
		
		//val bestSplitsSink = bestSplit.	write("/home/kay/rf/rf_bestsplits", CsvOutputFormat(newLine, ","))
		new ScalaPlan(Seq(finaTreeNodesSink, nodeQueueSink))
											

		/*
		val bestSplitsSink = bestSplit.	write("/home/kay/rf/rf_bestsplits", CsvOutputFormat(newLine, ","))
		
		val outnodeDistributions = nodeDistributions.	write("/home/kay/rf/rf_node_distributions", CsvOutputFormat(newLine, ","))
		
		val nodeSamplesFeaturesSink = nodeFeatureDistributions.write("/home/kay/rf/rf_nodesamples_features", CsvOutputFormat(newLine, ","))			
		val nodeFeatureD = nodeFeatureDistributions.write("/home/kay/rf/rf_nodeFeatureD", CsvOutputFormat(newLine, ","))			
		val nodeDqj = nodeFeatureDistributions_qj.write("/home/kay/rf/rf_nodeDqj", CsvOutputFormat(newLine, ","))			
		val nodeDqjL = nodeFeatureDistributions_qjL.write("/home/kay/rf/rf_nodeDqjL", CsvOutputFormat(newLine, ","))			
		val nodeDqjR = nodeFeatureDistributions_qjR.write("/home/kay/rf/rf_nodeDqjR", CsvOutputFormat(newLine, ","))			
		new ScalaPlan(Seq(nodeDqj,nodeDqjL,nodeDqjR,nodeFeatureD,nodeSamplesFeaturesSink,bestSplitsSink,outnodeDistributions ))*/
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
	  val a = new Array[Int](10)
	  values.foreach(f=>a(f._1)=f._2)
	  a
	}

	def isStoppingCriterion( x : (Int,Int, (Int/*featureIndex*/, Double /*splitCandidate*/, Double /*quality*/, Int /*totalSamplesLeft*/, Int /*totalSamplesRight*/,  Int /*bestLabel*/, Double /*bestLabelProbability*/ ) ) ) = {
	  if( x._3._4 == 0 ||  x._3._5 == 0 || x._3._4 < minNrOfItems || x._3._5 < minNrOfItems  ){
	    true
	  }
	  else
		  false
	}
}
