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
					features.map({ case(featureValue,featureIndex) => ( treeId+"_"+nodeId+"_"+featureIndex, (treeId,nodeId,featureIndex), sampleIndex, label, featureValue, featureIndex, count )})
				}

			
		val nodeSampleFeatureHistograms = nodeSampleFeatures
							.map({ case ( key,(treeId,nodeId,feature),sampleIndex,label,featureValue,featureIndex, count) => 
										((treeId,nodeId,featureIndex),new Histogram(featureIndex, 10).update(featureValue,count).toString ) })

		val  nodeHistograms = nodeSampleFeatureHistograms
							.groupBy({_._1})
							.reduce( {(left,right) => ( left._1, Histogram.fromString(left._2).merge(Histogram.fromString(right._2)).toString ) } )
						
							
		val nodeFeatureDistributions = nodeHistograms
									.flatMap( { nodeHistogram => 
												Histogram.fromString(nodeHistogram._2).uniform(10)
															.map({ splitCandidate=> 
															  		(nodeHistogram._1._1+"_"+nodeHistogram._1._2+"_"+nodeHistogram._1._3, nodeHistogram._1, splitCandidate)}) 
									})
									.join(nodeSampleFeatures)
									.where( { x => x._1 })
									.isEqualTo { x => x._1 }
									.map({ (nodeHistogram, nodeSampleFeature) =>
									  	(	nodeHistogram._1+"_"+nodeHistogram._3 /* ( (treeId,nodeId,featureIndex)*/,
									  		nodeHistogram._3 /*splitCandidate*/,
									  	    nodeSampleFeature._3 /*sampleIndex*/,
									  		nodeSampleFeature._5 /*featureValue*/, 
									  		nodeSampleFeature._4 /*label*/, 
									  		1 )
									})
									
									
  		val nodeFeatureDistributions_qj = nodeFeatureDistributions
  									.map({ x => (x._1, createLabelArray(numClasses, List( (x._5 % numClasses, 1)))) })
 									.groupBy({x=>x._1})
									.reduce({(left,right)=>(left._1, left._2.zip(right._2).map(x=>x._1+x._2)) })
									.map({ x => (x._1, x._2.toList.mkString(" ") ) })

  		val nodeFeatureDistributions_qjL = nodeFeatureDistributions
  									.filter({x=> x._4 <= x._2})
  									.map({ x => (x._1, createLabelArray(numClasses, List( (x._5 % numClasses, 1)))) })
 									.groupBy({x=>x._1} )
									.reduce({(left,right)=>(left._1, left._2.zip(right._2).map(x=>x._1+x._2)) })
									.map({ x => (x._1, x._2.toList.mkString(" ") ) })

  		val nodeFeatureDistributions_qjR = nodeFeatureDistributions
  									.filter({x=> x._4 > x._2})
  									.map({ x => (x._1, createLabelArray(numClasses, List( (x._5 % numClasses, 1)))) })
 									.groupBy({x=>x._1})
									.reduce({(left,right)=>(left._1, left._2.zip(right._2).map(x=>x._1+x._2)) })
									.map({ x => (x._1, x._2.toList.mkString(" ") ) })

									
		val nodeDistributions = nodeFeatureDistributions_qj
		  						.join(nodeFeatureDistributions_qjL)	
								.where( { x => x._1 })
								.isEqualTo { x => x._1 }
								.map({ (qj, qjL) => (qj._1, qj._2,qjL._2) })
		  						
		  						.join(nodeFeatureDistributions_qjR)	
								.where( { x => x._1 })
								.isEqualTo { x => x._1 }	
								.map({ (qjqjL, INqjR) => 
								    val treeId =qjqjL._1.split("_")(0).toInt
								    val nodeId =qjqjL._1.split("_")(1).toInt
								    val treeIdnodeId = qjqjL._1.split("_").take(2).mkString("_")
								    val featureIndex = qjqjL._1.split("_")(2).toInt
								    val splitCandidate = qjqjL._1.split("_")(3).toDouble
									val tau = 0.5
									
									val qj = qjqjL._2.split(" ").map(_.toDouble )
									val qjL = qjqjL._3.split(" ").map(_.toDouble)
									val qjR = INqjR._2.split(" ").map(_.toDouble)

									val totalSamples = qj.sum.toInt
									val totalSamplesLeft = qjL.sum.toInt
									val totalSamplesRight = qjR.sum.toInt
									val bestLabel = qj.zipWithIndex.maxBy(_._1)._2
									val bestLabelProbability = qj(bestLabel) / totalSamples.toDouble;
									val quality = quality_function(tau, qj.map(_/totalSamples).toList, qjL.map(_/totalSamplesLeft).toList, qjR.map(_/totalSamplesRight).toList);			
								    
									(treeId,nodeId, (featureIndex,splitCandidate, quality, totalSamplesLeft, totalSamplesRight, bestLabel, bestLabelProbability) ) 
								})
								
		val bestSplit = nodeDistributions
								// group by treeIdnodeIdFeatureIndex and compute the max (best quality)
								.groupBy({x=>(x._1, x._2)})
								.reduce({ (left,right) =>  
								  			val bestSplit = if(left._3._2>right._3._2) left._3 else right._3 
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
							  	System.out.println( "left-bestsplit:"+x._4.split(" ").length )
							  	
							  	
							  	(treeId,nodeId, featureIndex/*featureId*/, 0.0 /*split*/, -1, x._3 /*baggingTable*/, "" /*featureList*/) 
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
							  	System.out.println( "right-bestsplit:"+x._4.split(" ").length )

							  	(treeId,nodeId, featureIndex/*featureId*/, 0.0 /*split*/, -1, x._3 /*baggingTable*/, "" /*featureList*/) 
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
												System.out.println(nodeResult)
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
						
		new ScalaPlan(Seq(finaTreeNodesSink, nodeQueueSink))
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
	    System.out.println("dammm")
	    System.out.println(x)
	    true
	  }
	  else
		  false
	}
}
