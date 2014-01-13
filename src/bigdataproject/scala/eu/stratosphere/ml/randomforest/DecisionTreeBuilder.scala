package bigdataproject.scala.eu.stratosphere.ml.randomforest

import eu.stratosphere.pact.common.plan.PlanAssembler
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription
import eu.stratosphere.scala._
import eu.stratosphere.scala.operators._
import scala.util.matching.Regex
import eu.stratosphere.pact.client.LocalExecutor
import util.Random
import scala.collection.mutable.Buffer

class DecisionTreeBuilder(var minNrOfItems: Int, var featureSubspaceCount: Int) extends PlanAssembler with PlanAssemblerDescription with Serializable {

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
  									.map({ x => (x._1, createLabelArray(10, List( (x._5 % 10, 1)))) })
 									.groupBy({x=>x._1})
									.reduce({(left,right)=>(left._1, left._2.zip(right._2).map(x=>x._1+x._2)) })
									.map({ x => (x._1, x._2.toList.map({p => p / x._2.sum.toDouble }).mkString(" ") ) })

  		val nodeFeatureDistributions_qjL = nodeFeatureDistributions
  									.filter({x=> x._4 <= x._2})
  									.map({ x => (x._1, createLabelArray(10, List( (x._5 % 10, 1)))) })
 									.groupBy({x=>x._1} )
									.reduce({(left,right)=>(left._1, left._2.zip(right._2).map(x=>x._1+x._2)) })
									.map({ x => (x._1, x._2.toList.map({p => p / x._2.sum.toDouble}).mkString(" ") ) })

  		val nodeFeatureDistributions_qjR = nodeFeatureDistributions
  									.filter({x=> x._4 > x._2})
  									.map({ x => (x._1, createLabelArray(10, List( (x._5 % 10, 1)))) })
 									.groupBy({x=>x._1})
									.reduce({(left,right)=>(left._1, left._2.zip(right._2).map(x=>x._1+x._2)) })
									.map({ x => (x._1, x._2.toList.map({p => p / x._2.sum.toDouble}).mkString(" ") ) })

									
		val bestSplits = nodeFeatureDistributions_qj
		  						.join(nodeFeatureDistributions_qjL)	
								.where( { x => x._1 })
								.isEqualTo { x => x._1 }
								.map({ (qj, qjL) => (qj._1, qj._2,qjL._2) })
		  						
		  						.join(nodeFeatureDistributions_qjR)	
								.where( { x => x._1 })
								.isEqualTo { x => x._1 }
								.map({ (qjqjL, INqjR) => 
								    val treeIdnodeId = qjqjL._1.split("_").take(2).mkString("_")
								    val featureIndex = qjqjL._1.split("_")(2).toInt
								    val splitCandidate = qjqjL._1.split("_")(3).toDouble
									val tau = 0.5
									val qj = qjqjL._2.split(" ").map(_.toDouble)
									val qjL = qjqjL._3.split(" ").map(_.toDouble)
									val qjR = INqjR._2.split(" ").map(_.toDouble)
									val quality = quality_function(tau, qj.toList, qjL.toList, qjR.toList);								  
									(treeIdnodeId, (featureIndex,splitCandidate, quality) ) 
								})
								// group by treeIdnodeIdFeatureIndex and compute the max (best quality)
								.groupBy({x=>x._1})
								.reduce({ (left,right) =>  
								  			val bestSplit = if(left._2._2>right._2._2) left._2 else right._2 
											(left._1 /*treeId_nodeId*/,  bestSplit ) /* treeIdnodeId,(featureIndex,splitCandidate,quality)*/
											})

		// compute new nodes to build
		val nodeWithBaggingTable = bestSplits
							.map({x=> (x._1, x._2) })
							.join( nodeSampleFeatures.map({ case(key,keyTuple,sampleIndex, label, featureValue, featureIndex, count)=>
								  						(keyTuple._1+"_"+keyTuple._2, sampleIndex, label, featureValue, featureIndex, count )
												}) )
							.where( x => x._1 )
							.isEqualTo { x => x._1 }
							.map({ (bestSplits, nodeSampleFeatures) =>
							  		(bestSplits._1 /*treeIdnodeId*/, bestSplits._2._1 /*featureIndex*/, bestSplits._2._1 /*splitCandidate*/, nodeSampleFeatures._2 /*sampleIndex*/,  nodeSampleFeatures._3 /*label*/, nodeSampleFeatures._4 /*featureValue*/, nodeSampleFeatures._5 /*featureIndex*/, nodeSampleFeatures._6 /*featureCount*/ )
							})
							
	/*						  		
			.groupBy { case (treeId, nodeId, sampleIndex, label, feature, count) => (treeId, nodeId)}
			.reduceGroup(histograms => {
				val buffered = histograms.buffered
				val keyValues = buffered.head				
				val buckets = 10
				val bufferList = buffered.toList.flatMap(x => 0.until(x._6).map(i => x))
				val mergedHistograms = bufferList
					.flatMap{case (treeId, nodeId, sampleIndex, label, features, count) => features.map(f => (f._1, f._2))}
					.groupBy(_._2)
					.map(x => x._2.map(f => new Histogram(f._2, buckets).update(f._1)).reduceLeft( (s1, s2) => s1.merge(s2)))

				//List[(Int, Int, List[(Int, Double)])] ..... label, sampleIndex, feature
				val sampleList = bufferList
					.map{case (treeId, nodeId, sampleIndex, label, features, count) => (label, sampleIndex, features.map(x => (x._2, x._1)))}
					.toList
					
				val treeId = keyValues._1
	  			val nodeId = keyValues._2
	  			val totalSamples = sampleList.length
	  			
				// find some split candidates to make further evaluation
				val splitCandidates = mergedHistograms.map(x => (x.feature, x.uniform(buckets), x)).filter(_._2.length > 0)
				
				// compute split-quality for each candidate
				val splitQualities = splitCandidates.flatMap {
					case (featureIndex, featureBuckets, x) =>
						featureBuckets.map(bucket => split_quality(sampleList, featureIndex, bucket, x, totalSamples))
				}

				// check the array with split qualities is not empty
				if (splitQualities.length > 0) {

					val bestSplit = splitQualities.maxBy(_._3)

					// create new bagging tables for the next level
					val leftNode = sampleList
						.flatMap({
							case (label,sampleIndex, features) =>
								features.filter({ case (feature, value) => feature == bestSplit._1 && value <= bestSplit._2 })
									.map(x => (sampleIndex,x._1, x._2))
						})
					val rightNode = sampleList
						.flatMap({
							case (label,sampleIndex, features) =>
								features.filter({ case (feature, value) => feature == bestSplit._1 && value > bestSplit._2 })
									.map(x => (sampleIndex,x._1, x._2))
						})

					// decide if there is a stopping condition
					val stoppingCondition = leftNode.isEmpty || rightNode.isEmpty || leftNode.lengthCompare(minNrOfItems) == -1 || rightNode.lengthCompare(minNrOfItems) == -1;

					System.out.println(bestSplit)
					System.out.println("right:" + rightNode.length)
					System.out.println("left:" + leftNode.length)

					// serialize based on stopping condition
					if (stoppingCondition) {
						// compute the label by max count (uniform distribution)
						val label = sampleList
							.groupBy({ case (label,sampleIndex,sample) => (label) })
							.maxBy(x => x._2.length)._1

						List((treeId, nodeId, -1 /* feature*/ , 0.0 /*split*/ , label, "", "", ""))

					} else {
						var left_nodeId = ((nodeId + 1) * 2) - 1
						var right_nodeId = ((nodeId + 1) * 2)

						var left_baggingTable = leftNode.map({ x => x._1 }).mkString(" ")
						var right_baggingTable = rightNode.map({ x => x._1 }).mkString(" ")

						//System.out.println(bestSplit)
						//System.out.println("leftnode: " + leftNode.length)
						//System.out.println("rightnode: " + rightNode.length)

						List(
							// emit the tree node
							(treeId, nodeId, bestSplit._1, bestSplit._2, -1, "", "", ""),

							// emit new nodes for the level node queue
							(treeId, left_nodeId, -1, 0.0, -1, left_baggingTable, bestSplit._1.toString, ""),
							(treeId, right_nodeId, -1, 0.0, -1, right_baggingTable, bestSplit._1.toString, ""))
					}

				} else {
					// compute the label by max count (uniform distribution)
					val label = sampleList
						.groupBy({ case (label, sampleIndex, sample) => (label) })
						.maxBy(x => x._2.length)._1

					System.out.println(label)

					// emit the final tree node
					List((treeId, nodeId, -1 /* feature*/ , 0.0 /*split*/ , label, "", "", ""))
				}
			})
			.flatMap(x => x)
		*/
		val newLine = System.getProperty("line.separator");

		/*
			
		// output to tree file if featureIndex != -1 (node) or leaf (label detected)  
		val finaTreeNodesSink = nodeResults
			.filter({
				case (treeId, nodeId, featureIndex, splitValue, label, baggingTable, _, _) =>
					featureIndex != -1 || label != -1
			})
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
		val nodeResultsWithFeatures = nodeResults
			.filter({
				case (treeId, nodeId, featureIndex, splitValue, label, _, _, _) =>
					featureIndex == -1 && label == -1
			})
			.join(nodeFeatures)
			.where({ x => (x._1, x._2) })
			.isEqualTo { y => (y._1, y._2) }
			.map({ (nodeResult, nodeFeatures) =>
				val selectedFeatureForNode = nodeResult._7.toInt
				val features = nodeFeatures._3.split(" ").map({ _.toInt }).filter(x => x != selectedFeatureForNode)
				val featureSpace = generateFeatureSubspace(featureSubspaceCount, features.toBuffer)

				(nodeResult._1, nodeResult._2, nodeResult._3, nodeResult._4, nodeResult._5, nodeResult._6, featureSpace.mkString(" "), features.mkString(" "))
			})

		// output nodes to build if 
		val nodeQueueSink = nodeResultsWithFeatures
			.write(outputNodeQueuePath, CsvOutputFormat(newLine, ","))
w
		new ScalaPlan(Seq(finaTreeNodesSink, nodeQueueSink))
		*/
		val sink1 = nodeFeatureDistributions_qj.write("/home/kay/rf_probs_qj", CsvOutputFormat(newLine, ","))
		val sink2 = nodeFeatureDistributions_qjL.write("/home/kay/rf_probs_qjL", CsvOutputFormat(newLine, ","))
		val sink3 = nodeFeatureDistributions_qjR.write("/home/kay/rf_probs_qjR", CsvOutputFormat(newLine, ","))
		val sink4 = bestSplits.write("/home/kay/rf_probs_best_splits", CsvOutputFormat(newLine, ","))

		new ScalaPlan(Seq(sink1,sink2,sink3,sink4))
		
	}
	// INPUT
	// List[(Int,Array[(Int,Double)])] => sampleList with featureIndex and value 
	//							  List( (label, List(s1f1,s1f2,s1f3,..s1fN)), ... )
	// Int => feature
	// Double => split candidate
	// Histogram histogram distribution
	// OUTPUT
	// (feature,candidate,quality)

	def split_quality(sampleList: List[(Int, Int, Array[(Int, Double)])],
		feature: Int,
		candidate: Double,
		h: Histogram,
		totalSamples: Int) = {

		// filter feature from all samples
		val featureList = sampleList
			.map({ case (label, sampleIndex, sample) => (label, sample.filter(x => x._1 == feature).head) })

		// probability for each label occurrence in the node
		val qj = featureList
			.groupBy(_._1) /*group by label */
			.map(x => (x._2.length.toDouble / totalSamples))

		//System.out.println("feature:"+feature+"    candidate:"+candidate+"    "+h.uniform(10)+"     histogram:"+h.toString)

		// compute probability distribution for each child (Left,Right) and the current candidate with the specific label
		val qLj = featureList
			.filter({ case (label, sample) => sample._2 <= candidate })
			.groupBy(_._1) /*group by label */
			.map(x => (x._2.length.toDouble / totalSamples))

		val qRj = featureList
			.filter({ case (label, sample) => sample._2 > candidate })
			.groupBy(_._1) /*group by label */
			.map(x => (x._2.length.toDouble / totalSamples))

		// TODO: quality_function do not use the qj List, instead use the qLj list two times
		val tau = 0.5
		val quality = quality_function(tau, qj.toList, qLj.toList, qRj.toList);

		(feature, candidate, quality)
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

}
