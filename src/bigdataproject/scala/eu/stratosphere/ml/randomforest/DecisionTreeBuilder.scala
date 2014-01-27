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
import eu.stratosphere.compiler.PactCompiler

class DecisionTreeBuilder(var minNrOfItems: Int, var featureSubspaceCount: Int) extends Program with ProgramDescription with Serializable {

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

		var nodesWithBaggingTableCounter = 0
		val nodesWithBaggingTable = inputNodeQueue flatMap { line =>
			val values = line.split(",")
			val treeId = values(0).toInt
			val nodeId = values(1).toInt
			val baggingTable = values(5).trim
			val featureSpace = values(6).trim
			baggingTable
				.split(" ")
				.map(sampleIndex => {
					if (nodesWithBaggingTableCounter % 40000 == 0)
					{
						System.out.println("loading nodes " + nodesWithBaggingTableCounter)
					}
					nodesWithBaggingTableCounter = nodesWithBaggingTableCounter + 1
					(treeId, nodeId, sampleIndex.toInt, featureSpace)
					})
		}

		val samples = trainingSet map { line =>
			var firstSpace = line.indexOf(' ', 0)
			var secondSpace = line.indexOf(' ', firstSpace + 1)
			
			val sampleIndex = line.substring(0, firstSpace).trim().toInt
			val label = line.substring(firstSpace, secondSpace).trim().toInt

			if (sampleIndex % 40000 == 0)
			{
				System.out.println("loading samples " + sampleIndex)
			}
			
			(sampleIndex, label, line)
		}
			
		var nodesAndSamplesCounter = 0

		val nodesAndSamples = samples
			.join(nodesWithBaggingTable)
			.where { x => x._1 }
			.isEqualTo { x => x._3 }
			.map {(sample, node) =>
				{
					if (nodesAndSamplesCounter % 40000 == 0)
					{
						System.out.println("joining nodes & samples " + nodesAndSamplesCounter)
					}
					nodesAndSamplesCounter = nodesAndSamplesCounter + 1
					val featureSpace = node._4.split(" ").map(_.toInt)
					//val sampleFeatures = sample._3.split(" ").drop(2)
					val sampleFeatures = sample._3.split(" ").drop(2).toIndexedSeq
					(
					node._1, //treeId
					node._2, //nodeid
					sample._1, //sampleIndex
					sample._2, //label
					//List((1.0, 1))
					//sample._3.split(" ").drop(2).zipWithIndex.filter(x => featureSpace.contains(x._2)).map(feature => (feature._1.toDouble, feature._2)).toList //features
					featureSpace.map(x => (sampleFeatures.apply(x).toDouble, x))
					)
				}
			}
			
		nodesAndSamples.contract.setParameter(PactCompiler.HINT_LOCAL_STRATEGY, PactCompiler.HINT_LOCAL_STRATEGY_HASH_BUILD_SECOND)

		
		var nodesToHistogramCounter = 0
		val nodeSampleFeatureHistograms  = nodesAndSamples
	        .flatMap  { case(treeId,nodeId,sampleIndex,label,features) =>
            {
				if (nodesToHistogramCounter % 40000 == 0)
				{
					System.out.println("creating new histograms " + nodesToHistogramCounter)
				}
				nodesToHistogramCounter = nodesToHistogramCounter + 1
				
            	features.map({ case(featureValue,featureIndex) => 
                      {
                      	( treeId,nodeId,featureIndex, new Histogram(featureIndex, 10).update(featureValue, 1)) 
                      }})
            }}
								
		var nodeHistogramsCounter = 0
		val  nodeHistograms = nodeSampleFeatureHistograms
			.groupBy({ x => (x._1,x._2,x._3)})
			.reduce( {(left,right) => {
				if (nodeHistogramsCounter % 40000 == 0)
				{
					System.out.println("merging histograms " + nodeHistogramsCounter)
				}
				nodeHistogramsCounter = nodeHistogramsCounter + 1
				(left._1, left._2, left._3, left._4.merge(right._4))
			}})
									
		val mergedNodeHistograms = nodeHistograms
			.map( { nodeHistogram => 
				{
					(
					nodeHistogram._1, /*treeId */
			  		nodeHistogram._2, /*nodeId*/
			  		nodeHistogram._3, /*featureId*/
			  		nodeHistogram._4.uniform(10)
			  		)
			  	}
			})
		
		var nodeResultsCounter = 0
		val nodeResults = mergedNodeHistograms
			.cogroup(nodesAndSamples)
			.where(histogram => (histogram._1, histogram._2))
			.isEqualTo(nodesAndSamples => (nodesAndSamples._1, nodesAndSamples._2))
			.map((histograms, samples) => {
				System.out.println("splitting node")
				
				val histogramsBuffered = histograms.buffered
				val keyValues = histogramsBuffered.head				
				val buckets = 10
				val histogramsBufferedList = histogramsBuffered.toList
				val samplesBuffered = samples.buffered.toList
					
				val treeId = keyValues._1
	  			val nodeId = keyValues._2
	  			
	  			System.out.println("composing sampleList")
	  			
	  			val sampleList = samplesBuffered
	  				
	  			val totalSamples = sampleList.length
	  			
	  			System.out.println("sampleList composed, total " + totalSamples)
	  			
				// find some split candidates to make further evaluation
				val splitCandidates = histogramsBufferedList.map(x => (x._3, x._4)).filter(_._2.length > 0)
				
				// compute split-quality for each candidate
				val splitQualities = splitCandidates.flatMap {
					case (featureIndex, featureBuckets) =>
						featureBuckets.map(bucket => split_quality(sampleList, featureIndex, bucket, totalSamples))
				}

				// check the array with split qualities is not empty
				if (splitQualities.length > 0) {

					val bestSplit = splitQualities.maxBy(_._3)

					// create new bagging tables for the next level
					val leftNode = sampleList
						.flatMap({
							case (tree, node, sampleIndex, label, features) =>
								features.filter({ case (value, feature) => feature == bestSplit._1 && value <= bestSplit._2 })
									.map(x => (sampleIndex,x._2, x._1))
						})
					val rightNode = sampleList
						.flatMap({
							case (tree, node, sampleIndex, label, features) =>
								features.filter({ case (value, feature) => feature == bestSplit._1 && value > bestSplit._2 })
									.map(x => (sampleIndex,x._2, x._1))
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
							.groupBy({ case (tree, node, sampleIndex, label, features) => (label) })
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
						.groupBy({ case (tree, node, sampleIndex, label, features) => (label) })
						.maxBy(x => x._2.length)._1

					System.out.println(label)

					// emit the final tree node
					List((treeId, nodeId, -1 /* feature*/ , 0.0 /*split*/ , label, "", "", ""))
				}
			})
			.flatMap(x => x)
			

		val newLine = System.getProperty("line.separator");
			
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

	def split_quality(sampleList: List[(Int, Int, Int, Int, Array[(Double, Int)])],
		feature: Int,
		candidate: Double,
		totalSamples: Int) = {

		// filter feature from all samples
		val featureList = sampleList
			.map({ case (tree, node, sampleIndex, label, features) => (label, features.filter(x => x._2 == feature).head) })

		// probability for each label occurrence in the node
		val qj = featureList
			.groupBy(_._1) /*group by label */
			.map(x => (x._2.length.toDouble / totalSamples))

		//System.out.println("feature:"+feature+"    candidate:"+candidate+"    "+h.uniform(10)+"     histogram:"+h.toString)

		// compute probability distribution for each child (Left,Right) and the current candidate with the specific label
		val qLj = featureList
			.filter({ case (label, feature) => feature._1 <= candidate })
			.groupBy(_._1) /*group by label */
			.map(x => (x._2.length.toDouble / totalSamples))

		val qRj = featureList
			.filter({ case (label, feature) => feature._1 > candidate })
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

}
