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



class DecisionTreeBuilder(var minNrOfItems: Int, var featureSubspaceCount: Int, var numClasses : Int, var treeLevel : Int, var numHinstogramBuckets : Int = 10) extends Program with ProgramDescription with Serializable {

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

		var c3=0
		val nodequeue = inputNodeQueue map { line =>
			val values = line.split(",")
			val treeId = values(0).toLong
			val nodeId = values(1)
			
			(treeId, nodeId, line )
		}

		val samples = trainingSet map { line =>
			var firstSpace = line.indexOf(' ', 0)
			var secondSpace = line.indexOf(' ', firstSpace + 1)
			var thirdSpace = line.indexOf(' ', secondSpace + 1)
			
			val sampleIndex = line.substring(0, firstSpace).trim().toInt
			val label = line.substring(firstSpace, secondSpace).trim().toInt
			val values = line.substring(thirdSpace).trim().split(" ")

		  	c3=c3+1;
		  	if(c3%20000 == 0 ){
		  		System.out.println("samples counter "+c3)
			}
		  	
			(sampleIndex, label, values)
		}
		
		val nodesAndBaggingTable = nodequeue.flatMap { case(treeId,nodeId,line) =>
			val values = line.split(",")
			val baggingTable = values(5).trim
			val featureSpace = values(6).trim     	
			baggingTable
				.split(" ")
				.map(sampleIndex => (treeId, nodeId, sampleIndex.toInt, featureSpace, 1))
		}
		
	  var counter=0;
		val nodesAndSamples = samples
			.join(nodesAndBaggingTable)
			.where { x => x._1 }
			.isEqualTo { x => x._3 }
			.map {(sample,node) =>
			  	counter=counter+1;
			  	if(counter%20000 == 0 ){
			  		System.out.println("nodesAndSamples counter "+counter)
				}
			  	(
				node._1, //treeId
				node._2, //nodeid
				sample._1, //sampleIndex
				sample._2, //label
				//node._4.split(" ").map(_.toInt).toIndexedSeq.map(n => (sample._3.apply(n).toDouble, n)).toList, //features
				node._4.split(" ").map(n => (sample._3(n.toInt).toDouble, n.toInt)).toList,
				node._5 //count
				)
				
			}
       
	    nodesAndSamples.contract.setParameter(PactCompiler.HINT_LOCAL_STRATEGY, PactCompiler.HINT_LOCAL_STRATEGY_HASH_BUILD_SECOND)

	
        val nodeSampleFeatures  = nodesAndSamples
                        .flatMap  { case(treeId,nodeId,sampleIndex,label,features,count) =>
                                features.map({ case(featureValue,featureIndex) => 
                                          ( treeId,nodeId,featureIndex, sampleIndex, label, featureValue, count )})
                        }

                
										
		val nodeSampleFeatureHistograms = nodeSampleFeatures
							.map({ case ( treeId,nodeId,featureIndex,sampleIndex,label,featureValue, count) =>
									( treeId,nodeId,featureIndex,new Histogram(featureIndex, numHinstogramBuckets).update(featureValue, count) ) 
									})
								
		var mergeCount = 0
		val  nodeHistograms = nodeSampleFeatureHistograms
							.groupBy({ x => (x._1,x._2,x._3)})
							.reduce( {(left,right) =>
								  	mergeCount=mergeCount+1;
								  	if(mergeCount%20000 == 0 ){
								  		System.out.println("merge counter "+mergeCount)
									}
								  	(left._1, left._2, left._3, left._4.merge(right._4)) } )
							.flatMap( { nodeHistogram => 
									nodeHistogram._4.uniform(numHinstogramBuckets)
													.map({ splitCandidate => 
													  		( 	nodeHistogram._1, /*treeId */
													  			nodeHistogram._2, /*nodeId*/
													  			nodeHistogram._3, /*featureId*/
													  			splitCandidate) }) 
							})
		
	    var c1=0
		val nodeFeatureDistributions = nodeHistograms
									.join(nodeSampleFeatures)
									.where( { x => (x._1,x._2,x._3) })
									.isEqualTo { x => (x._1,x._2,x._3) }
									.map({ (nodeHistogram, nodeSampleFeature) =>
									  	c1=c1+1;
									  	if(c1%100000 == 0 ){
									  		System.out.println("nodeFeatureDistributions counter "+c1)
										}

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

									
	   
	   //nodeFeatureDistributions.contract.setParameter(PactCompiler.HINT_LOCAL_STRATEGY, PactCompiler.)

		// compute node distributions in a distributed fashion
									
		val nodeFeatureDistributions_qj = nodeSampleFeatures
  									.map({ x => (x._1, x._2, x._3, createLabelArray(numClasses, List( (x._5/*label*/, x._7 /*count*/ )))) })  									
 									.groupBy({x=>(x._1,x._2,x._3)})
									.reduce({(left,right)=>(left._1,left._2,left._3, left._4.zip(right._4).map(x=>x._1+x._2)) })
									.map({ x => (x._1, x._2, x._3, x._4.toList.mkString(" ") ) })

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
	    var c2=0
		val nodeDistributions = nodeFeatureDistributions_qj 
		  						.cogroup(nodeFeatureDistributions_qjL)
								.where( { x => (x._1,x._2, x._3) })
								.isEqualTo { x =>  (x._1,x._2, x._3)}
								.flatMap({ (qj, qjL) => 
								      var node1 = qj.next
									  var empty_node : (Long,String,Int,Double,String) =  (0, "0", 0, 0.0, "null")
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
								  var node3 : (Long,String,Int,Double,String) = (0, "0", 0, 0.0, "null")
								  if( qjR.hasNext )
								     node3 = qjR.next
								  (node12._1, node12._2, node12._3, node12._4,  node12._5, node12._6, node3 )
								})
								.map({ case (treeId,nodeId,featureIndex,splitCandidate, qj, qjL, qjR ) =>
								  
								  	c2=c2+1;
							  		System.out.println("nodeDistributions counter "+c2)
								  	
									var p_qj = qj._4.split(" ").map(_.toDouble )
									var p_qjL = Array[Double]()
									var p_qjR = Array[Double]()

									if(qjL._5!="null")
										p_qjL = qjL._5.split(" ").map(_.toDouble)
									if(qjR._5!="null")
										p_qjR = qjR._5.split(" ").map(_.toDouble)
									
									val tau = 0.5
									
									val totalSamples = p_qj.sum.toInt
									val totalSamplesLeft = p_qjL.sum.toInt
									val totalSamplesRight = p_qjR.sum.toInt
									val bestLabel = p_qj.zipWithIndex.maxBy(_._1)._2
									val bestLabelProbability = p_qj(bestLabel) / totalSamples.toDouble;
									
									var quality = quality_function( tau, 
																	p_qj.map( _ /totalSamples).toList, 
																	p_qjL.map( _ /totalSamples).toList, 
																	p_qjR.map( _ /totalSamples).toList);
									
									(treeId,nodeId,(featureIndex,splitCandidate, quality, totalSamplesLeft, totalSamplesRight, bestLabel, bestLabelProbability) ) 									
								});
				
		// compute the best split for each tree-node
		val bestTreeNodeSplits = nodeDistributions
								// group by treeIdnodeIdFeatureIndex and compute the max (best quality)
								.groupBy({x=>(x._1, x._2)})
								.reduce({ (left,right) =>  
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
									  	
									  	// TREE-LEAF: if stopping criterion create node in output tree as a labeled one, but without a featureId
								  		if(isStoppingCriterion(x))
								  			(treeId, nodeId, -1/*featureId*/, 0.0 /*split*/, label, ""/*baggingTable*/, "" /*featureList*/) 
								  		// TREE-NODE: otherwise we have a nice split feature without a label
								  		else
								  			(treeId, nodeId, x._3._1.toInt/*featureId*/, x._3._2.toDouble/*split*/, -1, ""/*baggingTable*/, "" /*featureList*/) 
									})
								
		val nodestobuild = bestTreeNodeSplits.filter({ z  => !isStoppingCriterion(z) })
 						
		var c4=0
		// compute new nodes to build
		val nodeWithBaggingTable = nodesAndSamples
							.join( nodestobuild )
							.where( x => (x._1,x._2 ) )
							.isEqualTo { x => (x._1,x._2) }
							.map({ (nodeAndSamples,bestSplits) =>

								  	c4=c4+1;
								  	if(c4%20000==0)
								  		System.out.println("nodeWithBaggingTable counter "+c4)
								  	
								  	var sampleFeature = nodeAndSamples._5.find(x=>x._2==bestSplits._3._1)

							  		(	bestSplits._1 /*treeId*/, 
							  			bestSplits._2 /*nodeId*/, 
							  			bestSplits._3._1 /*featureIndex*/, 
							  			bestSplits._3._2 /*splitCandidate*/, 
							  			nodeAndSamples._3 /*sampleIndex*/,  
							  			nodeAndSamples._4 /*label*/, 
							  			sampleFeature.get._1.toDouble <= bestSplits._3._2 /* isLeft */,  
							  			bestSplits._3._1 /*featureIndex*/, 
							  			nodeAndSamples._6 /*sampleCount*/ )
							})
		nodeWithBaggingTable.contract.setParameter(PactCompiler.HINT_LOCAL_STRATEGY, PactCompiler.HINT_LOCAL_STRATEGY_HASH_BUILD_SECOND)
						
		var c5=0				
		val NodesWithBaggingTables = nodeWithBaggingTable
							.map({ case(treeId,nodeId, featureIndex, _, sampleIndex, _, isLeft, _, count)=> 
							  				(treeId,nodeId,featureIndex, (0 until count).toList.map(x=>sampleIndex), isLeft) })
							.groupBy({x=>(x._1,x._2,x._5)})
							.reduceGroup( baggingList => {
							   val buffered = baggingList.buffered
							   val keyValues = buffered.head
							   val treeId = keyValues._1
                               val featureIndex = keyValues._3
                               var isLeftNode = keyValues._5
                               
							  var nodeId : BigInt = (( BigInt(keyValues._2) + 1) * 2)
							  if (isLeftNode)
							  		nodeId = nodeId - 1

                              (treeId, nodeId.toString, featureIndex,  0.0 /*split*/, -1, baggingList.flatMap({ x=> x._4 }) /*baggingTable*/, "" /*featureList*/ )
							})

		
		// output to tree file if featureIndex != -1 (node) or leaf (label detected)  
		val finaTreeNodesSink = finalnodes
									.write(outputTreePath, CsvOutputFormat(newLine, ","))
		
									
		// prepare the treeId,nodeId and featureList for next round
		// map to new potential nodeIds
		val nodeFeatures = inputNodeQueue flatMap { line =>
			val values = line.trim.split(",")
			val treeId = values(0).toLong
			val nodeId : BigInt = BigInt( values(1) )
			val features = values(7)
			
			val leftNodeId : BigInt = ((nodeId + 1L) * 2) - 1
			val rightNodeId : BigInt = ((nodeId + 1L) * 2)

			List((treeId, leftNodeId.toString, features), (treeId, rightNodeId.toString, features))
		}
							
		// filter nodes to build, join the last featureList from node and remove :q
		val nodeResultsWithFeatures =  NodesWithBaggingTables
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
												    nodeResult._6.mkString(" "), 
												    featureSpace.mkString(" "), 
												    features.mkString(" "))
											})
											
											
		// output to tree file if featureIndex != -1 (node) or leaf (label detected)  
		val treeLevelSink = finalnodes .write(outputTreePath, CsvOutputFormat(newLine, ","))
									
		// output nodes to build if 
		val nodeQueueSink = nodeResultsWithFeatures .write(outputNodeQueuePath, CsvOutputFormat(newLine, ","))
		
		// debug
		val bestSplitSink = bestTreeNodeSplits .write("/home/kay/rf/rf_bestsplits_"+treeLevel, CsvOutputFormat(newLine, ","))
		
		val nodeDistributionsSink =  nodeDistributions .write("/home/kay/rf/rf_nodedistributions_"+treeLevel, CsvOutputFormat(newLine, ","))

		val nodeHistogramsSink = nodeHistograms
									.map({x=>(x._1, x._2, x._3, x._4.toString )})
									.write("/home/kay/rf/rf_nodehistograms"+treeLevel, CsvOutputFormat(newLine, ","))

		
		new ScalaPlan(Seq(treeLevelSink,nodeQueueSink,bestSplitSink,nodeDistributionsSink,nodeHistogramsSink ))
	}


	def impurity(q: List[Double]) = {
		gini(q)
		//entropy(q)
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

	def isStoppingCriterion( x : (Long, String, (Int/*featureIndex*/, Double /*splitCandidate*/, Double /*quality*/, Int /*totalSamplesLeft*/, Int /*totalSamplesRight*/,  Int /*bestLabel*/, Double /*bestLabelProbability*/ ) ) ) = {
	  if( x._3._4 == 0 ||  x._3._5 == 0 || x._3._4 < minNrOfItems || x._3._5 < minNrOfItems  ){
	    true
	  }
	  else
		  false
	}
}
