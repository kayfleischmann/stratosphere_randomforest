package bigdataproject.scala.eu.stratosphere.ml.randomforest

import eu.stratosphere.pact.common.plan.PlanAssembler
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription
import eu.stratosphere.scala._
import eu.stratosphere.scala.operators._
import scala.util.matching.Regex
import eu.stratosphere.pact.client.LocalExecutor
import java.util.ArrayList

  
class buildDecisionTree extends PlanAssembler with PlanAssemblerDescription with Serializable {
  
  override def getDescription() = {
	  "Usage: [inputPath] [outputPath] ([number_trees])"
  }
  
  override def getPlan(args: String*) = {
    val inputPath = args(0)
    val outputPath = args(1)
    val number_trees = 1 //args(2)

    
	var nodeQueue = scala.collection.mutable.ArrayBuffer.empty [Node]
    val trainingSet = TextFile(inputPath)
    val labels = List(0,1,2,3,4,5,6,7,8,9)
    
   // nodeQueue.+( new Node())

    // prepare string input for next operations
    val samples = trainingSet map { line => 
	  val values = line.split(" ")
	  val index = values.head.toInt
	  val label = values.tail.head.toInt
	  val features = values.tail.tail.map{ a =>  a.toDouble }
	  	(index,label,features)
    }

    
    // group by class label = samples    						  
    val local_histogram = samples  
    				.map { case (index,label, features) => 
    				  	// filter the nodes which contains the sample 
    					nodeQueue.filter { node => node.baggingTable.samples.contains(index) }
    					
    					// for each node (which has to built) filter these which contains the index, update histogram
    					.map({ node => { 
    							// zip histogram with featureIndex
    					  		node.featureHistograms.zip(node.randomFeatures).map( h =>{ 
    					  						val featureIndex = h._2
    					  						val featureClassHistograms = h._1
    					  						featureClassHistograms.zip(labels).map( t => t._1.update( features(featureIndex) ) )
    					  						h
    					  					})
    								}
    					  	})
    				}
					//.groupBy { _._2 }
					//.reduceGroup( values => ""+values.buffered.head._2+" "+values.length )
					//.map( values => ""+values.buffered.head._2+" "+values.length )    

					//localHistogram
	/*
	val out = scala.collection.mutable.ArrayBuffer.empty [ (Int,Int,Double) ]
	*/
	
	def formatOutput = (t1: Int, t2: Int) => "%s %d".format(t1, t2)
    						
    //CsvOutputFormat("\n", ",")
    val sink = local_histogram.write( outputPath, CsvOutputFormat("\n",","))
    
    new ScalaPlan(Seq(sink))
  }
    
  case class BaggingTable(samples:List[Int] )
  case class Node( treeId : Int, baggingTable : BaggingTable, featureHistograms : List[List[Histogram]], randomFeatures : List[Int] )
  case class Histogram(numBuckets : Integer) {
	val buckets = new Array[Long](numBuckets)
	
	def update ( value : Double ){
	}
  }
}
