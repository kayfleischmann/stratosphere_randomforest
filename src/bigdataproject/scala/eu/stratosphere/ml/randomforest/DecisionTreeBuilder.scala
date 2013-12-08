package bigdataproject.scala.eu.stratosphere.ml.randomforest

import eu.stratosphere.pact.common.plan.PlanAssembler
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription
import eu.stratosphere.scala._
import eu.stratosphere.scala.operators._
import scala.util.matching.Regex
import eu.stratosphere.pact.client.LocalExecutor

import util.Random
  
class DecisionTreeBuilder( var nodeQueue : Array[TreeNode]) extends PlanAssembler with PlanAssemblerDescription with Serializable {

  
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
	  	(index,label,features)
    }
    

    // for each sample and each tree and node, create an histogram tuple 
    val treenode_samples = samples  
    				.flatMap { case (index,label, features ) => 
    					nodeQueue
    							.filter { node => node.baggingTable.contains(index) }
		    					.map( node => 
		    					  		(	node.treeId+"_"+node.nodeId+"_"+label, 
		    					  			node.baggingTable.count( _ == index), 
		    					  			features.split(" ").zipWithIndex.filter( x => node.features.contains(x._2) ).map( f => f._1 ).mkString,
		    					  			label) )
    					}
    
    val nodeClassHistograms = treenode_samples
      .groupBy { _._1 }
      .reduceGroup { values =>
        		val buckets = 10
      			val buffered = values.buffered
      			val treeAndNode = buffered.head._1
      			val treeId = treeAndNode.split("_")(0).toInt
      			val nodeId = treeAndNode.split("_")(1).toInt        
      			val label =  treeAndNode.split("_")(2).toInt
      			
      			// generate class histograms for each tree-node			
      			var histograms = scala.collection.mutable.Buffer[(Int,Histogram)]()
      			buffered.map( s => {
      			  val key=s._1
      			  val count=s._2
      			  val features=s._3
      			  val label=s._4
      			  if( histograms.filter( p => p._1 == label ).length == 0 ) histograms += ( (label, new Histogram(buckets)) )
      			  
      			})
      			
      			//val histogram = values.buffered.map( v => v._3.split(" ") map( _.toDouble * v._2) ).reduceLeft( (x,y) => x.toList.zip(y).map(x=>x._1+x._2).toArray )

      			(treeId+"_"+nodeId, label, 1 /*histogram.length*/ )
      	}
      

    val nodeHistograms = nodeClassHistograms
      .groupBy { _._1  }
      .reduceGroup { values =>
      			val buffered = values.buffered
      			val treeAndNode = buffered.head._1
      			val treeId = treeAndNode.split("_")(0).toInt
      			val nodeId = treeAndNode.split("_")(1).toInt 
      			// select random features (m)
      			
      			// find the best split
      			// store this in separate file
      			
      			// generate new nodes if necessary

      			// store new node
      			(nodeId,treeId,values.length)
      		}
      
    val sink = nodeClassHistograms.write( outputPath, CsvOutputFormat("\n",","))
    
    //val sink2 = nodeHistograms.write( outputPath, CsvOutputFormat("\n",","))

    
    new ScalaPlan(Seq(sink))
  }
  
}
