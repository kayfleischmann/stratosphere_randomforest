package bigdataproject.scala.eu.stratosphere.ml.randomforest

import eu.stratosphere.pact.common.plan.PlanAssembler
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription
import eu.stratosphere.scala._
import eu.stratosphere.scala.operators._
import scala.util.matching.Regex
import eu.stratosphere.pact.client.LocalExecutor

import util.Random
  
class buildDecisionTree( var nodeQueue : Array[TreeNode]) extends PlanAssembler with PlanAssemblerDescription with Serializable {

  
  override def getDescription() = {
	  "Usage: [inputPath] [outputPath] ([number_trees])"
  }
  
  override def getPlan(args: String*) = {
    val inputPath = args(0)
    val outputPath = args(1)
    val number_trees = args(2)

    val trainingSet = TextFile(inputPath)
    
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
    				.flatMap { case (index,label, features) => 
    					nodeQueue
    							.filter { node => node.baggingTable.contains(index) }
		    					.map( node => (node.treeId+"_"+node.nodeId+"_"+label,node.baggingTable.count( _ == index), features, label) )
    					}
    
    val nodeHistograms = treenode_samples
      .groupBy { _._1 }
      .reduceGroup { values => 
      			val buffered = values.buffered
      			val treeAndNode = buffered.head._1
      			val treeId = treeAndNode.split("_")(0).toInt
      			val nodeId = treeAndNode.split("_")(1).toInt        
      			val label =  treeAndNode.split("_")(2).toInt
      			
      			val histogram = values.buffered.map( v => v._3.split(" ") map( _.toDouble * v._2) ).reduceLeft( (x,y) => x.toList.zip(y).map(x=>x._1+x._2).toArray )

      			
      			(treeId, nodeId, label, histogram.length, treeAndNode )
      	}
      
    val sink = nodeHistograms.write( outputPath, CsvOutputFormat("\n",","))
    
    new ScalaPlan(Seq(sink))
  }
}
