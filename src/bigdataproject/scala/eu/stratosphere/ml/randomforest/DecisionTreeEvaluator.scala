package bigdataproject.scala.eu.stratosphere.ml.randomforest

import eu.stratosphere.client.LocalExecutor
import eu.stratosphere.api.common.Plan
import eu.stratosphere.api.common.Program
import eu.stratosphere.api.common.ProgramDescription
import eu.stratosphere.api.scala._
import eu.stratosphere.api.scala.operators._


import scala.util.matching.Regex
import util.Random
  
class DecisionTreeEvaluator() extends Program with ProgramDescription with Serializable {

  
  override def getDescription() = {
	  "Usage: [inputPath] [treePath] [outputPath] ()"
  }
  
  override def getPlan(args: String*) = {
    val inputPath = args(0)
    val treePath = args(1)
    val outputPath = args(2)
    
    val inputFile = TextFile(inputPath)
    val treeFile = TextFile(treePath)
    
    val treeNodes = treeFile
    		.map({nodedata =>
		        val treeId = nodedata.split(",")(0)	        
		        (treeId,nodedata)
	      		})
	      	.groupBy( _._1 )
	      	.reduceGroup( values => {
	      		val buffered = values.buffered.toList
	      		val treeId = buffered.head._1
	      		val nodes = buffered;
	      		(nodes.map({x=>x._2}).mkString(";"))
	      	} )
    val treeEvaluations = inputFile
    			.cross(treeNodes)
    			.map((line, tree) => {    				
				 val nodes = tree.split(";").map(node => {
				                val nodeData = node.split(",").map(_.trim())
				                new TreeNode(nodeData(0).toLong, BigInt(nodeData(1)), null, null, null, nodeData(2).toInt, nodeData(3).toDouble, nodeData(4).toInt)
				              })				          
		  val values = line.split(" ")
		  val index = values.head.trim().toInt
		  val label = values.tail.head.trim().toInt
		  val features = values.tail.tail
	      
		  var currentNodeIndex : BigInt = 0;
	      var labelVote = -1;
	      do
	      {
	         //System.out.println(values.zipWithIndex.toList);
	         //System.out.println(currentNodeIndex);
	         //System.out.println(nodes.toList);
			  val currentNode = nodes.find(_.nodeId == currentNodeIndex).orNull
			  labelVote = currentNode.label
			  
			  if (labelVote == -1)
			  {
			    //right child:
			    currentNodeIndex = ((currentNode.nodeId + 1) * 2)
			    
			    //left child:
				if (features(currentNode.splitFeatureIndex).toDouble <= currentNode.splitFeatureValue)
				    currentNodeIndex -= 1
			  }
	    	  
	      }while (labelVote == -1)
		  
	      (index,labelVote,label)
	    })
	    
	val forestEvaluations = treeEvaluations
		.filter(_._2 > -1)
		.groupBy(_._1)
		.reduceGroup( values => {
			val buffered = values.buffered.toList
			val dataItemIndex = buffered.head._1
			val actualLabel = buffered.head._3
			val winningLabelGuess = buffered
				.groupBy(_._2)
				.map(labelOccurance => (labelOccurance._1, labelOccurance._2.length))
				.maxBy(_._2)._1
			
			//group by label, then count occurances
			(dataItemIndex, winningLabelGuess, actualLabel)
		})
	  
    val sink = forestEvaluations.write(outputPath, CsvOutputFormat("\n",","))
    new ScalaPlan(Seq(sink))
  }
}
