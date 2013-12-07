package bigdataproject.scala.eu.stratosphere.ml.randomforest

case class TreeNode ( 
	treeId : Int, 
	nodeId : Int, 
	baggingTable : Array[Int],
	features : Set[Int],
	candidateFeatures : Array[Int],
	splitFeature : Int,
	isLeaf : Boolean ){
}