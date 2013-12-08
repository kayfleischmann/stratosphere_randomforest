package bigdataproject.scala.eu.stratosphere.ml.randomforest

case class TreeNode ( 
	treeId : Int, 
	nodeId : Int, 
	
	// bagging table
	baggingTable : Array[Int],
	// list of features left for random feature-selection (m)
	features : Set[Int],
	candidateFeatures : Array[Int],
	featureSpace : Array[Int],
	splitFeature : Int,
	
	isLeaf : Boolean ){
}