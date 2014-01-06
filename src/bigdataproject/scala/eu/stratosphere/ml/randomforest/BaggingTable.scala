package bigdataproject.scala.eu.stratosphere.ml.randomforest

import scala.collection.mutable.HashMap

case class BaggingTable  {
	var baggingTable = HashMap[Int,Int]()
	def create(table : Array[Int])  : this.type = {
	  table.foreach(i =>
	    	if( baggingTable.contains(i) ) {
	    	  val value =  baggingTable.get(i).get
	    	  baggingTable.put(i,value+1)
	    	}
	    	else
	    	  baggingTable.put(i, 1)
	      )
      this
	}	
	def contains( index : Int ) = {
	  baggingTable.contains(index)
	}
	def count(index : Int ) = {
	  if( contains(index) )
	  	baggingTable.get(index).get
	  else
	  	 0
	}
}