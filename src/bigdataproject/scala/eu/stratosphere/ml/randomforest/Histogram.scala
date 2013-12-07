package bigdataproject.scala.eu.stratosphere.ml.randomforest
  
case class Histogram(numBuckets : Integer) {
  val buckets = new Array[Long](numBuckets)
  def update ( value : Double ){
  }
}