package bigdataproject.scala.eu.stratosphere.ml.randomforest

case class Histogram(maxBins : Integer) {
  var bins = scala.collection.mutable.Buffer[(Double,Int)]()
  def getBins = bins
 
  def uniform( Bnew : Integer ) = {
    val u = scala.collection.mutable.Buffer[Double]()
    val sums = bins.map(x=>sum(x._1))
    val binSum = bins.map(_._2).sum
    for(j <- 1 until Bnew){
     val s= (j.toDouble/Bnew) * binSum.toDouble
     val i = sums.zipWithIndex.filter( x=>(x._1<s) ).last._2 - 1
     val d = math.abs(s - sums(i))
     val pi = bins(i)
     val pi1 = bins(i+1)
     val a = pi1._2 - pi._2
     val b = 2*pi._2
     val c = -2*d
     val z = (-b + math.sqrt(b*b - 4*a*c)) / (2*a);
     val uj = pi._1+(pi1._1 - pi._1)*z
     u += uj
    }
    u.toSet
  }
  def sum(b:Double) = {
    val pos =  bins.zipWithIndex.filter( x=>(x._1._1<b) )
    if(pos.length==0)
     0
    else if( pos.length == bins.length )
     bins.map(_._2).sum
    else {
      val i = pos.last._2
      val bi = bins(i)
      val bi1 = bins(i+1)
      val mb =  bi._2 + ((bi1._2-bi._2)/(bi1._1-bi._1)) * (b - bi._1 ) 
      var s = ((bi._2+mb) / 2) * ((b - bi._1 )/(bi1._1-bi._1))
      for (j <- 0 until i)  s += bins(j)._2
      s = s + bins(i)._2 / 2
      s
    }
  }  
  def merge(h:Histogram) = {
    val h2 = new Histogram(maxBins)
    h2.bins = bins.clone
    h.bins.foreach( b => 
      h2.update(b._1, b._2 )
    )
    h2
  }  
  def update ( p : Double ) : this.type = {
    update(p,1)
    this
  }
  def update ( p : Double, c : Int ) : this.type = {
    var bin = bins.zipWithIndex.find(pm => pm._1._1 == p)
    if( bin != None )
      bins(bin.head._2) = (bin.head._1._1,bin.head._1._2+c)
    else{
     bins +=( (p, c) )
     sort
     compress_one
    }
    this
  }
  private def sort {
     bins=bins.sortWith( (e1, e2) => e1._1 <= e2._1 )
  }
  private def compress_one {
    // only compress if the numer of elements exeeds 
    // the maximum bins allowed
    if( bins.length > maxBins ){
      val q = bins.take(bins.length-1).zip(bins.tail).zipWithIndex.sortWith( (x,y) => (x._1._1._1-x._1._2._1)  > (y._1._1._1-y._1._2._1) ).head._2
      val qi = bins.remove(q)
      val qi1 = bins(q)
      bins(q) = ( (qi._1*qi._2 + qi1._1*qi1._2)/(qi._2+qi1._2), qi._2+qi1._2)
    }
  }
  override def toString = {
    maxBins+";"+bins.map(x=>""+x._1+" "+x._2).mkString(",")
  }
}
object Histogram {
  def fromString(str:String) = {
    val values = str.split(";")
    val maxBins=values(0).toInt
    val bins = values(1).split(",").map( x=> (x.split(" ")(0).toDouble, x.split(" ")(1).toInt ) )
    val h = new Histogram(maxBins)
    bins.foreach( b => h.update(b._1, b._2) )
    h
  }
}