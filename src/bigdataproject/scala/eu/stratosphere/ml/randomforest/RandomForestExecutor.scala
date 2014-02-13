package bigdataproject.scala.eu.stratosphere.ml.randomforest


object RandomForestExecutor {
 
  def main(args: Array[String]) {
    if(args.length < 3){
      println("not enough parameters")
      println("dest-path data-source build|eval number-trees [remoteJar remoteHost remotePort]")
      return
    }

    val mode=args(0)
    val path=args(1)
    val data=args(2)
    var remoteJar : String =null
    var remoteHost : String =null
    var remotePort : Int =0

    if(mode == "build"){
      val trees=args(3).toInt

      if(args.length > 4)
        remoteJar=args(3)
      if(args.length > 5)
        remoteHost=args(4)
      if(args.length > 6)
        remotePort=args(5).toInt

    new RandomForestBuilder(remoteJar,remoteHost,remotePort).build(
        path,
        data,
        trees
        )
    } else  if(mode == "eval"){

      new RandomForestBuilder().eval(
        data,
        path+"rf_output_tree",
        path+"rf_output_evaluation"
      )
    } else {
      print("unknown mode")
    }
    System.exit(0)
  }
 
}