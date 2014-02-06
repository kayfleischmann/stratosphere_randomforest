package bigdataproject.scala.eu.stratosphere.ml.randomforest
package bigdataproject.scala.eu.stratosphere.ml.randomforest


import eu.stratosphere.api.common.Program
import eu.stratosphere.api.common.ProgramDescription
import eu.stratosphere.api.scala._
import eu.stratosphere.api.scala.operators._

import java.io._


class SampleCountEstimator extends Program with ProgramDescription with Serializable {
  override def getDescription() = {
    "Usage: [inputPath] [outputPath]"
  }
  override def getPlan(args: String*) = {
    val newLine = System.getProperty("line.separator");
    val inputPath = args(0)
    val outputPath = args(1)
    val trainingSet = TextFile(inputPath)
    val samples = trainingSet map { line => (1) } reduce { (x,y)=>(x+y) }
    val totalCountSink = samples.write(outputPath, CsvOutputFormat(newLine, ","))
    new ScalaPlan(Seq(totalCountSink))
  }
}