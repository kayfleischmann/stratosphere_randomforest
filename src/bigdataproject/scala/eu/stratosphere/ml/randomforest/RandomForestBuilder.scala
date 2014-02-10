package bigdataproject.scala.eu.stratosphere.ml.randomforest

import eu.stratosphere.client.LocalExecutor
import scala.util.Random
import java.io._
import scala.io.Source
import scala.collection.mutable.Buffer
import org.apache.log4j.Level
import eu.stratosphere.client.PlanExecutor
import eu.stratosphere.client.RemoteExecutor
import eu.stratosphere.core.fs.FileSystem
import eu.stratosphere.core.fs.Path
import bigdataproject.scala.eu.stratosphere.ml.randomforest.SampleCountEstimator
import java.net.URI


class RandomForestBuilder(val remoteJar : String = null,
                          val remoteJobManager : String = null,
                          val remoteJobManagerPort : Int = 0 ) {

  def getSampleCount( ex : PlanExecutor, filename: String, outputPath : String ): Int = {
    val outputSampleCountPath = outputPath+"/samples_count"
    val plan = new SampleCountEstimator().getPlan( filename, outputSampleCountPath )
    val runtime = ex.executePlan(plan)

    val fs : FileSystem = FileSystem.get(new File(outputSampleCountPath).toURI)
    val is : InputStream = fs.open(new Path(outputSampleCountPath) )
    val br : BufferedReader = new BufferedReader(new InputStreamReader(is))
    val line=br.readLine()
    line.toInt
	}
	def getFeatureCount(filename: String): Int = {
    val fs : FileSystem = FileSystem.get(new File(filename).toURI)
    val is : InputStream = fs.open(new Path(filename) )
    val br : BufferedReader = new BufferedReader(new InputStreamReader(is))
    val line=br.readLine()
		try {
      line.split(" ").tail.tail.size
		} finally {
		}
	}

	def generateFeatureSubspace(randomCount: Int, maxRandomNumber: Int): Array[Int] = {
		var features = Buffer[Int]();
		// Generate an arrayList of all Integers
		for (i <- 0 until maxRandomNumber) {
			features += i;
		}
		generateFeatureSubspace(randomCount, features)
	}

	def generateFeatureSubspace(randomCount: Int, features: Buffer[Int]): Array[Int] = {
		var arr: Array[Int] = Array()
		arr = Array(randomCount)
		arr = Array.fill(randomCount)(0)
		for (i <- 0 until randomCount) {
			val random = new Random().nextInt(features.length);
			arr(i) = features.remove(random);
		}
		arr;
	}
	
	def eval(inputFile: String, treeFile: String, outputFile: String) = {
    val fs : FileSystem = FileSystem.get(new File(inputFile).toURI)
    val inputPath = inputFile
		val treePath = treeFile
		val outputPath = outputFile

    // prepare executor
    var ex : PlanExecutor = null
    if( remoteJar == null ){
      val localExecutor = new LocalExecutor();
      localExecutor.start()
      ex = localExecutor
      LocalExecutor.setLoggingLevel(Level.ERROR)
    } else {
      ex = new RemoteExecutor(remoteJobManager, remoteJobManagerPort, remoteJar );
    }

		val plan = new DecisionTreeEvaluator().getPlan(inputPath, treePath, outputPath)
		val runtime = ex.executePlan(plan)

    val src =Source.fromInputStream(fs.open(new Path(new File(outputFile).toURI)))
		try {
			val lines = src.getLines.map(_.split(",").map(_.toInt)).toList

			System.out.println("statistics");
			System.out.println("total results: " + lines.length)
			val correct = lines.filter(x => x(1) == x(2)).length
			System.out.println("correct: " + correct)
			val wrong = lines.filter(x => x(1) != x(2)).length
			System.out.println("wrong: " + wrong)
			System.out.println("percentage: " + (correct.toDouble * 100 / lines.length.toDouble))
		} finally {
			src.close()
		}

		System.exit(0)
	}

	def build(outputPath: String, inputPath: String, inputNodeQueuePath: String, outputNodeQueuePath: String, outputTreePath: String, numTrees: Int) = {

    // prepare executor
    var ex : PlanExecutor = null
    if( remoteJar == null ){
      val localExecutor = new LocalExecutor();
      localExecutor.start()
      ex = localExecutor
      LocalExecutor.setLoggingLevel(Level.ERROR)
    } else {
      ex = new RemoteExecutor(remoteJobManager, remoteJobManagerPort, remoteJar );
    }


    val fs : FileSystem = FileSystem.get(new File(outputPath).toURI)

		// start measuring time
		val t0 = System.currentTimeMillis

    System.out.println(inputPath)

		var nodesQueue = Buffer[TreeNode]()
		val totalFeatureCount = getFeatureCount(inputPath)
		val featureSubspaceCount = Math.round(Math.log(totalFeatureCount).toFloat + 1);

		// add node to build for each tree
		val sampleCount = getSampleCount(ex, inputPath, outputPath)
		for (treeId <- 0 until numTrees) {
			// TODO: the features left is the whole set minus still used best-splits
			val features = (0 until totalFeatureCount).toArray

			// randomized
			val featureSubspace = DecisionTreeUtils.generateFeatureSubspace(featureSubspaceCount, totalFeatureCount)
			nodesQueue += new TreeNode(treeId, 0, features, featureSubspace, -1, -1, -1)
		} //for

		// write the initial nodes to file to join in the iteration
		writeNodes(nodesQueue, new File(inputNodeQueuePath).toURI(), sampleCount);

		// if next level, read from file which node has to be split
		// each line treeId,nodeId, featuresIndicies, baggingTable

		var nodeQueueSize = 0
		var level = 0
		var totalNodes = nodesQueue.length

		// do some cleanup stuff
    fs.delete(new Path(new File(outputTreePath).toURI), false )

		val level_outputTreePath = outputTreePath + "CurrentLevel"

		do {
			val plan = new DecisionTreeBuilder(70, featureSubspaceCount, level ).getPlan(
				inputPath,
				inputNodeQueuePath,
				outputNodeQueuePath,
				level_outputTreePath,
				numTrees.toString,
				outputPath)
			val runtime = ex.executePlan(plan)
			
			// delete old input node queue
      fs.delete(new Path(new File(inputNodeQueuePath).toURI), false )


			// change output nodequeue to input queue
      fs.rename(new Path(new File(outputNodeQueuePath).toURI), new Path(new File(inputNodeQueuePath).toURI))

			// check how many nodes to build
			nodeQueueSize = Source.fromInputStream(fs.open(new Path(new File(inputNodeQueuePath).toURI))).getLines().length

			// increment for next level
			level = level + 1;
			totalNodes += nodeQueueSize

      val is : InputStream = fs.open(new Path(level_outputTreePath) )
      val os : OutputStream = fs.create(new Path(outputTreePath), true )
      val newLine = System.getProperty("line.separator");
      val fw = new OutputStreamWriter(os)

      // append output data into global tree-file
      val br : BufferedReader = new BufferedReader(new InputStreamReader(is))
      var line=br.readLine()
      do {
          fw.write(line)
          fw.write(newLine)
          line = br.readLine();
      } while(line != null)
			fw.close()

      // delete temporal file
      fs.delete(new Path(new File(level_outputTreePath).toURI), false )

		} while (nodeQueueSize > 0)


		// stop measuring time
		val t1 = System.currentTimeMillis

		System.out.println("statistics");
		System.out.println("build-time: " + ((t1 - t0) / 1000.0) / 60.0 + "mins")
		System.out.println("samples: " + sampleCount)
		System.out.println("features per sample: " + totalFeatureCount)
		System.out.println("trees: " + numTrees)
		System.out.println("tree-levels (iterations): " + (level - 1))

		System.exit(0)
	}

	// write node-queue efficiently to file
	// line format:
	// treeID, nodeId, baggingTable, featureSpace, features
	def writeNodes(nodes: Buffer[TreeNode], outputPath: URI, baggingTableSize : Int) {
    val fs : FileSystem = FileSystem.get(outputPath)
    val os : OutputStream = fs.create(new Path(outputPath), true )
		val newLine = System.getProperty("line.separator");
    val fw = new OutputStreamWriter(os)
		try {
			for (i <- 0 until nodes.length) {
				val node = nodes(i)
        fw.write(node.treeId + ",")
        fw.write(node.nodeId + ",")
        fw.write(node.splitFeatureIndex + ",")
        fw.write(node.splitFeatureValue + ",")
        fw.write(node.label + ",")
				
				for (i <- 0 until baggingTableSize)
				{
          fw.write(Random.nextInt(baggingTableSize) + " ")
				}

        fw.write(",")
        fw.write(node.featureSpace.mkString(" ") + ",")
        fw.write(node.features.mkString(" "));
				if (i != nodes.length - 1)
          fw.write(newLine)
			}
		} finally {
      fw.close()
		}
	}

}