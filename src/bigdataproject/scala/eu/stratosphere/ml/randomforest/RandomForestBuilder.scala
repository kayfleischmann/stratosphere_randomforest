package bigdataproject.scala.eu.stratosphere.ml.randomforest

import eu.stratosphere.client.LocalExecutor
import scala.util.Random
import java.util.ArrayList
import java.io.File
import scala.io.Source
import scala.collection.mutable.Buffer
import java.io.FileWriter
import java.io.BufferedInputStream
import java.io.FileInputStream
import org.apache.log4j.Level

class RandomForestBuilder {
	def getSampleCount(filename: String): Int = {
		val src = io.Source.fromFile(filename)
		try {
			src.getLines.size.toInt
		} finally {
			src.close()
		}
	}
	def getFeatureCount(filename: String): Int = {
		val src = io.Source.fromFile(filename)
		try {
			src.getLines.find(_ => true).orNull.split(" ").tail.tail.size
		} finally {
			src.close()
		}
	}

	def generateRandomBaggingTable(number: Int) = {
		var baggingTable = new BaggingTable
		for (i <- 0 until number) {
			baggingTable.add(Random.nextInt(number))
		}
		baggingTable
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
			var random = new Random().nextInt(features.length);
			arr(i) = features.remove(random);
		}
		arr;
	}

	def eval(inputFile: String, treeFile: String, outputFile: String) = {
		val inputPath = new File(inputFile).toURI().toString()
		val treePath = new File(treeFile).toURI().toString()
		val outputPath = new File(outputFile).toURI().toString()

		val ex = new LocalExecutor()
		LocalExecutor.setLoggingLevel(Level.ERROR)
		ex.start()
		val plan = new DecisionTreeEvaluator().getPlan(inputPath, treePath, outputPath)
		val runtime = ex.executePlan(plan)

		val src = io.Source.fromFile(outputFile)
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

		// start measuring time
		val t0 = System.currentTimeMillis

		System.out.println("building random forest on stratosphere started");
		var nodesQueue = Buffer[TreeNode]()
		val totalFeatureCount = getFeatureCount(inputPath)
		var featureSubspaceCount = Math.round(Math.log(totalFeatureCount).toFloat + 1);

		// add node to build for each tree
		val sampleCount = getSampleCount(inputPath)
		for (treeId <- 0 until numTrees) {
			// TODO: the features left is the whole set minus still used best-splits
			var features = (0 until totalFeatureCount).toArray

			// randomized
			var featureSubspace = DecisionTreeUtils.generateFeatureSubspace(featureSubspaceCount, totalFeatureCount)
			val randomSamples = generateRandomBaggingTable(sampleCount)
			nodesQueue += new TreeNode(treeId, 0, randomSamples, features, featureSubspace, -1, -1, -1)
		} //for

		System.out.println("write initial nodequeue to build");

		// write the initial nodes to file to join in the iteration
		writeNodes(nodesQueue, inputNodeQueuePath);

		// if next level, read from file which node has to be split
		// each line treeId,nodeId, featuresIndicies, baggingTable

		println("Reading input from " + inputPath)
		println("Writing node-queue output to " + outputNodeQueuePath)
		println("Writing trees output to " + outputTreePath)

		// generate plan with a distributed nodesQueue
		val ex = new LocalExecutor()
		LocalExecutor.setLoggingLevel(Level.ERROR)
		ex.start()

		var nodeQueueSize = 0
		var level = 0
		var totalNodes = nodesQueue.length

		// do some cleanup stuff
		new File(outputTreePath).delete
		val level_outputTreePath = outputTreePath + "CurrentLevel"

		do {
			val plan = new DecisionTreeBuilder(70, featureSubspaceCount).getPlan(
				new File(inputPath).toURI().toString(),
				new File(inputNodeQueuePath).toURI().toString(),
				new File(outputNodeQueuePath).toURI().toString(),
				new File(level_outputTreePath).toURI().toString(),
				numTrees.toString)
			val runtime = ex.executePlan(plan)
			println("runtime: " + runtime)

			// delete old input node queue
			new File(inputNodeQueuePath).delete()

			// change output nodequeue to input queue
			new File(outputNodeQueuePath).renameTo(new File(inputNodeQueuePath))

			// check how many nodes to build
			nodeQueueSize = Source.fromFile(inputNodeQueuePath).getLines().length
			// increment for next level
			level = level + 1;
			totalNodes += nodeQueueSize

			//store nodes for tree file
			val fw = new FileWriter(new File(outputTreePath), true)
			fw.write(Source.fromFile(level_outputTreePath).getLines().mkString(System.getProperty("line.separator")))
			fw.write(System.getProperty("line.separator"))
			fw.close()
			new File(level_outputTreePath).delete()

		} while (nodeQueueSize > 0)

		ex.stop();

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
	def writeNodes(nodes: Buffer[TreeNode], outputPath: String) {
		val fw = new FileWriter(outputPath, false)
		val newLine = System.getProperty("line.separator");
		try {
			for (i <- 0 until nodes.length) {
				var node = nodes(i)
				fw.write(node.treeId + ",")
				fw.write(node.nodeId + ",")
				fw.write(node.splitFeatureIndex + ",")
				fw.write(node.splitFeatureValue + ",")
				fw.write(node.label + ",")

				node.baggingTable.getBaggingTable.foreach {
					case (feature, count) => {
						fw.write(List.fill(count)(feature).mkString(" ") + " ");
					}
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