Random Forest implentation for Stratosphere
==============

Creating the random forest
--------------

*Some comment*

**Data format**

**Usage with LocalExecutor**

	/**
	 * Builds a random forest model based on the training data set. Iteratively executes a new
	 * [[bigdataproject.scala.eu.stratosphere.ml.randomforest.RandomForestBuilder]] for every level of
	 * the forest, in case there are nodes to split on that level.
	 * 
	 * @param outputPath Folder that contains the output model.
	 * 
	 * @param dataFilePath Test data set. Format:
	 * [zero based line index] [label] [feature 1 value] [feature 2 value] [feature N value]
	 * 
	 * @param numTrees Number of trees in the forest
	 */
	new RandomForestBuilder().build([outputPath], [dataFilePath], [numTrees])

**Usage on a cluster**

	java -cp ... ... 


Using the random forest to classify data
--------------

**Data format**
Test data should be in the same format as for creating the random forest.

**Usage with LocalExecutor**

	new RandomForestBuilder().eval()

**Usage on a cluster**

	stratosphere run ... ...