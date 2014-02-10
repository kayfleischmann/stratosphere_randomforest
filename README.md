Random Forest implentation for Stratosphere
==============

Creating the random forest
--------------

*Some comment*

**Data format**

**Usage with LocalExecutor**

	new RandomForestBuilder().build()

**Usage on a cluster**

	new RandomForestBuilder().build()


Using the random forest to classify data
--------------

**Data format**
Test data should be in the same format as for creating the random forest.

**Usage with LocalExecutor**

	new RandomForestBuilder().eval()

**Usage on a cluster**

	new RandomForestBuilder().eval()