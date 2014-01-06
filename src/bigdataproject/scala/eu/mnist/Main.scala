package eu.stratosphere.randomforest;


import mnist.tools.MnistManager;
import java.io.PrintWriter

object MainClass {
  def main(args: Array[String]) {
	  	saveData(new MnistManager( "train-images.idx3-ubyte",
			  					"train-labels.idx1-ubyte"),
			  					"C:\\Projects\\StratosphereRandomForest\\Prototype1\\normalized.txt", 5000)
	  	saveData(new MnistManager( "t10k-images.idx3-ubyte",
			  					"t10k-labels.idx1-ubyte"),
			  					"C:\\Projects\\StratosphereRandomForest\\Prototype1\\normalized_test.txt", 100)
  }
  
  private def saveData(m : MnistManager, outPath : String, count : Int): Unit = {
    
    var labels = m.getLabels()
    var num_samples = labels.getCount() 
    System.out.println( "samples found " + labels.getCount() )
    System.out.println( "image-format " + m.getImages().getCols() + " cols, " + m.getImages().getRows() + " rows" )
    System.out.println();
    var out = new PrintWriter(outPath);
    var index = 0;
    for (i <- 0 until num_samples) {
    	//System.out.println("read sample "+i)
    	val image = m.readImage()
    	val label = m.readLabel()

    	if (true)
    	{
    		//System.out.println("Label:" + m.readLabel());
    		
    		//System.out.println(i + ":" + m.readLabel() + ";" + image.map(line => line.mkString(",")).mkString(","))
    		
    		var text = ""
			 if (index > 0)
			   text += "\r\n"
    		text += index + " " + label + " " + image.map(line => line.map(arv => arv.toDouble / 256.toDouble).mkString(" ")).mkString(" ")
    		out.print(text);
    		//System.out.println("Image length: " + m.getImages().getEntryLength());
    		//val images = m.getImages()
    		
    		//System.out.println("Current Index: " + m.getImages().getCurrentIndex());
    		
    		//System.out.println("Label length: " + m.getLabels().getEntryLength());
    		//System.out.println("Label Index: " + m.getLabels().getCurrentIndex());
    		index = index + 1;
    	}
    }
    out.close()
  }
}