import com.twitter.cassovary.util.FileUtils
import java.io.{PrintWriter, File}
import util.Random

object LargeRandomGraphGenerator {
  def main(args: Array[String]) {
    val directory = args(0)
    val numParts = args(1).toInt
    val numNodes = args(2).toInt
    val nodesPerPart = (numNodes.toDouble / numParts).ceil.toInt
    val approxNumEdges = args(3).toLong

    // So that on average numEdges/numNodes edges are created for each node
    val randUpperLimit = 2 * (approxNumEdges.toDouble / numNodes).toInt + 1

    FileUtils.makeDirs(directory)

    // Write edges to files
    var j = 0
    var nodeCount = 0
    var edgeCount = 0
    var p: PrintWriter = null
    for (i <- 1 to numNodes) {
      // Make a new PrintWriter if we need to make a new part file
      if (nodeCount % nodesPerPart == 0) {
        if (p != null) p.close()
        p = FileUtils.printWriter(directory+"/part-r-%05d".format(j))
        j += 1
      }
      val edges = Seq.fill(Random.nextInt(randUpperLimit))(Random.nextInt(numNodes)).toSet.filter(k => k != i)
      val numEdges = edges.size
      p.println(i + "\t" + numEdges)
      edges.foreach { k => p.println(k) }
      edgeCount += numEdges
      nodeCount += 1
    }

    // Close last PrintWriter
    if (p != null) p.close()

    // Write done marker
    val maxId = numNodes
    FileUtils.printToFile(new File(directory+"/done_marker.summ")) { p =>
      p.println(maxId + "\t" + numNodes + "\t" + edgeCount)
    }


  }
}