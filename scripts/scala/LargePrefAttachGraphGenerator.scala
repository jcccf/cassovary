import com.twitter.cassovary.util.FileUtils
import java.io.{PrintWriter, File}
import util.Random
import collection.mutable

object LargePrefAttachGraphGenerator {
  def main(args: Array[String]) {
    val directory = args(0)
    val numParts = args(1).toInt
    val numNodes = args(2).toInt
    val nodesPerPart = (numNodes.toDouble / numParts).ceil.toInt
    val approxNumEdges = args(3).toLong
    val probRandom = 2.0/7

    // So that on average numEdges/numNodes edges are created for each node
    val randUpperLimit = 2 * (approxNumEdges.toDouble / numNodes).toInt
    val edgesPerNode = (approxNumEdges.toDouble / numNodes).toInt

    FileUtils.makeDirs(directory)

    val nodeHash = mutable.HashMap[Int, Array[Int]]()

    // Complete graph for the first set of edges
    for (i <- 1 to edgesPerNode) {
      nodeHash(i) = Random.shuffle((1 to edgesPerNode).filter(k => k != i).toSeq).take(43).toArray
    }

    println(edgesPerNode)
    println(numNodes)
    for (i <- (edgesPerNode+1 to numNodes)) {
      // With probability p, pick uniformly from previous nodes, else pick an out-edge of a random node
      val a: Array[Int] = (1 to (Random.nextInt(randUpperLimit)+1)).map(_ =>
        if (Random.nextDouble() <= probRandom) {
          Random.nextInt(i-1) + 1
        }
        else {
          val rNode = Random.nextInt(i-1) + 1
          nodeHash(rNode)(Random.nextInt(nodeHash(rNode).length))
        }
      ).toSet.toArray
//      println("Array %d is %d".format(i, a.length))
      nodeHash(i) = a
    }

    for (i <- 1 to edgesPerNode) {
      nodeHash(i) = ((1 to 17).map(_ => Random.nextInt(numNodes-1) + 1).toSet.toArray) ++ nodeHash(i)
    }

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
      val edges = nodeHash(i)
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