import com.twitter.cassovary.util.FileUtils
import java.io.File
import util.Random

object RandomNumbers {

  def main(args: Array[String]) {

    val maxNodeId = args(1).toInt
    val numNodes = args(2).toInt

    FileUtils.printToFile(new File(args(0))) { p =>
      var i = 0
      while (i < numNodes) {
        p.println(Random.nextInt(maxNodeId) + 1)
        i += 1
      }
    }

  }

}