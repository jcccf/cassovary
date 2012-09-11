/*
* Copyright 2012 Twitter, Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
* file except in compliance with the License. You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software distributed
* under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
* CONDITIONS OF ANY KIND, either express or implied. See the License for the
* specific language governing permissions and limitations under the License.
*/

import com.twitter.cassovary.graph.NodeIdEdgesMaxId
import com.twitter.cassovary.util._
import com.twitter.ostrich.stats.Stats
import io.AdjacencyListGraphReader
import java.util.concurrent.{Future, Executors}
import net.lag.logging.Logger
import util.control.Exception
import java.io.File

object RenumberGraphDirectory {

  private case class MaxIdsEdges(localMaxId:Int, localNodeWithoutOutEdgesMaxId:Int, numEdges:Int, nodeCount:Int)

  private lazy val log = Logger.get("RenumberGraphDirectory")

  def main(args: Array[String]) {

    if (args.length < 4) {
      throw new Exception ("Provide source graphdir, prefix, destination graphdir, outMapping and inMapping!")
    }

    val ren = if (args.length > 4) {
      Some(Renumberer.fromFile(args(4)))
    } else {
      None
    }

    GraphRenumberer.renumberAdjacencyListGraph(args(0), args(1), args(2), None, args(3))
  }

}