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
package com.twitter.cassovary.server.experiments

import com.twitter.cassovary.server.{CachedDirectedGraphServerConfig, CachedDirectedGraphServerExperiment}
import com.twitter.cassovary.graph._
import com.twitter.cassovary.graph.GraphUtils.RandomWalkParams
import java.io.File
import com.twitter.cassovary.util.{FileUtils, ExecutorUtils}
import java.util.concurrent.{Future, Executors}
import net.lag.logging.Logger
import scala.collection.JavaConversions._
import java.util.HashMap
import node.{UniDirectionalNode, ArrayBasedDirectedNode}
import com.twitter.cassovary.graph.StoredGraphDir._
import scala.Some
import com.twitter.cassovary.graph.GraphUtils.RandomWalkParams
import com.twitter.cassovary.graph.StoredGraphDir

/**
 * Same as PtcExperiment, except that since we're only doing a 2 step random walk,
 * we can preload this subgraph around a specific node, and then do the random walk, so that
 * we don't hit the cache as often.
 */
class PrecachedPtcExperiment(config: CachedDirectedGraphServerConfig,
                    graph:CachedDirectedGraph)
  extends CachedDirectedGraphServerExperiment(config, graph) {

  class TinyGraph (nodes: HashMap[Int, Node], maxId: Int,
      val nodeWithOutEdgesMaxId: Int,
      val nodeWithOutEdgesCount: Int, val nodeCount: Int, val edgeCount: Long,
      val storedGraphDir: StoredGraphDir) extends DirectedGraph {

    override lazy val maxNodeId = maxId

    def iterator = nodes.values.iterator.filter { _ != null }

    def getNodeById(id: Int) = {
      if (id >= nodes.size) {
        None
      } else {
        val node = nodes(id)
        if (node == null) {
          None
        } else {
          Some(node)
        }
      }
    }
  }


  private val log = Logger.get(getClass.getName)

  val nodeList = config.nodeList
  val verbose = config.verbose

  def ptc(nodeList: String, graph: CachedDirectedGraph, walkParams: RandomWalkParams) {
    var j = 0
    FileUtils.linesFromFile(nodeList) { line =>
      val i = line.toInt
      if (graph.existsNodeId(i)) {
        j += 1
        log.info("PR for %d (id %d)".format(j, i))

        // Get all nodes up to 2 steps out
        val emptyArray = Array.empty[Int]
        val ns = graph.getNodeById(i).get.outboundNodes()
        val nodeMap = new HashMap[Int, Node]()
        ns.foreach { n =>
          val outboundNodes = graph.getNodeById(n).get.outboundNodes().toArray[Int]
          outboundNodes.foreach { k =>
            if (!nodeMap.contains(k)) nodeMap.put(k, UniDirectionalNode(k, emptyArray, StoredGraphDir.OnlyOut))
          }
          nodeMap.put(n, UniDirectionalNode(n, outboundNodes, StoredGraphDir.OnlyOut))
        }

        // Generate a graph
        val genGraph = new TinyGraph(nodeMap, 0, 0, 0, 0, 0, StoredGraphDir.OnlyOut)

        val graphUtils = new GraphUtils(genGraph)
        graphUtils.calculatePersonalizedReputation(i, walkParams)
        if (j % 1000 == 0) {
          log.info("cache_miss_stats: " + graph.statsString)
        }
      }
    }
  }

  def ptcVerbose(nodeList: String, graph: CachedDirectedGraph, walkParams: RandomWalkParams) {
    var j = 0
    FileUtils.linesFromFile(nodeList) { line =>
      val i = line.toInt
      if (graph.existsNodeId(i)) {
        j += 1
        val sb = new StringBuilder
        sb.append("PR for %d (id %d)\n".format(j, i))
        val graphUtils = new GraphUtils(graph)
        val (topNeighbors, paths) = graphUtils.calculatePersonalizedReputation(i, walkParams)
        topNeighbors.toList.sort((x1, x2) => x2._2.intValue < x1._2.intValue).take(10).foreach { case (id, numVisits) =>
          if (paths.isDefined) {
            val topPaths = paths.get.get(id).map { case (DirectedPath(nodes), count) =>
              nodes.mkString("->") + " (%s)".format(count)
            }
            sb.append("%8s%10s\t%s\n".format(id, numVisits, topPaths.mkString(" | ")))
          }
        }
        log.info(sb.toString)
        if (j % 1000 == 0) {
          log.info("cache_miss_stats: " + graph.statsString)
        }
      }
    }
  }


  def run {
    // Do a random walk
    val walkParams = if (verbose)
      RandomWalkParams(10000, 0.0, Some(2000), Some(1), Some(3), false, GraphDir.OutDir, false, true)
    else
      RandomWalkParams(10000, 0.0, Some(2000), None, Some(3), false, GraphDir.OutDir, false, true)

    val nodeFile = new File(nodeList)
    if (nodeFile.isDirectory) {
      val filelist = nodeFile.list
      log.info("Concurrent Start with %s threads!".format(filelist.size))
      val futures = ExecutorUtils.parallelWork[String, Unit](Executors.newFixedThreadPool(filelist.size), filelist,
      { file =>
        val cGraph = graph.getThreadSafeChild


        if (verbose)
          ptcVerbose(nodeList+"/"+file, cGraph, walkParams)
        else
          ptc(nodeList+"/"+file, cGraph, walkParams)
      })
      futures.toArray.map { f => f.asInstanceOf[Future[Unit]].get }
    }
    else {
      log.info("Single Threaded Start!")
      if (verbose)
        ptcVerbose(nodeList, graph, walkParams)
      else
        ptc(nodeList, graph, walkParams)
    }

    log.info("Finished starting up!")
  }

}
