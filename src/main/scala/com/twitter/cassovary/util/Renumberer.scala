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
package com.twitter.cassovary.util

import io.AdjacencyListGraphReader
import net.lag.logging.Logger
import java.util.concurrent.{ExecutorService, Future, Executors}
import com.twitter.ostrich.stats.Stats
import com.twitter.cassovary.graph.NodeIdEdgesMaxId
import java.io.File
import java.util.concurrent.atomic.AtomicInteger

object Renumberer {

  def apply(maxId: Int) = {
    new Renumberer(maxId)
  }

  def fromFile(filename: String) = {
    val r = new Renumberer(0)
    val rd = new LoaderSerializerReader(filename)
    r.fromReader(rd)
    rd.close
    r
  }
}

/**
 * Renumber integer ids to integers in increasing order (hereby referred to as indices)
 * Useful when needing to "compact" a list of non-sequential integers
 * Any translations must be on ids >= 1, or the reverse mapping is undefined.
 * @param maxId
 */
class Renumberer(var maxId: Int) {
  private var idToIndex = new Array[Int](maxId+1)
  private var indexToId: Array[Int] = null
  private var index = 0
  private val log = Logger.get("Renumberer")

  /**
   * Translates a given id to a unique identifying index
   * For any Renumberer object, the same id always returns the same index
   * This isn't the case for different Renumberer objects
   * The renumbered indices start from 1
   * @param id id to map/translate
   * @return index corresponding to id
   */
  def translate(id: Int): Int = {
    if (idToIndex(id) == 0) {
      synchronized {
        index += 1
        idToIndex(id) = index
        index
      }
    }
    else {
      idToIndex(id)
    }
  }

  /**
   * Number of unique translations done
   */
  def count = index

  /**
   * Given an array of ids, return the corresponding array of indices
   * @param ary input array of ids
   * @return array of corresponding indices
   */
  def translateArray(ary: Array[Int]): Array[Int] = {
    ary.map({ elt => translate(elt) })
  }

  /**
   * Given an index, return the id
   * The initial query may take some time as the reverse index is lazily built
   * Will return 0 if a given id doesn't exist in the index
   * @param index
   */
  def reverseTranslate(index: Int): Int = {
    prepareReverse
    try {
      indexToId(index)
    }
    catch {
      case e: ArrayIndexOutOfBoundsException => 0
    }
  }

  /**
   * (Re)build the reverse index whenever the forward index is updated
   */
  def prepareReverse {
    if (indexToId == null || indexToId.size != index + 1) {
      log.info("Rebuilding reverse index...")
      indexToId = new Array[Int](index + 1)
      var j = 0
      idToIndex.foreach { i =>
        indexToId(i) = j
        j += 1
      }
      log.info("Finished rebuilding")
    }
  }

  /**
   * Resize this renumberer
   * @param newMaxId New MaxId to resize to
   */
  def resize(newMaxId: Int) {
    if (newMaxId <= idToIndex.size - 1) {
      throw new IllegalArgumentException("NewMaxId %s <= Old MaxId %s".format(newMaxId, idToIndex.size - 1))
    }
    val newIdToIndex = new Array[Int](newMaxId+1)
    Array.copy(idToIndex, 0, newIdToIndex, 0, idToIndex.size)
    idToIndex = newIdToIndex
  }

  /**
   * Save this renumberer to a LoaderSerializerWriter
   * @param writer
   */
  def toWriter(writer:LoaderSerializerWriter) {
    writer.arrayOfInt(idToIndex)
    writer.int(index)
  }

  /**
   * Load a renumberer from a LoaderSerializerReader
   * Note that this doesn't allow you to recover the reverse mapping
   * @param reader
   */
  def fromReader(reader:LoaderSerializerReader) {
    idToIndex = reader.arrayOfInt()
    maxId = idToIndex.size - 1
    index = reader.int
  }
}

object GraphRenumberer {
  case class MaxIdsEdges(localMaxId:Int, localNodeWithoutOutEdgesMaxId:Int, numEdges:Int, nodeCount:Int)
  lazy val log = Logger.get("GraphRenumberer")

  /**
   * Given a directory containing a graph in the AdjacencyList format, renumber the nodes,
   * thus compacting the id space
   * @param sourceDirectory Original graph directory
   * @param prefixFilenames Filename prefix in the original graph directory
   * @param destinationDirectory Directory the renumbered graph should be written to
   * @param renumberer Provide a Renumberer if you want to use a particular mapping
   * @param renumbererFile Where the Renumberer will be written to (since it may get resized or new entries added)
   * @param executorService Executor service to use (defaults to using 10 threads)
   */
  def renumberAdjacencyListGraph(sourceDirectory: String, prefixFilenames: String, destinationDirectory: String, renumberer: Option[Renumberer],
                        renumbererFile: String, executorService: ExecutorService = Executors.newFixedThreadPool(10)) = {

    if (new File(destinationDirectory+"/done_marker.summ").exists()) {
      log.info("Some graph already exists at %s!".format(destinationDirectory))
    }
    else {
      val iteratorSeq = new AdjacencyListGraphReader(sourceDirectory, prefixFilenames).iteratorSeq

      // Load graph once to get maxId
      log.info("Loading graph to determine maxId...")
      var maxId = 0
      var nodeWithOutEdgesMaxId = 0
      var numEdges = 0
      var nodeWithOutEdgesCount = 0
      val futures1 = Stats.time("graph_load_reading_maxid_and_calculating_numedges") {
        def readOutEdges(iteratorFunc: () => Iterator[NodeIdEdgesMaxId]) = {
          var localMaxId, localNodeWithOutEdgesMaxId, numEdges, nodeCount = 0
          iteratorFunc().foreach { item =>
          // Keep track of Max IDs
            localMaxId = localMaxId max item.maxId
            localNodeWithOutEdgesMaxId = localNodeWithOutEdgesMaxId max item.id
            // Update nodeCount and total edges
            numEdges += item.edges.length
            nodeCount += 1
          }
          MaxIdsEdges(localMaxId, localNodeWithOutEdgesMaxId, numEdges, nodeCount)
        }
        ExecutorUtils.parallelWork[() => Iterator[NodeIdEdgesMaxId], MaxIdsEdges](executorService,
          iteratorSeq, readOutEdges)
      }
      futures1.toArray map { future =>
        val f = future.asInstanceOf[Future[MaxIdsEdges]]
        val MaxIdsEdges(localMaxId, localNWOEMaxId, localNumEdges, localNodeCount) = f.get
        maxId = maxId max localMaxId
        nodeWithOutEdgesMaxId = nodeWithOutEdgesMaxId max localNWOEMaxId
        numEdges += localNumEdges
        nodeWithOutEdgesCount += localNodeCount
      }

      val ren = renumberer match {
        case None => new Renumberer(maxId)
        case Some(r) => {
          if (r.maxId < maxId) {
            log.info("Resizing from %s to %s".format(r.maxId, maxId))
            r.resize(maxId)
          }
          r
        }
      }

      // Load graph a second time to map only the out-nodes
      log.info("Loading graph again to map only out-nodes...")
      val futures2 = Stats.time("graph_load_renumbering_nodes_with_outedges") {
        def readOutEdges(iteratorFunc: () => Iterator[NodeIdEdgesMaxId]) = {
          iteratorFunc().foreach { item =>
            ren.translate(item.id)
          }
        }
        ExecutorUtils.parallelWork[() => Iterator[NodeIdEdgesMaxId], Unit](executorService,
          iteratorSeq, readOutEdges)
      }
      futures2.toArray map { future => future.asInstanceOf[Future[Unit]].get }
      assert(ren.count == nodeWithOutEdgesCount) // Sanity check

      // Load graph a final time to map the edges and write them out
      log.info("Loading graph to write out the renumbered version...")
      FileUtils.makeDirs(destinationDirectory)
      val shardCounter = new AtomicInteger()
      val futures3 = Stats.time("graph_load_write_nodes_with_outedges") {
        def readOutEdges(iteratorFunc: () => Iterator[NodeIdEdgesMaxId]) = {
          val f = FileUtils.printWriter("%s/part-r-%05d".format(destinationDirectory, shardCounter.getAndIncrement))
          iteratorFunc().foreach { item =>
            val id = ren.translate(item.id)
            val edgeIds = ren.translateArray(item.edges)
            f.println("%s\t%s".format(id, item.edges.length))
            edgeIds.foreach { eid => f.println(eid) }
          }
          f.close()
        }
        ExecutorUtils.parallelWork[() => Iterator[NodeIdEdgesMaxId], Unit](executorService,
          iteratorSeq, readOutEdges)
      }
      futures3.toArray map { future => future.asInstanceOf[Future[Unit]].get }

      FileUtils.printToFile(new File(destinationDirectory+"/done_marker.summ")) { p =>
        p.println(ren.count + "\t" + ren.count + "\t" + numEdges)
      }

      // Write mapping out
      log.info("Writing mapping out...")
      val serializer = new LoaderSerializerWriter(renumbererFile)
      ren.toWriter(serializer)
      serializer.close

      log.info("Done!")
    }
  }
}