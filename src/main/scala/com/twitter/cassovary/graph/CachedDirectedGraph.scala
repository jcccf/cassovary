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
package com.twitter.cassovary.graph

import com.twitter.cassovary.graph.StoredGraphDir._
import java.util.concurrent.{Future, ExecutorService}
import node._
import com.google.common.annotations.VisibleForTesting
import com.twitter.ostrich.stats.Stats
import java.util.concurrent.atomic.AtomicLong
import com.google.common.util.concurrent.MoreExecutors
import java.io._
import collection.mutable
import net.lag.logging.Logger
import com.google.common.cache.{LoadingCache, CacheLoader, Weigher, CacheBuilder}
import com.twitter.cassovary.util._
import cache._
import scala.Some

private case class MaxIdsEdges(localMaxId:Int, localNodeWithoutOutEdgesMaxId:Int, numEdges:Int, nodeCount:Int)

/**
 * Methods for constructing a disk-cached directed graph
 */
object CachedDirectedGraph {

  private lazy val log = Logger.get("CachedDirectedGraph")

  /**
   * Iterate through the graph once to find the global maximum node id,
   * the maximum node id among nodes that have edges (nodeWithXEdgesMaxId)
   * and the number of nodes that have edges (nodeWithXEdgesCount), and
   * the total number of edges
   */
  def getMaxCounts(iteratorSeq: Seq[ () => Iterator[NodeIdEdgesMaxId] ],
                   executorService: ExecutorService,
                   serializer: LoaderSerializer, name: String) = {
    var maxId, nodeWithXEdgesMaxId, nodeWithXEdgesCount = 0
    var numEdges = 0L
    serializer.writeOrRead(name, { writer =>
      val futures = Stats.time("graph_load_reading_maxid_and_calculating_numedges") {
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
      futures.toArray map { future =>
        val f = future.asInstanceOf[Future[MaxIdsEdges]]
        val MaxIdsEdges(localMaxId, localNWOEMaxId, localNumEdges, localNodeCount) = f.get
        maxId = maxId max localMaxId
        nodeWithXEdgesMaxId = nodeWithXEdgesMaxId max localNWOEMaxId
        numEdges += localNumEdges
        nodeWithXEdgesCount += localNodeCount
      }
      writer.integers(Seq(maxId, nodeWithXEdgesMaxId, nodeWithXEdgesCount)).longs(Seq(numEdges))
    }, { reader =>
      maxId = reader.int
      nodeWithXEdgesMaxId = reader.int
      nodeWithXEdgesCount = reader.int
      numEdges = reader.long
    })
    (maxId, nodeWithXEdgesMaxId, nodeWithXEdgesCount, numEdges)
  }

  /**
   * Generate offset and numEdge tables for out-edges, not renumbered
   */
  def generateOffsetTables(iteratorSeq: Seq[ () => Iterator[NodeIdEdgesMaxId] ],
                           executorService: ExecutorService,
                           serializer: LoaderSerializer, name: String, numShards: Int, maxId: Int) = {

    var nodeIdSet: mutable.BitSet = null
    var idToIntOffset: Array[Long] = null
    var idToNumEdges: Array[Int] = null
    var edgeOffsets: Array[AtomicLong] = null

    serializer.writeOrRead(name, { writer =>
      nodeIdSet = new mutable.BitSet(maxId+1)
      idToIntOffset = new Array[Long](maxId+1)
      idToNumEdges = new Array[Int](maxId+1)
      edgeOffsets = new Array[AtomicLong](numShards)
      (0 until numShards).foreach { i => edgeOffsets(i) = new AtomicLong() }
      Stats.time("graph_load_generating_offset_tables") {
        def readOutEdges(iteratorFunc: () => Iterator[NodeIdEdgesMaxId]) {
          iteratorFunc() foreach { item =>
            val id = item.id
            val itemEdges = item.edges.length
            // Update nodeId set
            nodeIdSet(id) = true
            item.edges foreach { edge => nodeIdSet(edge) = true }
            // Store offsets
            val edgeOffset = edgeOffsets(id % numShards).getAndAdd(itemEdges)
            idToNumEdges(id) = itemEdges
            idToIntOffset(id) = edgeOffset
          }
        }
        ExecutorUtils.parallelWork[() => Iterator[NodeIdEdgesMaxId], Unit](executorService,
          iteratorSeq, readOutEdges).toArray.map { future => future.asInstanceOf[Future[Unit]].get }
      }
      log.info("Writing offset tables to file...")
      writer.bitSet(nodeIdSet)
        .arrayOfLong(idToIntOffset)
        .arrayOfInt(idToNumEdges)
        .atomicLongArray(edgeOffsets)
    }, { reader =>
      nodeIdSet = reader.bitSet()
      idToIntOffset = reader.arrayOfLong()
      idToNumEdges = reader.arrayOfInt()
      edgeOffsets = reader.atomicLongArray()
    })
    (nodeIdSet, idToIntOffset, idToNumEdges, edgeOffsets)
  }

  /**
   * Generate offset tables for in-edges, both renumbered and not renumbered
   */
  def generateOffsetTablesForIn(iteratorSeq: Seq[ () => Iterator[NodeIdEdgesMaxId] ],
                                executorService: ExecutorService,
                                serializer: LoaderSerializer, name: String, numShards: Int,
                                inArraySize: Int, ren: Renumberer = null) = {
    var idToIntOffset: Array[Long] = null
    var idToNumEdges: Array[Int] = null
    var edgeOffsets: Array[AtomicLong] = null

    serializer.writeOrRead(name, { writer =>
      idToIntOffset = new Array[Long](inArraySize+1)
      idToNumEdges = new Array[Int](inArraySize+1)
      edgeOffsets = new Array[AtomicLong](numShards)
      (0 until numShards).foreach { i => edgeOffsets(i) = new AtomicLong() }
      Stats.time("graph_load_generating_offset_tables") {
        if (ren == null) {
          def readInEdges(iteratorFunc: () => Iterator[NodeIdEdgesMaxId]) {
            iteratorFunc() foreach { item =>
              val id = item.id
              val itemEdges = item.edges.length
              // Store offsets
              val edgeOffset = edgeOffsets(id % numShards).getAndAdd(itemEdges)
              idToNumEdges(id) = itemEdges
              idToIntOffset(id) = edgeOffset
            }
          }
          ExecutorUtils.parallelWork[() => Iterator[NodeIdEdgesMaxId], Unit](executorService,
            iteratorSeq, readInEdges).toArray.map { future => future.asInstanceOf[Future[Unit]].get }
        }
        else {
          def readInEdges(iteratorFunc: () => Iterator[NodeIdEdgesMaxId]) {
            iteratorFunc() foreach { item =>
              val id = ren.translate(item.id)
              val itemEdges = item.edges.length
              item.edges foreach { edge => ren.translate(edge) }
              // Store offsets
              val edgeOffset = edgeOffsets(id % numShards).getAndAdd(itemEdges)
              idToNumEdges(id) = itemEdges
              idToIntOffset(id) = edgeOffset
            }
          }
          ExecutorUtils.parallelWork[() => Iterator[NodeIdEdgesMaxId], Unit](executorService,
            iteratorSeq, readInEdges).toArray.map { future => future.asInstanceOf[Future[Unit]].get }
        }
      }

      val w = new LongIntWriter(serializer.directory + "/lin_" + name)
      w.writeLongArrayAndIntArray(idToIntOffset, idToNumEdges)
      w.close()

      writer.atomicLongArray(edgeOffsets)
    }, { reader =>
      val r = new LongIntReader(serializer.directory + "/lin_" + name)
      val in = r.readLongAndIntArray
      r.close()
      idToIntOffset = in._1; idToNumEdges = in._2
      edgeOffsets = reader.atomicLongArray()
    })
    (idToIntOffset, idToNumEdges, edgeOffsets)
  }

  /**
   * Allocate disk space for shards
   */
  def allocateShards(serializer: LoaderSerializer, name: String, shardDirectories: Array[String],
                     numShards: Int, edgeOffsets: Array[AtomicLong]) = {
    serializer.writeOrRead(name, { writer =>
      Stats.time("graph_load_allocating_shards") {
        val esw = new MultiDirIntShardsWriter(shardDirectories, numShards)
        (0 until numShards).foreach { i =>
          esw.shardWriters(i).allocate(edgeOffsets(i).get)
        }
        esw.close
      }
      writer.integers(Seq(1))
    }, { r => })
  }

  /**
   * Write shards to disk in rounds.
   * In each round, initialize some shards in memory, write to those shards only, then
   * write those shards to disk.
   * Minimizes the amount of memory needed to rewrite shards, at the cost of this generation
   * step taking longer.
   */
  def writeShardsInRounds(iteratorSeq: Seq[ () => Iterator[NodeIdEdgesMaxId] ],
                          executorService: ExecutorService,
                          serializer: LoaderSerializer, name: String, shardDirectories: Array[String],
                          numShards: Int, numRounds: Int, edgeOffsets: Array[AtomicLong],
                          idToIntOffset: Array[Long], idToNumEdges: Array[Int]) = {
    serializer.writeOrRead(name, { writer =>
      val shardSizes = edgeOffsets.map { i => i.get().toInt }
      val msw = new MultiDirMemIntShardsWriter(shardDirectories, numShards, shardSizes, numRounds)
      (0 until numRounds).foreach { roundNo =>
        log.info("Beginning round %s...".format(roundNo))
        msw.startRound(roundNo)
        val (modStart, modEnd) = msw.roundRange
        Stats.time("graph_load_writing_round_to_shards") {
          def readInEdges(iteratorFunc: () => Iterator[NodeIdEdgesMaxId]) {
            iteratorFunc() foreach { item =>
              val id = item.id
              val shardId = id % numShards
              if (modStart <= shardId && shardId < modEnd) {
                val edgeOffset = idToIntOffset(id)
                val numEdges = idToNumEdges(id)
                msw.writeIntegersAtOffsetFromOffset(id, edgeOffset.toInt, item.edges, 0, numEdges)
              }
            }
          }
          ExecutorUtils.parallelWork[() => Iterator[NodeIdEdgesMaxId], Unit](executorService,
            iteratorSeq, readInEdges).toArray.map { future => future.asInstanceOf[Future[Unit]].get }
        }
        log.info("Ending round %s...".format(roundNo))
        msw.endRound
      }
      writer.integers(Seq(1))
    }, { r => })
  }

  /**
   * Count the total number of unique nodes (even those with no edges) in the graph.
   */
  def countTotalNodes(serializer: LoaderSerializer, name: String, maxId: Int, nodeIdSet: mutable.BitSet) = {
    var numNodes = 0
    serializer.writeOrRead(name, { writer =>
      Stats.time("graph_load_count_total_num_of_nodes") {
        for ( id <- 0 to maxId )
          if (nodeIdSet(id))
            numNodes += 1
        writer.integers(Seq(numNodes))
      }
    }, { reader => numNodes = reader.int })
    numNodes
  }

  /**
   * Make a new CachedDirectedGraph
   * TODO this is confusing now, because outIteratorSeq can be the in-edge sequence if inIteratorSeq is not provided.
   * @param outIteratorSeq
   * @param inIteratorSeq
   * @param executorService
   * @param storedGraphDir
   * @param cacheType What kind of cache do you want to use? (lru, lru_na, clock, clock_na, guava)
   * @param cacheMaxNodes How many nodes should the cache store (at most)?
   * @param cacheMaxEdges How many edges should the cache store (at most)?
   * @param shardDirectories Where do you want generated shards to live?
   * @param numShards How many shards to generate?
   * @param numRounds How many rounds to generate the shards in? More rounds => less memory but takes longer
   * @param useCachedValues Reload everything? (i.e. ignore any cached objects).
   * @param cacheDirectory Where should the cached objects live? This directory must be different for different graphs
   * @param renumbered Has this graph been renumbered? (or was this graph parsed using GraphRenumberer?)
   * @return an appropriate subclass of CachedDirectedGraph
   */
  def apply(outIteratorSeq: Seq[ () => Iterator[NodeIdEdgesMaxId] ],
            inIteratorSeq: Seq[ () => Iterator[NodeIdEdgesMaxId] ],
            executorService: ExecutorService,
            storedGraphDir: StoredGraphDir, cacheType: String, cacheMaxNodes:Int, cacheMaxEdges:Long,
            shardDirectories: Array[String], inShardDirectories: Array[String], numShards: Int, numRounds: Int,
            useCachedValues: Boolean, cacheDirectory: String, renumbered: Boolean, inDisk: Boolean):CachedDirectedGraph = {

    val shardDirectoriesIn = if (inShardDirectories != null) {
      inShardDirectories
    } else {
      shardDirectories.map { shardDirectory => shardDirectory + "_in" }
    }

    log.info("---Beginning Load---")
    log.info("Cache Info: cacheMaxNodes is %s, cacheMaxEdges is %s, cacheType is %s".format(cacheMaxNodes, cacheMaxEdges, cacheType))
    log.info("Disk Shard Info: directory is %s, numShards is %s, numRounds is %s".format(shardDirectories.deep.mkString(", "), numShards, numRounds))
    log.info("useCachedValues is %s, cacheDirectory is %s".format(useCachedValues, cacheDirectory))

    // Step 0 - Initialize serializer
    val serializer = new LoaderSerializer(cacheDirectory, useCachedValues)

    // Step 1
    // Find maxId, nodeWithOutEdgesMaxId, numInts
    // Needed so that we can initialize arrays with the appropriate sizes
    var maxId, nodeWithOutEdgesMaxId, nodeWithOutEdgesCount = 0
    var inMaxId, nodeWithInEdgesMaxId, nodeWithInEdgesCount = 0
    var numEdges, inNumEdges = 0L
    log.info("Reading maxId and Calculating numInts...")
    val oc = getMaxCounts(outIteratorSeq, executorService, serializer, "maxcounts.bin")
    maxId = oc._1; nodeWithOutEdgesMaxId = oc._2; nodeWithOutEdgesCount = oc._3; numEdges = oc._4
    // Also do this for inIteratorSeq if it exists
    if (inIteratorSeq != null) {
      log.info("Reading maxId and Calculating numInts for in-edges...")
      val ic = getMaxCounts(inIteratorSeq, executorService, serializer, "maxcounts_in.bin")
      inMaxId = ic._1; nodeWithInEdgesMaxId = ic._2; nodeWithInEdgesCount = ic._3; inNumEdges = ic._4
    }

    var nodeIdSet: mutable.BitSet = null
    var idToIntOffset: Array[Long] = null
    var idToNumEdges: Array[Int] = null
    var edgeOffsets: Array[AtomicLong] = null
    var numNodes = 0

    var idToIntOffsetIn: Array[Long] = null
    var idToNumEdgesIn: Array[Int] = null
    var edgeOffsetsIn: Array[AtomicLong] = null

    // Step 2
    // Generate the lookup table for nodeIdSet
    // Generate and store shard offsets and # of edges for each node
    log.info("Generating offset tables...")
    val o = generateOffsetTables(outIteratorSeq, executorService, serializer, "offset_tables.bin", numShards, maxId)
    nodeIdSet = o._1; idToIntOffset = o._2; idToNumEdges = o._3; edgeOffsets = o._4

    // Step 3 - Allocating shards
    log.info("Allocating shards...")
    allocateShards(serializer, "shard_allocate.bin", shardDirectories, numShards, edgeOffsets)

    // Step 4x - Generate shards on disk in rounds
    log.info("Writing to shards in rounds...")
    writeShardsInRounds(outIteratorSeq, executorService, serializer, "shard_write.bin", shardDirectories,
      numShards, numRounds, edgeOffsets, idToIntOffset, idToNumEdges)

    // Step 5 - Count number of nodes
    log.info("Counting total number of nodes...")
    numNodes = countTotalNodes(serializer, "total_nodes.bin", maxId, nodeIdSet)

    if (inIteratorSeq != null) {

      var reloadOut = false
      if (inDisk && !serializer.exists("shard_write_in.bin")) { // Null these out only if we've not completed inIterator stuff
        nodeIdSet = null; idToIntOffset = null; idToNumEdges = null; edgeOffsets = null; reloadOut = true
      }

      // Step i1r - Generate Offset Table Counts
      // TODO don't load these in the first place if inDisk is true
      log.info("Generating offset tables for in-shards...")
      val r = generateOffsetTablesForIn(inIteratorSeq, executorService, serializer, "offset_tables_in.bin", numShards, inMaxId)
      idToIntOffsetIn = r._1; idToNumEdgesIn = r._2; edgeOffsetsIn = r._3

      // Step i2 - Allocate Shards
      log.info("Allocating in-shards...")
      allocateShards(serializer, "shard_allocate_in.bin", shardDirectoriesIn, numShards, edgeOffsetsIn)

      // Step i3 - Write shards
      log.info("Writing to in-shards in rounds...")
      writeShardsInRounds(inIteratorSeq, executorService, serializer, "shard_write_in.bin", shardDirectoriesIn,
        numShards, numRounds, edgeOffsetsIn, idToIntOffsetIn, idToNumEdgesIn)

      if (reloadOut) { // Reload only if we've just completed
        idToIntOffsetIn = null; idToNumEdgesIn = null; edgeOffsetsIn = null
        val o = generateOffsetTables(outIteratorSeq, executorService, serializer, "offset_tables.bin", numShards, maxId)
        nodeIdSet = o._1; idToIntOffset = o._2; idToNumEdges = o._3; edgeOffsets = o._4
      } else if (inDisk) { // Null out if inDisk
        idToIntOffsetIn = null; idToNumEdgesIn = null; edgeOffsetsIn = null
      }

    }

    log.info("---Ended Load!---")

    // Sanity checks if graph has been renumbered
    if (renumbered) {
      assert(maxId == numNodes)
      assert(nodeWithOutEdgesCount == nodeWithOutEdgesMaxId) // But not for InEdges because that's not necessarily true
      assert(nodeWithInEdgesMaxId <= (maxId max inMaxId)) // A weaker assumption for inMaxId
    }

    // Define the NodeIdSet function - speed up checking when we renumber
    val nodeIdSetFn = if (renumbered) { (i: Int) => { i > 0 && i <= numNodes } } else { nodeIdSet }

    log.info("numNodes is %s, numEdges is %s".format(numNodes, numEdges))
    log.info("maxId is %s, nodeWithOutEdgesCount is %s, nodeWithOutEdgesMaxId is %s".format(maxId,
      nodeWithOutEdgesCount, nodeWithOutEdgesMaxId))
    log.info("inMaxId is %s, nodeWithInEdgesMaxId is %s, node WithInEdgesCount is %s".format(inMaxId,
      nodeWithInEdgesMaxId, nodeWithInEdgesCount))

    // Parse out cacheType and nodeType for FastCachedDirectedGraph
    val (cType, nType) = if (cacheType.contains("_")) {
      val a = cacheType.split("_")
      (a(0), a(1))
    } else {
      (cacheType, "node")
    }

    // Return our graph!
    if (inIteratorSeq == null) { // Only one type of edge
      cacheType match {
        case "guava" => GuavaCachedDirectedGraph(nodeIdSetFn, cacheMaxNodes+cacheMaxEdges,
          shardDirectories, numShards, idToIntOffset, idToNumEdges,
          maxId, nodeWithOutEdgesMaxId, nodeWithOutEdgesCount,
          inMaxId, nodeWithInEdgesMaxId, nodeWithInEdgesCount,
          numNodes, numEdges, storedGraphDir)
        case _ => FastCachedDirectedGraph(nodeIdSetFn, cacheMaxNodes, cacheMaxEdges,
          shardDirectories, numShards, idToIntOffset, idToNumEdges,
          maxId, nodeWithOutEdgesMaxId, nodeWithOutEdgesCount,
          inMaxId, nodeWithInEdgesMaxId, nodeWithInEdgesCount,
          numNodes, numEdges, storedGraphDir, cType, nType)
      }
    } else { // Both types of edges
      FastDualCachedDirectedGraph(nodeIdSetFn, cacheMaxNodes, cacheMaxEdges,
        shardDirectories, shardDirectoriesIn, numShards, idToIntOffset, idToNumEdges, idToIntOffsetIn, idToNumEdgesIn,
        maxId, nodeWithOutEdgesMaxId, nodeWithOutEdgesCount,
        inMaxId, nodeWithInEdgesMaxId, nodeWithInEdgesCount,
        numNodes, numEdges, cType, nType,
        new InDiskParams(inDisk, cacheDirectory+"/lin_offset_tables_in.bin"))
    }
  }

  @VisibleForTesting
  def apply(iteratorFunc: () => Iterator[NodeIdEdgesMaxId],
      storedGraphDir: StoredGraphDir, shardDirectory:String,
      cacheType:String, cacheDirectory:String = null,
      renumbered: Boolean):CachedDirectedGraph =
    apply(Seq(iteratorFunc), null, MoreExecutors.sameThreadExecutor(),
      storedGraphDir, cacheType, 2, 4, Array(shardDirectory), null, 8, 2,
      useCachedValues = true, cacheDirectory = cacheDirectory, renumbered = renumbered, inDisk = false)
}

abstract class CachedDirectedGraph(maxId: Int,
    val nodeWithOutEdgesMaxId:Int, val nodeWithOutEdgesCount:Int, val inMaxId: Int,
    val nodeWithInEdgesMaxId: Int, val nodeWithInEdgesCount: Int) extends DirectedGraph {

  override lazy val maxNodeId = maxId

  def iterator = (0 to maxId).view.flatMap(getNodeById(_)).iterator

  def getMisses: Long

  def statsString: String

  def writeStats(fileName: String) {
    FileUtils.printToFile(new File(fileName))(p => {
      p.println(statsString)
    })
  }

  def getThreadSafeChild: CachedDirectedGraph
}

object FastCachedDirectedGraph {
  /**
   * An implementation of the directed graph trait backed by a cache of int arrays (and shards on the disk).
   * - If the nodeType is "node",
   *   - Assigns a couple of nodes per thread, and reuses the nodes each time getNodeById is called.
   *   -  Uses much less memory than NodeArray, but users must be careful to ALWAYS call getNodeById
   *      when accessing properties of a node within a single thread.
   *   - getNodeById always returns the same node object (simply mutating it before doing so).
   *   - In other words, do not save references to node objects.
   * - If the nodeType is "array",
   *   - Each thread gets allocated its own array of node objects.
   *   - Requires significantly more memory than using "node",
   *     but users can save references to node objects without fear.
   *
   * @param nodeIdSet nodes with either outgoing or incoming edges
   * @param cacheMaxNodes maximum number of nodes that the cache can store
   * @param cacheMaxEdges maximum number of edges that the cache can store
   * @param shardDirectories where shards live on disk
   * @param numShards number of shards to split into
   * @param idToIntOffset offset into a shard on disk
   * @param idToNumEdges the number of edges
   * @param maxId max node id in the graph
   * @param nodeCount number of nodes in the graph
   * @param edgeCount number of edges in the graph
   * @param storedGraphDir the graph directions stored
   * @param cacheType the type of cache (clock, lru, random, etc.)
   * @param nodeType how nodes are returned (node, array, etc.)
   */
  def apply(nodeIdSet:(Int => Boolean),
    cacheMaxNodes:Int, cacheMaxEdges:Long,
    shardDirectories:Array[String], numShards:Int,
    idToIntOffset:Array[Long], idToNumEdges:Array[Int],
    maxId: Int, nodeWithOutEdgesMaxId: Int, nodeWithOutEdgesCount: Int,
    inMaxId: Int, nodeWithInEdgesMaxId: Int, nodeWithInEdgesCount: Int,
    nodeCount: Int, edgeCount: Long, storedGraphDir: StoredGraphDir,
    cacheType: String = "lru", nodeType: String = "node"): CachedDirectedGraph = {

    val cache = cacheType match {
      case "lru" => FastLRUIntArrayCache(shardDirectories, numShards,
        nodeWithOutEdgesMaxId, cacheMaxNodes, cacheMaxEdges, idToIntOffset, idToNumEdges)
      case "bufflru" => BufferedFastLRUIntArrayCache(shardDirectories, numShards,
        nodeWithOutEdgesMaxId, cacheMaxNodes, cacheMaxEdges, idToIntOffset, idToNumEdges)
      case "lockfreereadlru" => LocklessReadFastLRUIntArrayCache(shardDirectories, numShards,
        nodeWithOutEdgesMaxId, cacheMaxNodes, cacheMaxEdges, idToIntOffset, idToNumEdges)
      case "random" => RandomizedIntArrayCache(shardDirectories, numShards,
        nodeWithOutEdgesMaxId, cacheMaxNodes, cacheMaxEdges, idToIntOffset, idToNumEdges)
      case "locklessrandom" => LocklessRandomizedIntArrayCache(shardDirectories, numShards,
        nodeWithOutEdgesMaxId, cacheMaxNodes, cacheMaxEdges, idToIntOffset, idToNumEdges)
      case "clock" => FastClockIntArrayCache(shardDirectories, numShards,
        nodeWithOutEdgesMaxId, cacheMaxNodes, cacheMaxEdges, idToIntOffset, idToNumEdges)
      case "nocache" => NotAnIntArrayCache(shardDirectories, numShards,
        nodeWithOutEdgesMaxId, cacheMaxNodes, cacheMaxEdges, idToIntOffset, idToNumEdges)
      case _ => throw new IllegalArgumentException("Unknown cacheType %s".format(nodeType))
    }

    nodeType match {
      case "node" => new FastCachedDirectedGraph (nodeIdSet,
        cacheMaxNodes, cacheMaxEdges,
        shardDirectories, numShards,
        idToIntOffset, idToNumEdges,
        maxId, nodeWithOutEdgesMaxId, nodeWithOutEdgesCount,
        inMaxId, nodeWithInEdgesMaxId, nodeWithInEdgesCount,
        nodeCount, edgeCount, storedGraphDir, cache)
      case "array" => new NodeArrayFastCachedDirectedGraph (nodeIdSet,
        cacheMaxNodes, cacheMaxEdges,
        shardDirectories, numShards,
        idToIntOffset, idToNumEdges,
        maxId, nodeWithOutEdgesMaxId, nodeWithOutEdgesCount,
        inMaxId, nodeWithInEdgesMaxId, nodeWithInEdgesCount,
        nodeCount, edgeCount, storedGraphDir, cache)
      case _ => throw new IllegalArgumentException("Unknown nodeType %s".format(nodeType))
    }
  }
}

class FastCachedDirectedGraph (
    val nodeIdSet:(Int => Boolean),
    val cacheMaxNodes:Int, val cacheMaxEdges:Long,
    val shardDirectories:Array[String], val numShards:Int,
    val idToIntOffset:Array[Long], val idToNumEdges:Array[Int],
    maxId: Int, nodeWithOutEdgesMaxId: Int, nodeWithOutEdgesCount: Int,
    inMaxId: Int, nodeWithInEdgesMaxId: Int, nodeWithInEdgesCount: Int,
    val nodeCount: Int, val edgeCount: Long, val storedGraphDir: StoredGraphDir, val cache: IntArrayCache)
    extends CachedDirectedGraph(maxId, nodeWithOutEdgesMaxId, nodeWithOutEdgesCount,
      inMaxId, nodeWithInEdgesMaxId, nodeWithInEdgesCount) {

  def getThreadSafeChild = new FastCachedDirectedGraph(nodeIdSet,
    cacheMaxNodes, cacheMaxEdges,
    shardDirectories, numShards,
    idToIntOffset, idToNumEdges,
    maxId, nodeWithOutEdgesMaxId, nodeWithOutEdgesCount,
    inMaxId, nodeWithInEdgesMaxId, nodeWithInEdgesCount,
    nodeCount, edgeCount, storedGraphDir, cache.getThreadSafeChild)

  def getMisses = cache.getStats._1

  def statsString: String = {
    val (misses, hits, currentSize, currRealCapacity) = cache.getStats
    "%s\t%s\t%s\t%s\t%s".format(misses, hits + misses, misses.toDouble / (hits+misses), currentSize, currRealCapacity)
  }

  val node = CachedDirectedNode(0, 0, storedGraphDir, cache)
  val someNode = Some(node)
  val emptyNode = EmptyDirectedNode(0, storedGraphDir)
  val someEmptyNode = Some(emptyNode)

  def getNodeById(id: Int) = { // Stats.time("cached_get_node_by_id")
    if (id > maxId || !nodeIdSet(id)) { // Invalid id
      None
    }
    else {
      val numEdges = try { idToNumEdges(id) } catch { case _ => 0 }
      if (numEdges == 0) {
        emptyNode.id = id
        someEmptyNode
      }
      else {
        node.id = id
        node.size = numEdges
        someNode
      }
    }
  }
}

class NodeArrayFastCachedDirectedGraph (
    val nodeIdSet:(Int => Boolean),
    val cacheMaxNodes:Int, val cacheMaxEdges:Long,
    val shardDirectories:Array[String], val numShards:Int,
    val idToIntOffset:Array[Long], val idToNumEdges:Array[Int],
    maxId: Int, nodeWithOutEdgesMaxId: Int, nodeWithOutEdgesCount: Int,
    inMaxId: Int, nodeWithInEdgesMaxId: Int, nodeWithInEdgesCount: Int,
    val nodeCount: Int, val edgeCount: Long, val storedGraphDir: StoredGraphDir,
    val cache: IntArrayCache)
  extends CachedDirectedGraph(maxId, nodeWithOutEdgesMaxId, nodeWithOutEdgesCount,
    inMaxId, nodeWithInEdgesMaxId, nodeWithInEdgesCount) {

  def getThreadSafeChild = new NodeArrayFastCachedDirectedGraph(nodeIdSet,
    cacheMaxNodes, cacheMaxEdges,
    shardDirectories, numShards,
    idToIntOffset, idToNumEdges,
    maxId, nodeWithOutEdgesMaxId, nodeWithOutEdgesCount,
    inMaxId, nodeWithInEdgesMaxId, nodeWithInEdgesCount,
    nodeCount, edgeCount, storedGraphDir, cache.getThreadSafeChild)

  def getMisses = cache.getStats._1

  def statsString: String = {
    val (misses, hits, currentSize, currRealCapacity) = cache.getStats
    "%s\t%s\t%s\t%s\t%s".format(misses, hits + misses, misses.toDouble / (hits+misses), currentSize, currRealCapacity)
  }

  val emptyArray = new Array[Int](0)
  val nodeList = new Array[Option[Node]](maxId+1)

  def getNodeById(id: Int) = {
    if (id > maxId || !nodeIdSet(id)) // Invalid id
      None
    else {
      nodeList(id) match {
        case null => {
          val numEdges = try { idToNumEdges(id) } catch { case _ => 0 }
          if (numEdges == 0) {
            val node = Some(ArrayBasedDirectedNode(id, emptyArray, storedGraphDir))
            nodeList(id) = node
            node
          }
          else {
            val node = Some(CachedDirectedNode(id, numEdges, storedGraphDir, cache))
            nodeList(id) = node
            node
          }
        }
        case n => n
      }
    }
  }

}

object GuavaCachedDirectedGraph {
  def apply(nodeIdSet:(Int => Boolean),
    cacheMaxNodesAndEdges: Long,
    shardDirectories:Array[String], numShards:Int,
    idToIntOffset:Array[Long], idToNumEdges:Array[Int],
    maxId: Int, nodeWithOutEdgesMaxId: Int, nodeWithOutEdgesCount: Int,
    inMaxId: Int, nodeWithInEdgesMaxId: Int, nodeWithInEdgesCount: Int,
    nodeCount: Int, edgeCount: Long, storedGraphDir: StoredGraphDir) = {

    val reader = new MultiDirIntShardsReader(shardDirectories, numShards)

    // Guava Cache
    val cacheG:LoadingCache[Int,Array[Int]] = CacheBuilder.newBuilder()
      .maximumWeight(cacheMaxNodesAndEdges)
      .weigher(new Weigher[Int,Array[Int]] {
      def weigh(k:Int, v:Array[Int]):Int = v.length
    })
      .asInstanceOf[CacheBuilder[Int,Array[Int]]]
      .build[Int,Array[Int]](new CacheLoader[Int,Array[Int]] {
      def load(id:Int):Array[Int] = {
        val numEdges = try { idToNumEdges(id) } catch { case _ => 0 }
        if (numEdges == 0) {
          throw new NullPointerException("Guava loadArray idToIntOffsetAndNumEdges %s".format(id))
        }
        else {
          // Read in the node from disk
          val intArray = new Array[Int](numEdges)
          reader.readIntegersFromOffsetIntoArray(id, idToIntOffset(id), numEdges, intArray, 0)
          intArray
        }
      }
    })

    new GuavaCachedDirectedGraph(nodeIdSet,
      cacheMaxNodesAndEdges, shardDirectories, numShards, idToIntOffset, idToNumEdges,
      maxId, nodeWithOutEdgesMaxId, nodeWithOutEdgesCount,
      inMaxId, nodeWithInEdgesMaxId, nodeWithInEdgesCount,
      nodeCount, edgeCount, storedGraphDir, reader, cacheG)
  }
}

/**
 * This is an implementation of the directed graph trait backed by a Google Guava cache
 * and shards on disk
 * Assigns a couple of nodes per thread, and reuses the nodes each time getNodeById
 * is called.
 * Uses much less memory than NodeArray, but may introduce concurrency issues!
 * @param nodeIdSet nodes with either outgoing or incoming edges
 * @param cacheMaxNodesAndEdges maximum number of nodes and edges that the cache can store
 * @param shardDirectories where shards live on disk
 * @param numShards number of shards to split into
 * @param idToIntOffset offset into a shard on disk
 * @param idToNumEdges offset into a shard on disk and the number of edges
 * @param maxId max node id in the graph
 * @param nodeCount number of nodes in the graph
 * @param edgeCount number of edges in the graph
 * @param storedGraphDir the graph directions stored
 */
class GuavaCachedDirectedGraph (
    val nodeIdSet:(Int => Boolean),
    val cacheMaxNodesAndEdges: Long,
    val shardDirectories:Array[String], val numShards:Int,
    val idToIntOffset:Array[Long], val idToNumEdges:Array[Int],
    maxId: Int, nodeWithOutEdgesMaxId: Int, nodeWithOutEdgesCount: Int,
    inMaxId: Int, nodeWithInEdgesMaxId: Int, nodeWithInEdgesCount: Int,
    val nodeCount: Int, val edgeCount: Long, val storedGraphDir: StoredGraphDir, reader: MultiDirIntShardsReader,
    val cacheG: LoadingCache[Int,Array[Int]])
    extends CachedDirectedGraph(maxId, nodeWithOutEdgesMaxId, nodeWithOutEdgesCount,
      inMaxId, nodeWithInEdgesMaxId, nodeWithInEdgesCount) {

  def getThreadSafeChild = new GuavaCachedDirectedGraph(nodeIdSet,
    cacheMaxNodesAndEdges, shardDirectories, numShards, idToIntOffset, idToNumEdges,
    maxId, nodeWithOutEdgesMaxId, nodeWithOutEdgesCount,
    inMaxId, nodeWithInEdgesMaxId, nodeWithInEdgesCount,
    nodeCount, edgeCount, storedGraphDir, reader, cacheG)

  def getMisses = cacheG.stats().missCount()

  def statsString: String = {
    val stats = cacheG.stats()
    "%s\t%s\t%s\t%s".format(stats.missCount(), stats.requestCount(), stats.missRate(), stats.averageLoadPenalty())
  }

  val cache = new IntArrayCache {
    def get(id: Int) = cacheG.get(id)
    def getStats = throw new IllegalArgumentException("Can't call getStats on this!")
    def getThreadSafeChild = this
  }

  val node = CachedDirectedNode(0, 0, storedGraphDir, cache)
  val someNode = Some(node)
  val emptyNode = EmptyDirectedNode(0, storedGraphDir)
  val someEmptyNode = Some(emptyNode)

  def getNodeById(id: Int) = { // Stats.time("cached_get_node_by_id")
    if (id > maxId || !nodeIdSet(id)) { // Invalid id
      None
    }
    else {
      val numEdges = try { idToNumEdges(id) } catch { case _ => 0 }
      if (numEdges == 0) {
        emptyNode.id = id
        someEmptyNode
      }
      else {
        node.id = id
        node.size = numEdges
        someNode
      }
    }
  }

}