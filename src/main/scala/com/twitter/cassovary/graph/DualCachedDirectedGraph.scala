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

import com.twitter.cassovary.util.cache._
import node.DualCachedDirectedNode
import scala.Some
import com.twitter.cassovary.util.DiskIntArrayReader

case class InDiskParams(enabled: Boolean, offsetLengthFilename: String)

object FastDualCachedDirectedGraph {

  /**
   * Create a CachedDirectedGraph that has access to both the in-edges and the out-edges.
   *
   * @param nodeIdSet Function that checks to see if a node id is present in the graph
   * @param cacheMaxNodes Maximum number of nodes stored in the cache
   * @param cacheMaxEdges Maximum number of edges stored in the cache
   * @param shardDirectories Directory to store out-shards (that make up all the edges of a graph)
   * @param inShardDirectories Directory to store in-shards (that make up all the edges of a graph)
   * @param numShards Number of shards to split into
   * @param idToIntOffsetOut Offset into a shard for a specific id (out-edges)
   * @param idToNumEdgesOut Number of out-edges for a specific id
   * @param idToIntOffsetIn Offset into a shard for a specific id (in-edges)
   * @param idToNumEdgesIn Number of in-edges for a specific id
   * @param maxId Original MaxId in the out-edge graph
   * @param realMaxId Actual MaxId (only different from maxId if the graph was renumbered)
   * @param realMaxIdOutEdges
   * @param realMaxIdInEdges
   * @param nodeWithOutEdgesMaxId
   * @param nodeWithOutEdgesCount
   * @param inMaxId Original MaxId in the in-edge graph
   * @param nodeWithInEdgesMaxId
   * @param nodeWithInEdgesCount
   * @param nodeCount Total number of nodes
   * @param edgeCount Total number of out-edges
   * @param cacheType
   * @param nodeType
   * @param inDiskParams
   * @return
   */
  def apply(nodeIdSet: (Int => Boolean),
            cacheMaxNodes: Int, cacheMaxEdges: Long,
            shardDirectories: Array[String], inShardDirectories: Array[String], numShards: Int,
            idToIntOffsetOut: Array[Long], idToNumEdgesOut: Array[Int],
            idToIntOffsetIn: Array[Long], idToNumEdgesIn: Array[Int],
            maxId: Int, realMaxId: Int, realMaxIdOutEdges: Int, realMaxIdInEdges: Int,
            nodeWithOutEdgesMaxId: Int, nodeWithOutEdgesCount: Int,
            inMaxId: Int, nodeWithInEdgesMaxId: Int, nodeWithInEdgesCount: Int,
            nodeCount: Int, edgeCount: Long,
            cacheType: String = "lru", nodeType: String = "node", inDiskParams: InDiskParams): CachedDirectedGraph = {

    def makeCache(shardDirs: Array[String], idToIntOffset: Array[Long], idToNumEdges: Array[Int], realMaxId: Int) = cacheType match {
      case "lru" => FastLRUIntArrayCache(shardDirs, numShards,
        realMaxId, cacheMaxNodes, cacheMaxEdges, idToIntOffset, idToNumEdges)
      case "bufflru" => BufferedFastLRUIntArrayCache(shardDirs, numShards,
        realMaxId, cacheMaxNodes, cacheMaxEdges, idToIntOffset, idToNumEdges)
      case "lockfreereadlru" => LocklessReadFastLRUIntArrayCache(shardDirs, numShards,
        realMaxId, cacheMaxNodes, cacheMaxEdges, idToIntOffset, idToNumEdges)
      case "random" => RandomizedIntArrayCache(shardDirs, numShards,
        realMaxId, cacheMaxNodes, cacheMaxEdges, idToIntOffset, idToNumEdges)
      case "locklessrandom" => LocklessRandomizedIntArrayCache(shardDirs, numShards,
        realMaxId, cacheMaxNodes, cacheMaxEdges, idToIntOffset, idToNumEdges)
      case "clock" => FastClockIntArrayCache(shardDirs, numShards,
        realMaxId, cacheMaxNodes, cacheMaxEdges, idToIntOffset, idToNumEdges)
      case _ => throw new IllegalArgumentException("Unknown cacheType %s".format(nodeType))
    }

    val outCache = makeCache(shardDirectories, idToIntOffsetOut, idToNumEdgesOut, realMaxIdOutEdges)
    // inCache only created if inDisk is false

    (inDiskParams.enabled, nodeType) match {
      case (false, "node") => new FastDualCachedDirectedGraph(nodeIdSet,
        cacheMaxNodes, cacheMaxEdges,
        shardDirectories, numShards,
        idToIntOffsetOut, idToNumEdgesOut,
        idToIntOffsetIn, idToNumEdgesIn,
        maxId, realMaxId, realMaxIdOutEdges, realMaxIdInEdges,
        nodeWithOutEdgesMaxId, nodeWithOutEdgesCount,
        inMaxId, nodeWithInEdgesMaxId, nodeWithInEdgesCount,
        nodeCount, edgeCount, outCache,
        makeCache(inShardDirectories, idToIntOffsetIn, idToNumEdgesIn, realMaxIdInEdges))
      case (true, "node") => new InDiskFastDualCachedDirectedGraph(nodeIdSet,
        cacheMaxNodes, cacheMaxEdges,
        idToIntOffsetOut, idToNumEdgesOut,
        maxId, realMaxId, realMaxIdOutEdges, realMaxIdInEdges,
        nodeWithOutEdgesMaxId, nodeWithOutEdgesCount,
        inMaxId, nodeWithInEdgesMaxId, nodeWithInEdgesCount,
        nodeCount, edgeCount, outCache,
        new DiskIntArrayReader(inDiskParams.offsetLengthFilename, inShardDirectories, numShards))
      case _ => throw new IllegalArgumentException("Unknown nodeType %s asked of FastDualCachedDirectedGraph".format(nodeType))
    }
  }
}

/**
 * CachedDirectedGraph that stores both in-edges and out-edges in individual caches.
 * Requires double the amount of memory of a regular CachedDirectedGraph that only stores a single direction.
 */
class FastDualCachedDirectedGraph(val nodeIdSet: (Int => Boolean),
                                  val cacheMaxNodes: Int, val cacheMaxEdges: Long,
                                  val shardDirectories: Array[String], val numShards: Int,
                                  val idToIntOffsetOut: Array[Long], val idToNumEdgesOut: Array[Int],
                                  val idToIntOffsetIn: Array[Long], val idToNumEdgesIn: Array[Int],
                                  maxId: Int, realMaxId: Int, realMaxIdOutEdges: Int, realMaxIdInEdges: Int,
                                  nodeWithOutEdgesMaxId: Int, nodeWithOutEdgesCount: Int,
                                  inMaxId: Int, nodeWithInEdgesMaxId: Int, nodeWithInEdgesCount: Int,
                                  val nodeCount: Int, val edgeCount: Long,
                                  val outCache: IntArrayCache, val inCache: IntArrayCache)
  extends CachedDirectedGraph(maxId, realMaxId, nodeWithOutEdgesMaxId, nodeWithOutEdgesCount,
    inMaxId, nodeWithInEdgesMaxId, nodeWithInEdgesCount) {

  val storedGraphDir = StoredGraphDir.BothInOut

  def getThreadSafeChild = new FastDualCachedDirectedGraph(nodeIdSet,
    cacheMaxNodes, cacheMaxEdges,
    shardDirectories, numShards,
    idToIntOffsetOut, idToNumEdgesOut,
    idToIntOffsetIn, idToNumEdgesIn,
    maxId, realMaxId, realMaxIdOutEdges, realMaxIdInEdges,
    nodeWithOutEdgesMaxId, nodeWithOutEdgesCount,
    inMaxId, nodeWithInEdgesMaxId, nodeWithInEdgesCount,
    nodeCount, edgeCount,
    outCache.getThreadSafeChild, inCache.getThreadSafeChild)

  def getMisses = outCache.getStats._1 + inCache.getStats._1

  def statsString: String = {
    val (misses, hits, currentSize, currRealCapacity) = outCache.getStats
    val (misses2, hits2, currentSize2, currRealCapacity2) = inCache.getStats
    "%s\t%s\t%s\t%s\t%s".format(misses, hits + misses, misses.toDouble / (hits + misses), currentSize, currRealCapacity) +
      "%s\t%s\t%s\t%s\t%s".format(misses2, hits2 + misses2, misses2.toDouble / (hits2 + misses2), currentSize2, currRealCapacity2)
  }

  val shapeShiftingNode = DualCachedDirectedNode.shapeShifter(0, idToNumEdgesOut, outCache, idToNumEdgesIn, inCache)
  val someShapeShiftingNode = Some(shapeShiftingNode)

  def getNodeById(id: Int) = {
    if (id > realMaxId || !nodeIdSet(id)) {
      None
    }
    else {
      shapeShiftingNode.id = id
      someShapeShiftingNode
    }
  }

}

/**
 * Version of graph that stores both in and out neighbors that always reads in-neighbors from disk.
 * Requires marginally more memory than the regular Out-edge-only CachedDirectedGraph, at the cost of
 * in-edge reads being expensive disk reads.
 */
class InDiskFastDualCachedDirectedGraph(val nodeIdSet: (Int => Boolean),
                                        val cacheMaxNodes: Int, val cacheMaxEdges: Long,
                                        val idToIntOffsetOut: Array[Long], val idToNumEdgesOut: Array[Int],
                                        maxId: Int, realMaxId: Int, realMaxIdOutEdges: Int, realMaxIdInEdges: Int,
                                        nodeWithOutEdgesMaxId: Int, nodeWithOutEdgesCount: Int,
                                        inMaxId: Int, nodeWithInEdgesMaxId: Int, nodeWithInEdgesCount: Int,
                                        val nodeCount: Int, val edgeCount: Long,
                                        val outCache: IntArrayCache, val inReader: DiskIntArrayReader)
  extends CachedDirectedGraph(maxId, realMaxId, nodeWithOutEdgesMaxId, nodeWithOutEdgesCount,
    inMaxId, nodeWithInEdgesMaxId, nodeWithInEdgesCount) {

  val storedGraphDir = StoredGraphDir.BothInOut

  def getThreadSafeChild = new InDiskFastDualCachedDirectedGraph(nodeIdSet,
    cacheMaxNodes, cacheMaxEdges,
    idToIntOffsetOut, idToNumEdgesOut,
    maxId, realMaxId, realMaxIdOutEdges, realMaxIdInEdges,
    nodeWithOutEdgesMaxId, nodeWithOutEdgesCount,
    inMaxId, nodeWithInEdgesMaxId, nodeWithInEdgesCount,
    nodeCount, edgeCount,
    outCache.getThreadSafeChild, inReader.getThreadSafeChild)

  def getMisses = outCache.getStats._1

  def statsString: String = {
    val (misses, hits, currentSize, currRealCapacity) = outCache.getStats
    "%s\t%s\t%s\t%s\t%s".format(misses, hits + misses, misses.toDouble / (hits + misses), currentSize, currRealCapacity)
  }

  val shapeShiftingNode = DualCachedDirectedNode.inDiskShapeShifter(0, idToNumEdgesOut, outCache, inReader)
  val someShapeShiftingNode = Some(shapeShiftingNode)

  def getNodeById(id: Int) = {
    if (id > realMaxId || !nodeIdSet(id)) {
      None
    }
    else {
      shapeShiftingNode.id = id
      someShapeShiftingNode
    }
  }

}