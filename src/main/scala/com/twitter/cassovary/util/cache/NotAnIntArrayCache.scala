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
package com.twitter.cassovary.util.cache

import com.twitter.cassovary.util.{LinkedIntIntMap, MultiDirIntShardsReader}
import com.twitter.ostrich.stats.Stats
import concurrent.Lock

object NotAnIntArrayCache {
  /**
   * This is definitely not an IntArrayCache. Seriously.
   * It just reads edges directly from disk.
   *
   * @param shardDirectories Directories where edge shards live
   * @param numShards Number of edge shards
   * @param maxId Maximum id that will be requested
   * @param cacheMaxNodes Maximum number of nodes the cache can have
   * @param cacheMaxEdges Maximum number of edges the cache can have
   * @param idToIntOffset Array of node id -> offset in a shard
   * @param idToNumEdges Array of node id -> number of edges
   */
  def apply(shardDirectories: Array[String], numShards: Int,
            maxId: Int, cacheMaxNodes: Int, cacheMaxEdges: Long,
            idToIntOffset: Array[Long], idToNumEdges: Array[Int]) = {

    new NotAnIntArrayCache(shardDirectories, numShards,
      maxId, cacheMaxNodes, cacheMaxEdges,
      idToIntOffset, idToNumEdges,
      new MultiDirIntShardsReader(shardDirectories, numShards))
  }
}

class NotAnIntArrayCache private(shardDirectories: Array[String], numShards: Int,
                                   maxId: Int, cacheMaxNodes: Int, cacheMaxEdges: Long,
                                   idToIntOffset: Array[Long], idToNumEdges: Array[Int],
                                   val reader: MultiDirIntShardsReader) extends IntArrayCache {

  def getThreadSafeChild = new NotAnIntArrayCache(shardDirectories, numShards,
    maxId, cacheMaxNodes, cacheMaxEdges,
    idToIntOffset, idToNumEdges,
    new MultiDirIntShardsReader(shardDirectories, numShards))

  def get(id: Int): Array[Int] = {
    val numEdges = idToNumEdges(id)
    if (numEdges == 0) {
      throw new NullPointerException("FastLRUIntArrayCache idToIntOffsetAndNumEdges %s".format(id))
    }
    else {
      // Read in array
      val intArray = new Array[Int](numEdges)
      reader.readIntegersFromOffsetIntoArray(id, idToIntOffset(id), numEdges, intArray, 0)
      intArray
    }
  }

  def getStats = {
    (0, 0, 0, 0)
  }
}
