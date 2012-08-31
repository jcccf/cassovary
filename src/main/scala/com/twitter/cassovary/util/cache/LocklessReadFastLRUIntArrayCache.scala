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

import com.twitter.ostrich.stats.Stats
import com.twitter.cassovary.util.{LinkedIntIntMap, MultiDirIntShardsReader}
import concurrent.Lock

object LocklessReadFastLRUIntArrayCache {

  /**
   * Similar to the {@code BufferedFastLRUIntArrayCache} except that
   * this version has lock-free reads when not updating the cache. However,
   * this version consumes more memory than the BufferedFastLRUIntArrayCache
   * and the FastLRUIntArrayCache as a cost of allowing lock-free reads.
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
            idToIntOffset: Array[Long], idToNumEdges: Array[Int]): LocklessReadFastLRUIntArrayCache = {

    new LocklessReadFastLRUIntArrayCache(shardDirectories, numShards,
      maxId, cacheMaxNodes, cacheMaxEdges,
      idToIntOffset, idToNumEdges,
      new MultiDirIntShardsReader(shardDirectories, numShards),
      new Array[Array[Int]](maxId + 1),
      new LinkedIntIntMap(maxId, cacheMaxNodes),
      new IntArrayCacheNumbers,
      new Lock)
  }
}

class LocklessReadFastLRUIntArrayCache private(shardDirectories: Array[String], numShards: Int,
                                               maxId: Int, cacheMaxNodes: Int, cacheMaxEdges: Long,
                                               idToIntOffset: Array[Long], idToNumEdges: Array[Int],
                                               val shardReader: MultiDirIntShardsReader,
                                               val idToArray: Array[Array[Int]],
                                               val linkedMap: LinkedIntIntMap,
                                               val numbers: IntArrayCacheNumbers,
                                               val lock: Lock) extends IntArrayCache {


  val bufferArray = new Array[Int](10)
  var bufferPointer = 0

  def getThreadSafeChild = new LocklessReadFastLRUIntArrayCache(shardDirectories, numShards,
    maxId, cacheMaxNodes, cacheMaxEdges,
    idToIntOffset, idToNumEdges,
    new MultiDirIntShardsReader(shardDirectories, numShards),
    idToArray, linkedMap, numbers, lock)

  def addToBuffer(threadId: Long, index: Int) {
    bufferArray(bufferPointer) = index
    bufferPointer += 1
  }

  def emptyBuffer(threadId: Long) = {
    var i = 0
    while (i < bufferPointer) {
      val id = bufferArray(i)
      if (linkedMap.contains(id)) {
        linkedMap.moveToHead(id)
      }
      i += 1
    }
    bufferPointer = 0
  }

  def get(id: Int): Array[Int] = {
    val threadId = Thread.currentThread.getId

    var a: Array[Int] = idToArray(id)
    if (a != null) {
      // Manage buffer
      numbers.hits += 1
      addToBuffer(threadId, id)
      if (bufferPointer == 10) {
        lock.acquire
        emptyBuffer(threadId)
        lock.release
      }

      a
    }
    else Stats.time("fastlru_miss") {

      val numEdges = idToNumEdges(id)
      if (numEdges == 0) {
        throw new NullPointerException("FastLRUIntArrayCache idToIntOffsetAndNumEdges %s".format(id))
      }
      else {
        // Read in array
        val intArray = new Array[Int](numEdges)

        shardReader.readIntegersFromOffsetIntoArray(id, idToIntOffset(id), numEdges, intArray, 0)

        lock.acquire
        if (linkedMap.contains(id)) {
          lock.release
          intArray
        }
        else {
          numbers.misses += 1

          // Empty buffer
          emptyBuffer(threadId)

          // Evict from cache
          numbers.currRealCapacity += numEdges
          while (linkedMap.getCurrentSize == cacheMaxNodes || numbers.currRealCapacity > cacheMaxEdges) {
            val oldId = linkedMap.getTailId
            numbers.currRealCapacity -= idToArray(oldId).length
            idToArray(oldId) = null // Don't need this because it will get overwritten
            linkedMap.removeFromTail()
          }

          linkedMap.addToHeadAndNotExists(id)
          idToArray(id) = intArray

          lock.release
          intArray
        }
      }
    }
  }

  def getStats = {
    (numbers.misses, numbers.hits, linkedMap.getCurrentSize, numbers.currRealCapacity)
  }

}
