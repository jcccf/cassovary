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

import com.twitter.cassovary.util.MultiDirIntShardsReader
import java.util.concurrent.atomic.{AtomicReferenceArray, AtomicLong, AtomicIntegerArray}
import util.Random

object LocklessRandomizedIntArrayCache {

  /**
   * Create a LocklessRandomized Int Array Cache. Evicts elements in a random order.
   * Both reads and writes are lock-free, although for writes, consistency
   * is ensured through atomic updates instead of locks.
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

    new LocklessRandomizedIntArrayCache(shardDirectories, numShards,
      maxId, cacheMaxNodes, cacheMaxEdges,
      idToIntOffset, idToNumEdges,
      new MultiDirIntShardsReader(shardDirectories, numShards),
      new AtomicReferenceArray[Array[Int]](maxId + 1),
      new AtomicIntegerArray(cacheMaxNodes),
      new IntArrayCacheNumbers,
      new AtomicLong
    )
  }
}

class LocklessRandomizedIntArrayCache private(shardDirectories: Array[String], numShards: Int,
                                              maxId: Int, cacheMaxNodes: Int, cacheMaxEdges: Long,
                                              idToIntOffset: Array[Long], idToNumEdges: Array[Int],
                                              val reader: MultiDirIntShardsReader,
                                              val idToArray: AtomicReferenceArray[Array[Int]],
                                              val indexToId: AtomicIntegerArray,
                                              val numbers: IntArrayCacheNumbers,
                                              val currRealCapacity: AtomicLong) extends IntArrayCache {

  val mappy = Map(0 -> 0, 1 -> 3, 2 -> 1, 3 -> 1, 4 -> 0, 5 -> 1, 6 -> 4)

  val rand = new Random

  def getThreadSafeChild = new LocklessRandomizedIntArrayCache(shardDirectories, numShards,
    maxId, cacheMaxNodes, cacheMaxEdges,
    idToIntOffset, idToNumEdges,
    new MultiDirIntShardsReader(shardDirectories, numShards),
    idToArray, indexToId, numbers, currRealCapacity)

  def get(id: Int) = {
    val a = idToArray.get(id)
    if (a != null) {
      numbers.hits += 1
      a
    }
    else {
      numbers.misses += 1
      val numEdges = idToNumEdges(id)
      if (numEdges == 0) {
        throw new NullPointerException("FastLRUIntArrayCache idToIntOffsetAndNumEdges %s".format(id))
      }
      else {
        // Read in array
        val intArray = new Array[Int](numEdges)
        reader.readIntegersFromOffsetIntoArray(id, idToIntOffset(id), numEdges, intArray, 0)

        val a = idToArray.getAndSet(id, intArray)
        if (a == null) {

          var change = numEdges

          // Location to place new id must be constant so that we don't evict an id multiple times
          val idToEvict = indexToId.getAndSet(id % cacheMaxNodes, id)
          if (idToEvict > 0) {
            // Deference idToEvict only if they aren't the same
            if (idToEvict != id) {
              val array = idToArray.getAndSet(idToEvict, null)
              if (array != null) {
                change -= array.size
              }
            }
            else {
              change -= numEdges
            }
          }

          // Then keep evicting until we go under the capacity limit
          var whileCount = 0
          if (currRealCapacity.addAndGet(change) > cacheMaxEdges) {
            while (change > 0) {
              val idToEvict = indexToId.getAndSet(rand.nextInt(cacheMaxNodes), 0)
              if (idToEvict > 0) {
                val array = idToArray.getAndSet(idToEvict, null)
                if (array != null) {
                  currRealCapacity.addAndGet(-array.size)
                  change -= array.size
                }
              }
              whileCount += 1
              if (whileCount % 1000 == 0) {
                println(Thread.currentThread().getId + " Stuck " + currRealCapacity + " " + cacheMaxEdges + " " + indexToId)
              }
            }
          }
        }

        intArray
      }
    }
  }

  def debug() {
    println(currRealCapacity + " " + cacheMaxEdges + " " + indexToId)
  }

  def getStats = {
    (numbers.misses, numbers.hits, -1, currRealCapacity.get)
  }

}
