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

import com.twitter.ostrich.stats.Stats
import concurrent.Lock
import scala.collection.mutable
import java.util.concurrent.locks.ReentrantReadWriteLock

/**
 * Array-based LRU algorithm implementation
 * @param shardDirectories
 * @param numShards
 * @param maxId
 * @param cacheMaxNodes
 * @param cacheMaxEdges
 * @param idToIntOffset
 * @param idToNumEdges
 */
class LockfreeReadFastLRUIntArrayCache(shardDirectories: Array[String], numShards: Int,
                                       maxId: Int, cacheMaxNodes: Int, cacheMaxEdges: Long,
                                       idToIntOffset: Array[Long], idToNumEdges: Array[Int]) extends IntArrayCache {

  val shardReader = new MultiDirEdgeShardsReader(shardDirectories, numShards)
  val idToArray = new Array[Array[Int]](maxId + 1)
  val linkedMap = new LinkedIntIntMap(maxId, cacheMaxNodes)
  var currRealCapacity: Long = 0
  var hits, misses: Long = 0
  val lock = new Lock

  val moveHeadBufferMap = new mutable.HashMap[Long, Array[Int]]
  val moveHeadBufferMapPointer = new mutable.HashMap[Long, Int]

  def addToBuffer(threadId: Long, index: Int) {
    try {
      moveHeadBufferMap(threadId)(moveHeadBufferMapPointer(threadId)) = index
      moveHeadBufferMapPointer(threadId) += 1
    }
    catch {
      case _ => {
        moveHeadBufferMap(threadId) = new Array[Int](10)
        moveHeadBufferMap(threadId)(0) = index
        moveHeadBufferMapPointer(threadId) = 1
      }
    }
  }

  def emptyBuffer(threadId: Long) = {
    if (moveHeadBufferMap.contains(threadId)) {
      for (i <- 0 until moveHeadBufferMapPointer(threadId)) {
        val id = moveHeadBufferMap(threadId)(i)
        if (linkedMap.contains(id)) {
          linkedMap.moveToHead(id)
        }
      }
    }
  }

  def bufferIsFull(threadId: Long) = {
    moveHeadBufferMapPointer(threadId) == 10
  }

  def get(id: Int): Array[Int] = {
    val threadId = Thread.currentThread.getId

    var a: Array[Int] = idToArray(id)
    if (a != null) {
      // Manage buffer
      addToBuffer(threadId, id)
      if (bufferIsFull(threadId)) {
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

        // Each EdgeShardReader is synchronized (i.e. 1 reader per shard)
        shardReader.readIntegersFromOffsetIntoArray(id, idToIntOffset(id) * 4, numEdges, intArray, 0)

        lock.acquire
        if (linkedMap.contains(id)) {
          lock.release
          intArray
        }
        else {
          misses += 1

          //println("Going to empty...")
          // Empty buffer
          emptyBuffer(threadId)
          //println("Emptied buffer!")

          // Evict from cache
          currRealCapacity += numEdges
          while (linkedMap.getCurrentSize == cacheMaxNodes || currRealCapacity > cacheMaxEdges) {
            val oldId = linkedMap.getTailId
            currRealCapacity -= idToArray(oldId).length
            idToArray(oldId) = null // Don't need this because it will get overwritten
            linkedMap.removeFromTail()
          }

          linkedMap.addToHead(id)
          idToArray(id) = intArray

          lock.release
          intArray
        }
      }
    }
  }

  def getStats = {
    (misses, hits, linkedMap.getCurrentSize, currRealCapacity)
  }

}
