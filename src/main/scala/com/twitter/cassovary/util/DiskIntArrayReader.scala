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

import java.io._

class LongIntReader(filename: String) {
  val rf = new RandomAccessFile(filename, "r")

  def readLongAndIntegerFromOffset(offset: Int) = {
    rf.seek(offset * 12)
    val l = rf.readLong()
    val i = rf.readInt()
    (l, i)
  }

  def readLongAndIntArray = {
    val dis = new DataInputStream(new BufferedInputStream(new FileInputStream(filename)))
    val size = (rf.length() / 12).toInt
    val longArray = new Array[Long](size)
    val intArray = new Array[Int](size)
    var i = 0
    while (i < size) {
      longArray(i) = dis.readLong()
      intArray(i) = dis.readInt()
      i += 1
    }
    dis.close()
    (longArray, intArray)
  }

  def close() { rf.close() }
}

class LongIntWriter(filename: String) {
  val dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(filename)))

  def writeLongArrayAndIntArray(longArray: Array[Long], intArray: Array[Int]) {
    if (longArray.length != intArray.length) throw new IllegalArgumentException("Must write similar length long and int array!")
    var i = 0
    while (i < longArray.length) {
      dos.writeLong(longArray(i))
      dos.writeInt(intArray(i))
      i += 1
    }
  }

  def close() { dos.close() }

}

class DiskIntArrayReader(offsetLengthFilename: String, shardDirectories: Array[String], numShards: Int) {

  val olReader = new LongIntReader(offsetLengthFilename)
  val reader = new MultiDirIntShardsReader(shardDirectories, numShards)
  val empty = Array.empty[Int]

  def getThreadSafeChild = new DiskIntArrayReader(offsetLengthFilename, shardDirectories, numShards)

  def getNumEdges(id: Int) = olReader.readLongAndIntegerFromOffset(id)._2

  def get(id: Int) = {
    val (offset, length) = olReader.readLongAndIntegerFromOffset(id)
    if (length > 0) {
      val intArray = new Array[Int](length)
      reader.readIntegersFromOffsetIntoArray(id, offset, length, intArray, 0)
      intArray
    }
    else {
      empty
    }
  }

}
