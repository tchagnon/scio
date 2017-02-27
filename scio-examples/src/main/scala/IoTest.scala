/*
 * Copyright 2017 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.spotify.scio._
import org.apache.beam.sdk.io.{FileBasedSink, TFRecordIO, TextIO}

object IoTest {

  val lines = (1 to 100).map("line-%05d".format(_))

  def main(cmdlineArgs: Array[String]): Unit = {
    val args = Args(cmdlineArgs)
    val path = args("path")
//    writeText(path)
//    readText(path + "-*.deflate")

    val cts = Seq(
      TFRecordIO.CompressionType.NONE,
      TFRecordIO.CompressionType.DEFLATE,
      TFRecordIO.CompressionType.GZIP
    )
    for (ct <- cts) {
      writeTf(path + "-" + ct.name().toLowerCase, ct)
      readTf(path + "-" + ct.name().toLowerCase + "-*", ct)
    }
  }

  def writeText(path: String): Unit = {
    val sc = ScioContext()
    val transform = TextIO.Write
      .to(path)
      .withWritableByteChannelFactory(FileBasedSink.CompressionType.DEFLATE)
    sc.parallelize(lines)
      .saveAsCustomOutput("out", transform)
    sc.close()
  }

  def readText(path: String): Unit = {
    val sc = ScioContext()
    val transform = TextIO.Read
      .from(path)
      .withCompressionType(TextIO.CompressionType.DEFLATE)
    val p = sc.customInput("in", transform)
//    p.debug()
    p.count.debug()
    sc.close()
  }

  def writeTf(path: String, ct: TFRecordIO.CompressionType): Unit = {
    val sc = ScioContext()
    val transform = TFRecordIO.Write
      .to(path)
      .withCompressionType(ct)
    sc.parallelize(lines)
      .map(_.getBytes)
      .saveAsCustomOutput("out", transform)
    sc.close()
  }

  def readTf(path: String, ct: TFRecordIO.CompressionType): Unit = {
    val sc = ScioContext()
    val transform = TFRecordIO.Read
      .from(path)
      .withCompressionType(ct)
    val p = sc.customInput("in", transform).map(new String(_))
//    p.debug()
    p.count.debug()

    sc.close()
  }

}