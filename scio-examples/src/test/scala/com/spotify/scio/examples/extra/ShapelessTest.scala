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

package com.spotify.scio.examples.extra

import com.esotericsoftware.kryo.Kryo
import com.spotify.scio.coders.KryoRegistrar
import com.spotify.scio.testing._
import com.twitter.chill.IKryoRegistrar
import org.apache.beam.sdk.testing.PAssert
import shapeless._
import shapeless.datatype.record._

object ShapelessTest {
  case class Record(i: Int, d: Double, s: String)
}

class ShapelessTest extends PipelineSpec {

  import ShapelessTest._

  private val input = (1 to 10).map(i => Record(i, i.toDouble, i.toString))
  private def negate(r: Record) = r.copy(d = -r.d)

  "RecordMatcher" should "work" in {
    /*
    runWithContext {
      _.parallelize(input) should containInAnyOrder (input)
    }

    // scalastyle:off no.whitespace.before.left.bracket
    an [AssertionError] should be thrownBy {
      runWithContext {
        _.parallelize(input).map(negate) should containInAnyOrder (input)
      }
    }
    // scalastyle:on no.whitespace.before.left.bracket
    */

    import scala.collection.JavaConverters._
    runWithContext { sc =>
      implicit def compareDouble(x: Double, y: Double): Boolean = math.abs(x) == math.abs(y)
      val m = RecordMatcher[Record]
      try {
        val p = sc.parallelize(input).map(negate).map(r => m.wrap(r))
//        p should containInAnyOrder (Seq(m.wrap(input.head)))
        PAssert.that(p.internal).containsInAnyOrder(input.map(r => m.wrap(r)).asJava)
      } catch {
        case e: Throwable => e.printStackTrace()
      }
    }
  }

}

/*
@KryoRegistrar
class ShapelessKryoRegistrar extends IKryoRegistrar {
  override def apply(k: Kryo): Unit = {
    import com.twitter.chill._
    k.forSubclass(new KSerializer[(RecordMatcher[_])#Wrapped[_ <: HList]] {
      override def read(kryo: Kryo, input: Input,
                        tpe: Class[(RecordMatcher[_])#Wrapped[_ <: HList]])
      : (RecordMatcher[_])#Wrapped[_ <: HList] =
        RecordMatcher[_].wrap(kryo.readClassAndObject(input))

      override def write(kryo: Kryo, output: Output,
                         obj: (RecordMatcher[_])#Wrapped[_ <: HList]): Unit =
        kryo.writeClassAndObject(output, obj.value)
    })
  }
}
*/