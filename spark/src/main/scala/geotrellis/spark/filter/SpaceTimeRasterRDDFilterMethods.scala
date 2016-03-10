/*
 * Copyright (c) 2016 Azavea.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.spark.filter

import geotrellis.raster._
import geotrellis.spark._
import geotrellis.util.MethodExtensions

import org.apache.spark.rdd._
import org.joda.time.DateTime


abstract class SpaceTimeRasterRDDFilterMethods[K <: SpaceTimeKey, V, M] extends MethodExtensions[RDD[(K, V)] with Metadata[M]] {
  def filterByInstant(instant: DateTime): RDD[(SpatialKey, V)] with Metadata[M] = {
    val rdd = self.mapPartitions({ p =>
      p.flatMap({ case (key, tile) =>
        if (key.time == instant) Some((key.spatialKey, tile))
        else None
      })
    }, preservesPartitioning = true)
    val metadata = self.metadata
    ContextRDD(rdd, metadata)
  }

  def filterByInstant(instant: Long): RDD[(SpatialKey, V)] with Metadata[M] = {
    val dtInstant = SpaceTimeKey(0, 0, instant).time
    filterByInstant(dtInstant)
  }
}
