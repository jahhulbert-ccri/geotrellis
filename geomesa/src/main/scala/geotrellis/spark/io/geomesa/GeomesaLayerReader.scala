package geotrellis.spark.io.geomesa

import geotrellis.spark._
import geotrellis.spark.io.avro.AvroRecordCodec
import geotrellis.spark.io.{LayerNotFoundError, LayerQuery, FilteringLayerReader, AttributeStore}
import geotrellis.util.GetComponent
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import spray.json.JsonFormat

import scala.reflect.ClassTag

class GeomesaLayerReader(val attributeStore: AttributeStore)(implicit sc: SparkContext)
  extends FilteringLayerReader[LayerId] {

  val GeomesaAttributeStore(ads) = attributeStore


  /** read
    *
    * This function will read an RDD layer based on a query.
    *
    * @param id              The ID of the layer to be read
    * @param rasterQuery     The query that will specify the filter for this read.
    * @param numPartitions   The desired number of partitions in the resulting RDD.
    * @param indexFilterOnly If true, the reader should only filter out elements who's KeyIndex entries
    *                        do not match the indexes of the query key bounds. This can include keys that
    *                        are not inside the query key bounds.
    * @tparam K Type of RDD Key (ex: SpatialKey)
    * @tparam V Type of RDD Value (ex: Tile or MultibandTile )
    * @tparam M Type of Metadata associated with the RDD[(K,V)]

    */
  override def read[
    K: AvroRecordCodec: Boundable: JsonFormat: ClassTag,
    V: AvroRecordCodec: ClassTag,
    M: JsonFormat: GetComponent[?, Bounds[K]]
  ](id: LayerId, rasterQuery: LayerQuery[K, M], numPartitions: Int, filterIndexOnly: Boolean) = {

    // Startup checks
    if (!attributeStore.layerExists(id)) throw new LayerNotFoundError(id)
    if (implicitly[ClassTag[K]].toString != "geotrellis.spark.SpatialKey") throw new Exception("Unsupported Key Type")


    val LayerId(name, zoom) = id
    val GeomesaAttributeStore(ads) = attributeStore

    // Compute GeoTrellis metadata


    // Set up geomesa query and options

    // create a geomesa rdd
    val rdd = null

    // create a
    new ContextRDD(rdd, null.asInstanceOf[M])
  }

  override def defaultNumPartitions: Int = ???
}
