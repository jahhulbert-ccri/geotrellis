package geotrellis.spark.io.geomesa

import java.nio.charset.StandardCharsets
import java.util.UUID

import geotrellis.spark._
import geotrellis.spark.io._
import org.geotools.data.{Query, Transaction}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.DefaultFeatureCollection
import org.locationtech.geomesa.accumulo.data.{NoSplitter, AccumuloDataStore}
import org.locationtech.geomesa.accumulo.data.tables.{AttributeTable, RecordTable}
import org.locationtech.geomesa.accumulo.util.AccumuloSftBuilder
import org.locationtech.geomesa.features.ScalaSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.SftBuilder
import org.opengis.filter.Filter
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.JavaConversions._

// Might be crazy but I think this is just storing key/value pairs
// so i dumped them in some geo-less schema table so we can read them
// probably don't even need attribute indexing
//
// Perhaps this could be backed by geomesa's existing metadata store
class GeomesaAttributeStore(ads: AccumuloDataStore) extends DiscreteLayerAttributeStore with org.apache.spark.Logging {

  val sft = new AccumuloSftBuilder()
    .stringType("layerName", index=true)
    .intType("zoom")
    .stringType("attrName", index=true)
    .bytes("attrValue")
    .withIndexes(List(RecordTable.suffix))
    .recordSplitter(classOf[NoSplitter], Map.empty[String, String])
    .build("geotrellis_attribute")

  ads.createSchema(sft)

  protected val ff = CommonFactoryFinder.getFilterFactory2()

  override def read[T: JsonFormat](layerId: LayerId, attributeName: String): T = {
    val filter = ff.and(List(
      ff.equals(ff.property("layerName"), ff.literal(layerId.name)),
      ff.equals(ff.property("zoom"),      ff.literal(layerId.zoom)),
      ff.equals(ff.property("attrName"),  ff.literal(attributeName))
    ))
    val query = new Query(sft.getTypeName, filter, Array[String]("attrValue"))
    import org.locationtech.geomesa.utils.geotools.Conversions._

    val json = ads.getFeatureReader(query, Transaction.AUTO_COMMIT).getIterator.toIterable.headOption.map { sf =>
      new String(sf.get[Array[Byte]]("attrValue"), StandardCharsets.UTF_8)
    }.getOrElse(throw new AttributeNotFoundError(attributeName, layerId))

    json.parseJson.convertTo[(LayerId, T)]._2
  }

  override def layerExists(layerId: LayerId): Boolean = {
    val filter = ff.and(
      ff.equals(ff.property("layerName"), ff.literal(layerId.name)),
      ff.equals(ff.property("zoom"), ff.literal(layerId.zoom))
    )
    val query = new Query(sft.getTypeName, Filter.INCLUDE, Array[String]("layerName"))

    ads.getFeatureReader(query, Transaction.AUTO_COMMIT).getIterator.nonEmpty
  }

  override def write[T: JsonFormat](layerId: LayerId, attributeName: String, value: T): Unit = {
    val sf = ScalaSimpleFeatureFactory.buildFeature(sft,
      Seq[AnyRef](
        layerId.name,
        layerId.zoom.asInstanceOf[AnyRef],
        attributeName,
        (layerId, value).toJson.compactPrint.getBytes(StandardCharsets.UTF_8)
      ), UUID.randomUUID().toString)

    val fc = new DefaultFeatureCollection(null, sft)
    fc.add(sf)
    ads.getFeatureSource("geotrellis_attribute").addFeatures(fc)
  }

  override def delete(layerId: LayerId): Unit = ???

  override def delete(layerId: LayerId, attributeName: String): Unit = ???

  override def layerIds: Seq[LayerId] = {
    val query = new Query(sft.getTypeName, Filter.INCLUDE, Array[String]("layerName", "zoom"))

    ads.getFeatureReader(query, Transaction.AUTO_COMMIT).getIterator.map { sf =>
      val name = sf.get[String]("layerName")
      val zoom = sf.get[Int]("zoom")
      new LayerId(name, zoom)
    }.toSeq
  }

  override def readAll[T: JsonFormat](attributeName: String): Map[LayerId, T] = ???

  override def availableAttributes(id: LayerId): Seq[String] = {
    val filter = ff.and(
      ff.equals(ff.property("layerName"), ff.literal(id.name)),
      ff.equals(ff.property("zoom"), ff.literal(id.zoom))
    )
    val query = new Query(sft.getTypeName, filter, Array[String]("attrName"))
    import org.locationtech.geomesa.utils.geotools.Conversions._

    ads.getFeatureReader(query, Transaction.AUTO_COMMIT).getIterator.map{ sf =>
      sf.get[String]("attrName")
    }.toSeq

  }

  def getAccumuloDataStore = ads
}

object GeomesaAttributeStore {
  def unapply(attrStore: GeomesaAttributeStore) = Some(attrStore.getAccumuloDataStore)
}
