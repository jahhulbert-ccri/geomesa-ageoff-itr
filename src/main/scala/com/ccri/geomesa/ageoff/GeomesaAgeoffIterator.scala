package com.ccri.geomesa.ageoff

import java.util.concurrent.ConcurrentHashMap

import com.ccri.geomesa.ageoff.GeomesaAgeoffIterator.{IteratorCache, _}
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.{Filter, IteratorEnvironment, SortedKeyValueIterator}
import org.apache.hadoop.io.Text
import org.joda.time.format.ISOPeriodFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.locationtech.geomesa.accumulo.iterators.IteratorClassLoader
import org.locationtech.geomesa.features.SerializationOption.{SerializationOption, SerializationOptions}
import org.locationtech.geomesa.features.kryo.{KryoBufferSimpleFeature, KryoFeatureSerializer}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType

/**
  * An iterator that ages off data based on the date stored in the KryoSimpleFeature in the value of a row in
  * Accumulo. This iterator can be configured on scan, minc, and majc to provide data ageoff in GeoMesa
  *
  * See the README.md file for documentation and usage
  */
class GeomesaAgeoffIterator extends Filter {

  private var spec: String = null
  private var dtgIdx: Int = -1
  private var sft: SimpleFeatureType = null
  private var kryo: KryoFeatureSerializer = null
  private var reusableSF: KryoBufferSimpleFeature = null
  private var minTs: Long = -1
  private var reuseText: Text = null

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = {
    val copy = super[Filter].deepCopy(env).asInstanceOf[GeomesaAgeoffIterator]
    copy.spec = spec
    copy.dtgIdx = dtgIdx
    copy.sft = sft

    val kryoOptions = if (copy.sft.getSchemaVersion < 9) SerializationOptions.none else SerializationOptions.withoutId
    copy.kryo = IteratorCache.serializer(copy.spec, kryoOptions)
    copy.reusableSF = kryo.getReusableFeature
    copy.minTs = minTs
    copy.reuseText = new Text()

    copy
  }

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment): Unit = {
    IteratorClassLoader.initClassLoader(getClass)

    super[Filter].init(source, options, env)
    val now = DateTime.now(DateTimeZone.UTC)
    val retention = options.get(Options.RetentionPeriod)
    minTs = minimumTimestamp(now, retention)

    reuseText = new Text()

    spec = options.get(Options.Sft)
    sft = IteratorCache.sft(spec)
    dtgIdx = IteratorCache.dtgIndex(spec, sft)

    val kryoOptions = if (sft.getSchemaVersion < 9) SerializationOptions.none else SerializationOptions.withoutId
    val kryo = IteratorCache.serializer(spec, kryoOptions)
    reusableSF = kryo.getReusableFeature
  }

  override def accept(k: Key, v: Value): Boolean = {
    reusableSF.setBuffer(v.get)
    val ts = reusableSF.getDateAsLong(dtgIdx)
    ts > minTs
  }

}

object GeomesaAgeoffIterator {

  object Options {
    val Sft = "sft"
    val RetentionPeriod = "retention"
  }

  val periodFormat = ISOPeriodFormat.standard()

  def minimumTimestamp(now: DateTime, pStr: String): Long = {
    val p = periodFormat.parsePeriod(pStr)
    now.minus(p).getMillis
  }

  object IteratorCache {
    private val sftCache = new ConcurrentHashMap[String, SimpleFeatureType]()
    private val serializerCache = new ConcurrentHashMap[(String, String), KryoFeatureSerializer]()
    private val dtgIndexCache = new ConcurrentHashMap[String, java.lang.Integer]()

    def sft(spec: String): SimpleFeatureType = {
      val cached = sftCache.get(spec)
      if (cached != null) { cached } else {
        val sft = SimpleFeatureTypes.createType("", spec)
        if (sft == null) throw new IllegalStateException("Sft is null")
        sftCache.put(spec, sft)
        sft
      }
    }

    def serializer(spec: String, options: Set[SerializationOption]): KryoFeatureSerializer = {
      val optString = options.mkString
      val cached = serializerCache.get((spec, optString))
      if (cached != null) { cached } else {
        val serializer = new KryoFeatureSerializer(sft(spec), options)
        if (serializer == null) throw new IllegalStateException("Serializer is null")
        serializerCache.put((spec, optString), serializer)
        serializer
      }
    }

    def dtgIndex(spec:String, sft: SimpleFeatureType): Int = {
      val cached = dtgIndexCache.get(spec)
      if (cached != null) { cached } else {
        import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
        val idx = sft.getDtgIndex.get
        dtgIndexCache.put(spec, idx)
        idx
      }
    }
  }
}