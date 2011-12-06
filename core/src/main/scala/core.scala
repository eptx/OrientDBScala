package OrientDBScala

import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery
import scala.collection.JavaConverters._
import com.orientechnologies.orient.core.db.{ODatabaseRecordThreadLocal, ODatabaseComplex}
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE

object `package` {

  implicit def dbWrapper[A](db: ODatabaseComplex[A]) = new {
    def q[T](sql: String, params: AnyRef*): List[T] = {
            val params4java = params.toArray
            val results: java.util.List[T] = db.query(new OSQLSynchQuery[T](sql), params4java: _*)
            results.asScala.toList
    }
  }

  def createIndex(className:String, propertyName:String, oType:OType, indexType:INDEX_TYPE ) = ODatabaseRecordThreadLocal.INSTANCE.get.getMetadata.getSchema
    .createClass(className).createProperty(propertyName,oType).createIndex(indexType)
}