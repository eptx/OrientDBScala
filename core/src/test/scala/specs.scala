package OrientDBScala

import org.scalatest.{BeforeAndAfterAll, FeatureSpec, GivenWhenThen}
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE
import scala.collection.JavaConverters._
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE
import com.orientechnologies.orient.core.db.`object`.ODatabaseObjectTx
import com.orientechnologies.orient.core.record.impl.ODocument

//import Wrapper._
import com.orientechnologies.orient.core.db.graph.{OGraphVertex, ODatabaseGraphTx}
import org.scalatest.Assertions._
import com.orientechnologies.orient.core.metadata.schema.{OClass, OType}


class User(var user:String){
  def this() = this(null)
  override def toString = user
}

class FeaturesSpecTest extends FeatureSpec with GivenWhenThen with BeforeAndAfterAll {
  val maxUserCount = 1000
  val db: ODatabaseDocumentTx = new ODatabaseDocumentTx("local:db")
  if (!db.exists) db.create() else db.open("admin", "admin")

  val objdb: ODatabaseObjectTx = new ODatabaseObjectTx("memory:objdb")
  if (!objdb.exists) objdb.create() else objdb.open("admin", "admin")


  override def afterAll(configMap: Map[String, Any]){
    db.delete()
    db.close()

    //objdb.delete()
    //objdb.close()
  }

	feature("OrientDB Document DB") {
    scenario("Canary is alive") {
      assert(true === true)
    }
			
    scenario("DB insert "+maxUserCount+" records") {
      createIndex("User","user",OType.STRING,INDEX_TYPE.UNIQUE)
      db.declareIntent(new OIntentMassiveInsert())
      db.begin(TXTYPE.NOTX)
      var size = 0
      val doc = new ODocument(db)
      (1 to maxUserCount).foreach{i =>
        doc.reset
        doc.setClassName("User")
        doc.field("user","user"+i)
        size += doc.getSize
        doc.save
      }
      Console.println("Total Bytes: " + size + ", per record: " + (size/maxUserCount))
      db.declareIntent(null)
      val count = db.countClass("User")

      assert(maxUserCount === count)
    }

    scenario("DB Search") {
      val result = db.q[ODocument]("select user from User where user = 'user10'")
      result.foreach(doc => Console.println(doc))

		  assert(result.head.field("user").toString === "user10")
		}
	}

  feature("OrientDB Object DB") {

    scenario("DB insert "+maxUserCount+" records") {
      objdb.declareIntent(new OIntentMassiveInsert())
      objdb.begin(TXTYPE.NOTX)
      objdb.getEntityManager.registerEntityClass(classOf[User])

      (1 to maxUserCount).foreach{i =>
        val u = new User("user"+i)
        objdb.save(u)
      }
      objdb.declareIntent(null)
      val count = objdb.countClass(classOf[User])

      assert(maxUserCount === count)
    }

    scenario("DB Search") {
      val result = objdb.q[User]("select from User where user = 'user10'")

		  assert(result.head.user === "user10")
		}
	}

  feature("OrientDB Graph DB") {

    scenario("DB insert records") {

      def traverse(inNode:OGraphVertex)(op: OGraphVertex => Unit) {
        op(inNode)
        for(node <- inNode.browseOutEdgesVertexes.asScala) yield {
          traverse(node)(op)
        }
      }

      val graph:ODatabaseGraphTx = new ODatabaseGraphTx("memory:graph")
      graph.create()

      val root:OGraphVertex = graph.createVertex.set("id", "root")

      val a:OGraphVertex = graph.createVertex.set("id", "_a")
      val b:OGraphVertex = graph.createVertex.set("id", "_b")
      val c:OGraphVertex = graph.createVertex.set("id", "_c")

      val a1:OGraphVertex = graph.createVertex.set("id", "__a1")
      val a2:OGraphVertex = graph.createVertex.set("id", "__a2")
      val b1:OGraphVertex = graph.createVertex.set("id", "__b1")

      root.link(a)
      root.link(b)
      root.link(c)
      a.link(a1)
      a.link(a2)

      b.link(b1).save

      traverse(root)((n:OGraphVertex) => {
        n.save
        Console.println(n.get("id"))
      })

      val result = graph.q("select from OGraphVertex")

      assert(result.size === 7)


	  }
  }
}