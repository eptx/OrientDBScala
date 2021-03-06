package OrientDBScala

import org.scalatest.{BeforeAndAfterAll, FeatureSpec, GivenWhenThen}
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE
import scala.collection.JavaConverters._
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE
import com.orientechnologies.orient.core.db.`object`.ODatabaseObjectTx
import com.orientechnologies.orient.core.record.impl.ODocument

import com.orientechnologies.orient.core.db.graph.{OGraphVertex, ODatabaseGraphTx}
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract
import com.orientechnologies.orient.core.sql.OSQLEngine
import com.orientechnologies.orient.core.record.ORecord
import com.orientechnologies.orient.core.command.OCommandExecutor
import scala.Double


class User(var user:String){
  def this() = this(null)
  override def toString = user
}

class FeaturesSpecTest extends FeatureSpec with GivenWhenThen with BeforeAndAfterAll {
  val maxUserCount = 1000
  //create a doc db on disk
  val db: ODatabaseDocumentTx = new ODatabaseDocumentTx("local:db")
  if (!db.exists) db.create() else db.open("admin", "admin")
   //create an object db in memory
  val objdb: ODatabaseObjectTx = new ODatabaseObjectTx("memory:objdb")
  if (!objdb.exists) objdb.create() else objdb.open("admin", "admin")

  //after test cleanup
  override def afterAll(configMap: Map[String, Any]){
    db.delete()
    db.close()

    objdb.delete()
    objdb.close()
  }

  //run tests
	feature("As a Document DB") {
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
        doc.field("id",i)
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

    scenario("Custom Function") {
      //add a custom function
      OSQLEngine.getInstance.registerFunction("multxy", new OSQLFunctionAbstract("multxy",2,2) {
        def getSyntax = "multxy(x,y)"
        def execute(iCurrentRecord: ORecord[_], iParameters: Array[Object], iRequester: OCommandExecutor):java.lang.Integer = {
          iParameters(0).asInstanceOf[Int] * iParameters(1).asInstanceOf[Int]
        }
        def aggregateResults = false
      })

      val result = db.q[ODocument]("select multxy(id,2) as double_id from User where user = 'user10'")
		  assert(result.head.field("double_id").asInstanceOf[Int] === 20)
    }
	}

feature("Works with JSON") {
  val jsondb: ODatabaseDocumentTx = new ODatabaseDocumentTx("memory:jsondb")
  jsondb.create()


    scenario("Insert JSON") {
      val doc = new ODocument(jsondb)
      val json = """
{
  "gender": {"name": "Male"},
  "firstName": "Robert",
  "lastName": "Smith",
  "account": {"checking": 10, "savings": 1234}
}
"""
      doc.fromJSON(json)
      doc.setClassName("Person")
      doc.save
      assert(jsondb.countClass("Person") === 1)
    }

    scenario("Search JSON") {
      val result = jsondb.q[ODocument]("select account[savings] from Person")
      result.foreach(doc => Console.println(doc))

		  assert(Int.unbox(result.head.field("account")) === 1234)
		}
	}


  feature("As an Object DB") {

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

  feature("As a Graph DB") {

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