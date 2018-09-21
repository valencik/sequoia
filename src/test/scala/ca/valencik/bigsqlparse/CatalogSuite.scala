package ca.valencik.bigsqlparse

import org.scalatest._
import scala.collection.immutable.HashMap

class CatalogSpec extends FlatSpec with Matchers {

  val m: HashMap[String, HashMap[String, Seq[String]]] = HashMap(
    "a" -> HashMap("aa" -> Seq("aaa", "aaaa"), "ah" -> Seq("ahh")),
    "b" -> HashMap("bb" -> Seq("bbb"), "ba"         -> Seq("bah", "bahh")))
  val catalog = Catalog(m)

  "Catalog" should "resolve column names" in {
    catalog.nameColumnInTable("ba")("bah") shouldBe Some("b.ba.bah")
    catalog.nameColumnInTable("aa")("aaaa") shouldBe Some("a.aa.aaaa")
    catalog.nameColumnInTable("b")("nope") shouldBe None
  }

  it should "resolve table names" in {
    catalog.lookupTableName("b.ba") shouldBe Some(QualifiedName("b.ba"))
    catalog.lookupTableName("b.ba.baa") shouldBe Some(QualifiedName("b.ba.baa"))
    catalog.lookupTableName("a.aa") shouldBe Some(QualifiedName("a.aa"))
    catalog.lookupTableName("bah") shouldBe Some(QualifiedName("public.bah"))
    catalog.lookupTableName("notdb.nope") shouldBe None
    catalog.lookupTableName("1.2.3.4") shouldBe None
  }

  it should "return a new catalog with updated tempViews in addTempViewColumn" in {
    val emptyCatalog                               = Catalog()
    val expectedView: HashMap[String, Seq[String]] = HashMap("cats" -> Seq("name"))
    val actual                                     = emptyCatalog.addTempViewColumn("cats")("name")
    actual.tempViews shouldBe expectedView
    actual.schemaMap shouldBe HashMap.empty
  }
}
