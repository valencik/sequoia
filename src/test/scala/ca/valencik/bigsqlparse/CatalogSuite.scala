package ca.valencik.bigsqlparse

import org.scalatest._
import scala.collection.immutable.HashMap

class CatalogSpec extends FlatSpec with Matchers {

  val m: HashMap[String, HashMap[String, Seq[String]]] = HashMap(
    "a" -> HashMap("aa" -> Seq("aaa", "aaaa"), "ah" -> Seq("ahh")),
    "b" -> HashMap("bb" -> Seq("bbb"), "ba"         -> Seq("bah", "bahh")))
  val catalog = new Catalog(m)

  "Catalog" should "resolve column names" in {
    catalog.nameColumnInTable("ba")("bah") shouldBe Some("b.ba.bah")
    catalog.nameColumnInTable("aa")("aaaa") shouldBe Some("a.aa.aaaa")
    catalog.nameColumnInTable("b")("nope") shouldBe None
  }

  it should "resolve table names" in {
    catalog.nameTable("ba") shouldBe Some("b.ba")
    catalog.nameTable("aa") shouldBe Some("a.aa")
    catalog.nameTable("bah") shouldBe None
    catalog.nameTable("nope") shouldBe None
  }
}
