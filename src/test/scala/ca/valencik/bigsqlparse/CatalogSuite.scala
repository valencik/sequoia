package ca.valencik.sequoia

import org.scalatest._
import scala.collection.mutable.HashMap

class CatalogSpec extends FlatSpec with Matchers {

  def catalog: Catalog =
    Catalog(
      HashMap(
        "db" -> HashMap(
          "foo" -> Seq("a", "b", "c"),
          "bar" -> Seq("x", "y", "z")
        )))

  "Catalog" should "resolve column names" in {
    catalog.nameColumnInTable("db.foo")("a") shouldBe Some("db.foo.a")
    catalog.nameColumnInTable("db.bar")("x") shouldBe Some("db.bar.x")
    catalog.nameColumnInTable("foo")("a") shouldBe None
    catalog.nameColumnInTable("fake")("a") shouldBe None

    catalog.nameColumnInTable("public.fake")("x") shouldBe Some("public.fake.x")
    catalog.nameColumnInTable("cteAlias.f")("x") shouldBe Some("cteAlias.f.x")
  }

  it should "resolve table names" in {
    catalog.lookupTableName("db.foo") shouldBe Some(QualifiedName("db.foo"))
    catalog.lookupTableName("db.foo.a") shouldBe Some(QualifiedName("db.foo.a"))
    catalog.lookupTableName("fake") shouldBe Some(QualifiedName("public.fake"))
    catalog.lookupTableName("notdb.nope") shouldBe None
    catalog.lookupTableName("1.2.3.4") shouldBe None
  }

  it should "return a new catalog with updated tempViews in addTempViewColumn" in {
    val mutCatalog                                 = Catalog()
    val expectedView: HashMap[String, Seq[String]] = HashMap("cats" -> Seq("name"))
    mutCatalog.addTempViewColumn("cats")("name")
    mutCatalog.tempViews shouldBe expectedView
    mutCatalog.schemaMap shouldBe HashMap.empty
  }
}
