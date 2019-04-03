package ca.valencik.sequoia

case class Resolver private (
    c: Map[String, Set[String]], // immutable global database catalog
    ctes: Map[String, Set[String]], // ctes in scope
    r: Set[String], // relations in scope
    s: Set[String] // projection in current scope's SELECT clause
) {
  def relationIsInCatalog(rt: RawName): Boolean = c.contains(rt.value)
  def addRelationToScope(rt: RawName): Resolver = this.copy(r = r + rt.value)
  def relationIsAlias(rt: RawName): Boolean     = ctes.contains(rt.value)
  def addAliasToScope(rt: RawName): Resolver    = this.copy(r = r + rt.value)

  // TODO Can I clean up these flattens?
  // TODO I should be careful not to lose dups by using Set, I want to report these as errors
  lazy val columnsInScope: Set[String] = r.flatMap { rn =>
    Set(c.get(rn), ctes.get(rn)).flatten
  }.flatten
  def columnIsInScope(rc: RawName): Boolean = columnsInScope(rc.value)

  def addCTE(alias: String, cols: Set[String]): Resolver =
    this.copy(ctes = ctes.updated(alias, cols))
  def resetRelationScope(): Resolver = this.copy(r = Set.empty, s = Set.empty)
}
object Resolver {
  def apply(c: Map[String, Set[String]]): Resolver =
    new Resolver(c, Map.empty, Set.empty, Set.empty)

}
