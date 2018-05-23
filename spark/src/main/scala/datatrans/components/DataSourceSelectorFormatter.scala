package datatrans.components

sealed trait DataSourceSelectorFormatter[S,R] {
  def getOutput(spark: S, obj: R): Seq[(String, ()=>String)]
}

final case class MkDataSourceSelectorFormatter[S,R,I,K,V](selector : Selector[R,I,K], dataSource : DataSource[S,K,V], formatter : Formatter[S, I, V]) extends DataSourceSelectorFormatter[S,R] {
  override def getOutput(spark: S, obj: R): Seq[(String, ()=>String)] = {
    selector.getIdentifierAndKey(obj).map{
      case (i, k) =>
        formatter.format(spark, i, ()=>dataSource.get(spark, k))
    }
  }

}