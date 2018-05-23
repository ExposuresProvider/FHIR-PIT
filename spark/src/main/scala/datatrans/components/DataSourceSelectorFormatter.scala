package datatrans.components

import play.api.libs.json.JsValue

sealed trait DataSourceSelectorFormatter {
  def getRows(obj: JsValue): Seq[Seq[String]]
  def header : Seq[String]
}

final case class MkDataSourceSelectorFormatter[I,K,V](selector : Selector[I,K], dataSource : DataSource[K,V], formatter : Formatter[I, V]) extends DataSourceSelectorFormatter {
  def getRows(obj: JsValue): Seq[Seq[String]] = {
    selector.getIdentifierAndKey(obj).map{
      case (i, k) =>
        formatter.format(i, dataSource.get(k))
    }
  }

  def header : Seq[String] = formatter.header
}