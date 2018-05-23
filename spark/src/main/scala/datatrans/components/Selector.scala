package datatrans.components

import play.api.libs.json.JsValue

trait Selector[I, K] {
  def getIdentifierAndKey(obj: JsValue) : Seq[(I, K)]
}
