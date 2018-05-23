package datatrans.components

trait Selector[R, I, K] {
  def getIdentifierAndKey(obj: R) : Seq[(I, K)]
}
