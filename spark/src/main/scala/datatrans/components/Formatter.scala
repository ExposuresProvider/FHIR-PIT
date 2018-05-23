package datatrans.components

trait Formatter[I, V] {
  def format(identifier : I, value : V) : Seq[String]
  def header : Seq[String]
}
