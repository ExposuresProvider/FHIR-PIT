package datatrans.components

trait Formatter[S, I, V] {
  def format(spark:S, identifier : I, value : ()=>V) : (String, ()=>String)
}
