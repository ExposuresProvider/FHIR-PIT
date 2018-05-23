package datatrans.components

trait DataSource[S, K, V] {
  def get(spark: S, key: K):V
}
