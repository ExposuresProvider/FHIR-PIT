package datatrans.components

trait DataSource[K, V] {
  def get(key: K):V
}
