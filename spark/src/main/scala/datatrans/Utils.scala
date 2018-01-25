package datatrans

object Utils {

  def time[R](block: =>R) : R = {
    val start = System.nanoTime
    val res = block
    val finish = System.nanoTime
    val duration = (finish - start) / 1e9d
    println("time " + duration + "s")
    res
  }

  def updateMap(m: Map[String, Any], keys : Seq[String], r : Map[String, Any]) : Map[String, Any] = keys match {
    case Seq() => 
      r
    case Seq(k, ks@_*) => 
      val m2 = if (!m.isDefinedAt(k)) Map[String,Any]() else m(k).asInstanceOf[Map[String,Any]]
      m + (k -> updateMap(m2, ks, r))
  }

  def mergeMap(m: Map[String, Any], m2 : Map[String, Any]) : Map[String, Any] = 
    m2.foldLeft(m) {
      case (m3, kv) => 
        kv match {
          case (k,v) =>
            if(v.isInstanceOf[Map[_,_]] && m3.isDefinedAt(k))
              m3 + (k -> mergeMap(m3(k).asInstanceOf[Map[String,Any]], v.asInstanceOf[Map[String,Any]]))
            else
              m3 + (k -> v)
        }
      }

  def flattenMap(m :Map[String, Any]) : Map[Seq[String], Any] = {
    def _flattenMap(keys : Seq[String], m: Map[String,Any]) : Seq[(Seq[String], Any)] =
      m.toSeq.flatMap({case (k, v) => 
        val keys2 = keys :+ k
        if (v.isInstanceOf[Map[_,_]])
          _flattenMap(keys2, v.asInstanceOf[Map[String,Any]])
        else
          Seq((keys2, v))})
    Map(_flattenMap(Seq(), m):_*)
  }

}
