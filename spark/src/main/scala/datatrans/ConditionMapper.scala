package datatrans

object ConditionMapper {
  val condMap = Map(
    "Asthma" -> "(493[.]|J45[.]).*",
    "Croup" -> "(464[.]|J05[.]).*",
    "ReactiveAirway" -> "(496[.]|J44[.]|J66[.]).*",
    "Cough" -> "(786[.]|R05[.]).*",
    "Pneumonia" -> "(48[1-6][.]|J1[2-8].).*",
    "Obesity" -> "(278[.]|E66.[^3]).*",
    "UterineCancer" -> "(179|182|C55)[.].*",
    "CervicalCancer" -> "(180|C53)[.].*",
    "OvarianCancer" -> "(183|C56)[.].*",
    "ProstateCancer" -> "(185|C61)[.].*",
    "TesticularCancer" -> "(186|C62)[.].*",
    "KidneyCancer" -> "(189|C64)[.].*",
    "Endometriosis" -> "(617|N80)[.].*",
    "OvarianDysfunction" -> "(256|E28)[.].*",
    "TesticularDysfunction" -> "(257|E29)[.].*",
    "Pregnancy" -> "(V22|Z34)[.].*",
    "Menopause" -> "(627|N95)[.].*",
    "Diabetes" -> "(249|250|E08|E09|E10|E11|E13)[.].*",
    "Alopecia" -> "(704|L63|L64)[.].*",
    "Fibromyalgia" -> "(729|M79)[.].*",
    "AlcoholDependence" -> "(303|F10)[.].*",
    "DrugDependence" -> "(304|F11|F12|F13|F14|F15|F16|F17|F18|F19)[.].*",
    "Depression" -> "(311|F32|F33)[.].*",
    "Anxiety" -> "(300|F40|F41)[.].*",
    "Autism" -> "(299|F84)[.].*"
    )

  def map_condition(code : String) : Seq[String] =
    condMap.toSeq.flatMap {
      case (dx, res) =>
        if (res.r.pattern.matcher(code).matches()) {
          Seq(dx + "Dx")
        } else {
          Seq()
        }
    }

  val dx = condMap.keys.toSeq

}

