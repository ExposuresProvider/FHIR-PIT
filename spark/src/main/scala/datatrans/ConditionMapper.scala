package datatrans

object ConditionMapper {
  val icd9CondMap = Map(
    "Asthma" -> "(493[.]).*",
    "Croup" -> "(464[.]).*",
    "ReactiveAirway" -> "(496[.]).*",
    "Cough" -> "(786[.]).*",
    "Pneumonia" -> "(48[1-6][.]).*",
    "Obesity" -> "(278[.]).*",
    "UterineCancer" -> "(179|182)[.].*",
    "CervicalCancer" -> "(180)[.].*",
    "OvarianCancer" -> "(183)[.].*",
    "ProstateCancer" -> "(185)[.].*",
    "TesticularCancer" -> "(186)[.].*",
    "KidneyCancer" -> "(189)[.].*",
    "Endometriosis" -> "(617)[.].*",
    "OvarianDysfunction" -> "(256)[.].*",
    "TesticularDysfunction" -> "(257)[.].*",
    "Pregnancy" -> "(V22)[.].*",
    "Menopause" -> "(627)[.].*",
    "Diabetes" -> "(249|250)[.].*",
    "Alopecia" -> "(704)[.].*",
    "Fibromyalgia" -> "(729)[.].*",
    "AlcoholDependence" -> "(303)[.].*",
    "DrugDependence" -> "(304)[.].*",
    "Depression" -> "(311)[.].*",
    "Anxiety" -> "(300)[.].*",
    "Autism" -> "(299)[.].*"
    )

    val icd10CondMap = Map(
      "Asthma" -> "(J45[.]).*",
      "Croup" -> "(J05[.]).*",
      "ReactiveAirway" -> "(J44[.]|J66[.]).*",
      "Cough" -> "(R05[.]).*",
      "Pneumonia" -> "(J1[2-8].).*",
      "Obesity" -> "(E66.[^3]).*",
      "UterineCancer" -> "(C55)[.].*",
      "CervicalCancer" -> "(C53)[.].*",
      "OvarianCancer" -> "(C56)[.].*",
      "ProstateCancer" -> "(C61)[.].*",
      "TesticularCancer" -> "(C62)[.].*",
      "KidneyCancer" -> "(C64)[.].*",
      "Endometriosis" -> "(N80)[.].*",
      "OvarianDysfunction" -> "(E28)[.].*",
      "TesticularDysfunction" -> "(E29)[.].*",
      "Pregnancy" -> "(Z34)[.].*",
      "Menopause" -> "(N95)[.].*",
      "Diabetes" -> "(E08|E09|E10|E11|E13)[.].*",
      "Alopecia" -> "(L63|L64)[.].*",
      "Fibromyalgia" -> "(M79)[.].*",
      "AlcoholDependence" -> "(F10)[.].*",
      "DrugDependence" -> "(F11|F12|F13|F14|F15|F16|F17|F18|F19)[.].*",
      "Depression" -> "(F32|F33)[.].*",
      "Anxiety" -> "(F40|F41)[.].*",
      "Autism" -> "(F84)[.].*"
    )

  def map_condition(system: String, code : String) : Seq[String] = {
    val condMap = system match {
      case "http://hl7.org/fhir/sid/icd-9-cm" => icd9CondMap
      case "http://hl7.org/fhir/sid/icd-10-cm" => icd10CondMap
      case _ => println("unknown system " + system)
        Map()
    }

    condMap.toSeq.flatMap {
      case (dx, res) =>
        if (res.r.pattern.matcher(code).matches()) {
          Seq(dx + "Dx")
        } else {
          Seq()
        }
    }
  }

  val dx = icd9CondMap.keys.toSeq.union(icd10CondMap.keys.toSeq).map(_ + "Dx")

}

