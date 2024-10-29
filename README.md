[![Build Status](https://travis-ci.com/NCATS-Tangerine/FHIR-PIT.svg?branch=master)](https://travis-ci.com/NCATS-Tangerine/FHIR-PIT)

# FHIR &#174; PIT 
Fast Healthcare Interoperability Resources (FHIR®) is the registered trademark of HL7 and is used with the permission of HL7.

FHIR Patient data Integration Tool (FHIR PIT) is an open-source tool that uses geocodes and time stamps of varying spatiotemporal resolution to integrate clinical data (e.g., electronic health records [EHR]) with environmental exposures data from multiple public sources before stripping the data of Protected Health Information (PHI, including geocodes and time stamps) and binning feature variables to create integrated datasets, whereby rows represent individual patients or subjects and columns represent demographic variables, clinical measures, or exposure estimates. FHIR PIT is modular and extensible and can be adapted for virtually any type of data that requires geocodes and dates for integration with PHI or any Personally Identifiable Information.

FHIR PIT was inspired by and currently supports the [Integrated Clinical and Environmental Exposures Service (ICEES)](https://pubmed.ncbi.nlm.nih.gov/31077269/). The tool was motivated by a need to integrate diverse sources of environmental exposures data with EHR data at the patient level, effectively embedding estimates of environmental exposures such as airborne pollutant exposures within the EHR. ICEES has proven utility as an open-source tool for exploratory analysis of integrated clinical and environmental exposures data, with diverse use-case applications, including asthma and related common pulmonary disorders, primary ciliary dyskinesia and related rare pulmonary disorders, drug-induced liver injury, and coronavirus infection. ICEES is also an integral component of the [Biomedical Data Translator ("Translator") System](https://ncats.nih.gov/research/research-activities/translator/about), funded by the National Center for Advancing Translational Sciences. For additional information on ICEES, including peer-reviewed publications, please see https://github.com/NCATSTranslator/Translator-All/wiki/ICEES.

FHIR PIT consists of several transformation steps, which are building blocks that can be chained together or combined in parallel to form a transformation workflow. In addition, several of these transformation steps are generic such that they can take in any data that conform to a certain format. Adding new types of data amounts to adding new transformation steps or reusing generic steps.

FHIR PIT is implemented using Apache Spark, Python, and Singularity. Spark makes it easy to parallelize and distribute the data transformation. Python is used to simplify the application interface to the transformation steps. Singularity allows the application to run portably on different machines and platforms.

For details regarding how to build and run FHIR PIT with sample data, refer to https://github.com/ExposuresProvider/FHIR-PIT/tree/master/spark. FHIR PIT also has been dockerized to encapsulate the software and platform dependencies and facilitate execution and deployment (see https://github.com/ExposuresProvider/FHIR-PIT/blob/master/Dockerfile). A tutorial is available at https://github.com/ExposuresProvider/FHIR-PIT/tree/master/tutorial. An R-Bookdown version will soon be available.

One important note when running FHIR PIT is that the application may be memory-intensive, depending on the size of the input data. For example, for the sample data available in the GitHub repository, the application requires 4GB RAM to run through all of the data processing and integration steps, when running Docker natively. (Docker Desktop on Windows/Mac may incur additional memory overhead.)

For additional information on FHIR PIT, including an example use-case application, please see [Xu et al. 2020](https://bmcmedinformdecismak.biomedcentral.com/articles/10.1186/s12911-020-1056-9).

Issues are welcome! We will work to resolve any issues as quickly as possible.

# Funding Support

FHIR PIT is funded by the [National Center for Advancing Translational Sciences](https://ncats.nih.gov/), under a US PHS Other Transaction Award (OT2TR003430, prototype was funded under OT3TR002020), with additional funding provided by the [Renaissance Computing Institute](https://renci.org/).

# Suggested Citation

If you use FHIR PIT, we thank you and kindly ask that you consider citing:

Xu H, Cox S, Stillwell L, Pfaff E, Champion J, Ahalt SC, Fecho K. FHIR PIT: an open software application for spatiotemporal integration of clinical data and environmental exposures data. BMC Med Inform Decis Mak. 2020;20(1):53. Published 2020 Mar 11. doi:10.1186/s12911-020-1056-9.

# Contact Information

For assistance with FHIR PIT, please contact the developer team at fhir-pit@renci.org or directly click on [Contact Us](mailto:fhir-pit@renci.org). 

