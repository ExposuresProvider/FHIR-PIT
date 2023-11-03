[![Build Status](https://travis-ci.com/NCATS-Tangerine/FHIR-PIT.svg?branch=master)](https://travis-ci.com/NCATS-Tangerine/FHIR-PIT)

# FHIR PIT



FHIR Patient data Integration Tool (FHIR PIT) uses geocodes and time stamps of varying resolution (e.g., hour, year) to integrate the clinical data with environmental exposures data from multiple sources before stripping the data of PHI (including the geocodes and time stamps) and binning feature variables to create ICEES tables. Of note, FHIR PIT is modular and extensible and can be adapted for virtually any type of data that requires geocodes and dates for integration with PII.


FHIR PIT consists of several transformation steps which are building blocks that can be chained together or combined in parallel to form a transformation workflow. In addition, several of these transformation steps are generic such that they can take in any data that conform to certain format. Adding new types of data amounts to adding new transformation steps or reusing generic steps.


FHIR PIT is implemented using Apache Spark, Python, and Singularity. Spark makes it easy to parallelize and distribute the data transformation. Python is used to simplify the application interface to the transformation steps. Singularity allows us to easily make the application run on different machines and platforms portably.

For details regarding how to build and run FHIR PIT with sample data, refer to [this link](https://github.com/ExposuresProvider/FHIR-PIT/tree/demo-updated/spark).
