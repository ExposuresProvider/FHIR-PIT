FROM ubuntu:22.04

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y curl wget gnupg python3-pip python3.10-venv openjdk-8-jdk

RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list
RUN curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add

RUN apt-get update && apt-get install -y sbt

RUN mkdir -p /FHIR-PIT/spark /FHIR-PIT/target/scala-2.11 /FHIR-PIT/data/input
COPY ["spark", "/FHIR-PIT/spark"]
COPY ["data/input", "/FHIR-PIT/data/input"]

WORKDIR /FHIR-PIT/spark
RUN sbt -J-Xmx2G 'set test in assembly := {}' assembly

WORKDIR /FHIR-PIT
RUN wget https://archive.apache.org/dist/spark/spark-2.4.8/spark-2.4.8-bin-hadoop2.7.tgz
RUN tar zxvf spark-2.4.8-bin-hadoop2.7.tgz && rm spark-2.4.8-bin-hadoop2.7.tgz
ENV PATH="/FHIR-PIT/spark-2.4.8-bin-hadoop2.7/bin:${PATH}"

RUN wget https://github.com/dhall-lang/dhall-haskell/releases/download/1.42.0/dhall-yaml-1.2.12-x86_64-linux.tar.bz2
RUN tar -xvf dhall-yaml-1.2.12-x86_64-linux.tar.bz2
RUN mv bin/dhall-to-yaml-ng /usr/bin && rm dhall-yaml-1.2.12-x86_64-linux.tar.bz2
RUN dhall-to-yaml-ng --file spark/config/example_demo.dhall --output spark/config/example_demo.yaml

RUN python3 -m venv fhir-pit
ENV PATH="/FHIR-PIT/fhir-pit/bin:${PATH}"
RUN pip install pyspark==2.4.5

WORKDIR /FHIR-PIT/spark
ENTRYPOINT ["python3", "src/main/python/runPreprocPipeline.py"]
CMD ["local", "./config/example_demo.yaml"]
