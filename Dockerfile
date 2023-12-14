FROM ubuntu:22.04 as sbt_build

RUN apt-get update && apt-get install -y wget openjdk-8-jdk gnupg
RUN apt-get update && apt-get install curl -y

RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list
RUN curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add

RUN apt-get update && apt-get install -y sbt

RUN mkdir /app
COPY ["spark", "/app/"]
WORKDIR /app
RUN sbt -J-Xmx2G assembly

FROM alpine:3.18.5

RUN apk add openjdk8-jre wget gnupg python3 --no-cache

RUN mkdir -p /FHIR-PIT/spark/src/main /FHIR-PIT/target/scala-2.11 /FHIR-PIT/data/input
COPY ["spark/src/main/python", "/FHIR-PIT/spark/src/main/python"]
COPY ["spark/config", "/FHIR-PIT/spark/config"]
COPY ["data/input", "/FHIR-PIT/data/input"]
WORKDIR /FHIR-PIT

COPY --from=sbt_build /app/target/scala-2.11/Preproc-assembly-1.0.jar /FHIR-PIT/target/scala-2.11/Preproc-assembly-1.0.jar

RUN wget https://github.com/dhall-lang/dhall-haskell/releases/download/1.42.0/dhall-yaml-1.2.12-x86_64-linux.tar.bz2
RUN tar -xvf dhall-yaml-1.2.12-x86_64-linux.tar.bz2
RUN mv bin/dhall-to-yaml-ng /usr/bin && rm dhall-yaml-1.2.12-x86_64-linux.tar.bz2
RUN dhall-to-yaml-ng --file spark/config/example_demo.dhall --output spark/config/example_demo.yaml
RUN python -m venv fhir-pit && source fhir-pit/bin/activate

WORKDIR /FHIR-PIT/spark
ENTRYPOINT ["python", "src/main/python/runPreprocPipeline.py"]
CMD ["local", "./config/example_demo.yaml"]
