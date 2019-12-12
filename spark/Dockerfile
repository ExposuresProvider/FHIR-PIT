FROM ubuntu:18.04

RUN apt-get update && apt-get install -y wget openjdk-8-jdk gnupg

RUN echo "deb https://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823
RUN apt-get update && apt-get install -y sbt
RUN mkdir /app
RUN mkdir /app/project
COPY ["src", "/app/src"]
COPY ["build.sbt", "/app"]
COPY ["project/build.properties", "/app/project"]
COPY ["project/plugins.sbt", "/app/project"]
COPY ["config", "/app/config"]
WORKDIR /app
RUN sbt assembly

RUN wget https://github.com/dhall-lang/dhall-haskell/releases/download/1.28.0/dhall-yaml-1.0.0-x86_64-linux.tar.bz2
RUN tar -xvf dhall-yaml-1.0.0-x86_64-linux.tar.bz2
RUN mv bin/dhall-to-yaml-ng /usr/bin

ENTRYPOINT ["python", "src/main/python/runPreprocPipeline.py"]
CMD ["local", "/app/config/example.dhall"]
