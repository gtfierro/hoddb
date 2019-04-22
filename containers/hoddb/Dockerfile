FROM ubuntu:18.04
RUN apt-get -y update && apt-get install -y git libraptor2-dev libssl-dev
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
RUN mkdir /app 
ADD . /app/ 
WORKDIR /app 
CMD ["./log", "-config", "/etc/hod/hodconfig.yml"]
