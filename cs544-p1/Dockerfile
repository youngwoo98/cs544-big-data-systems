FROM ubuntu:22.04
RUN apt-get update \
	&& apt-get install -y wget \ 
	&& apt-get install unzip
COPY count.sh /var/run/count.sh
CMD ["./var/run/count.sh"]
