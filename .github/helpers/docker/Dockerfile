FROM ubuntu:bionic-20200219 AS tmp
WORKDIR /build
RUN apt-get update && apt-get install -y curl wget zip
RUN wget https://download.java.net/java/GA/jdk15.0.1/51f4f36ad4ef43e39d0dfdbaf6549e32/9/GPL/openjdk-15.0.1_linux-x64_bin.tar.gz
RUN tar -xzvf openjdk-15.0.1_linux-x64_bin.tar.gz
RUN ls && mv jdk-15.0.1 jre
ADD ./cli ./cli
ADD ./core ./core
ADD ./plugin-interface ./plugin-interface
ADD ./plugin ./plugin
ADD ./ee ./ee
ADD ./config.yaml ./config.yaml
ADD ./version.yaml ./version.yaml

RUN ls && ./jre/bin/java -classpath "./cli/*" io.supertokens.cli.Main true $@

FROM debian:bookworm-slim
RUN groupadd supertokens && useradd -m -s /bin/bash -g supertokens supertokens
RUN apt-get update && apt-get install -y --no-install-recommends gnupg dirmngr curl && rm -rf /var/lib/apt/lists/*
RUN DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get -y install tzdata
ENV GOSU_VERSION=1.7
RUN set -x \
	&& apt-get update && apt-get install -y --no-install-recommends ca-certificates wget && rm -rf /var/lib/apt/lists/* \
	&& wget -O /usr/local/bin/gosu "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$(dpkg --print-architecture)" \
	&& wget -O /usr/local/bin/gosu.asc "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$(dpkg --print-architecture).asc" \
	&& export GNUPGHOME="$(mktemp -d)" \
	&& gpg --batch --keyserver hkps://keys.openpgp.org --recv-keys B42F6819007F00F88E364FD4036A9C25BF357DD4 \
	&& gpg --batch --verify /usr/local/bin/gosu.asc /usr/local/bin/gosu \
	&& gpgconf --kill all \
	&& rm -rf "$GNUPGHOME" /usr/local/bin/gosu.asc \
	&& chmod +x /usr/local/bin/gosu \
	&& apt-get purge -y --auto-remove wget
COPY --from=tmp --chown=supertokens /usr/lib/supertokens /usr/lib/supertokens
COPY --from=tmp --chown=supertokens /usr/bin/supertokens /usr/bin/supertokens
COPY ./supertokens-postgresql-plugin/.github/helpers/docker/docker-entrypoint.sh /usr/local/bin/
RUN echo "$(md5sum /usr/lib/supertokens/config.yaml | awk '{ print $1 }')" >> /CONFIG_HASH
RUN ln -s usr/local/bin/docker-entrypoint.sh /entrypoint.sh # backwards compat
EXPOSE 3567
USER "supertokens"
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["supertokens", "start"]