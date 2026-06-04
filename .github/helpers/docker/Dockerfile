FROM debian:bookworm-slim AS tmp
WORKDIR /build
RUN apt-get update \
 && apt-get install -y --no-install-recommends wget unzip ca-certificates \
 && rm -rf /var/lib/apt/lists/*

RUN ARCH=$(dpkg --print-architecture) && \
    if [ "$ARCH" = "amd64" ]; then \
        wget -O jre.zip https://github.com/supertokens/jre/raw/refs/heads/master/jre-21.0.7-linux.zip; \
    elif [ "$ARCH" = "arm64" ]; then \
        wget -O jre.zip https://github.com/supertokens/jre/raw/refs/heads/master/jre-21.0.7-linux-aarch.zip; \
    else \
        echo "Unsupported architecture: $ARCH" && exit 1; \
    fi
RUN unzip jre.zip
RUN mv jre-21.0.7 jre || mv jre-21.0.7-linux-aarch/jre-21.0.7 jre

COPY ./cli ./cli
COPY ./core ./core
COPY ./agent ./agent
COPY ./plugin-interface ./plugin-interface
COPY ./plugin ./plugin
COPY ./ee ./ee
COPY ./config.yaml ./config.yaml
COPY ./version.yaml ./version.yaml

RUN ./jre/bin/java -classpath "./cli/*" io.supertokens.cli.Main true

FROM debian:bookworm-slim
RUN groupadd supertokens && useradd -m -s /bin/bash -g supertokens supertokens

RUN set -eux \
 && apt-get update \
 && DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        tzdata \
 && rm -rf /var/lib/apt/lists/*

COPY --from=tmp --chown=supertokens /usr/lib/supertokens /usr/lib/supertokens
COPY --from=tmp --chown=supertokens /usr/bin/supertokens /usr/bin/supertokens
COPY ./supertokens-postgresql-plugin/.github/helpers/docker/docker-entrypoint.sh /usr/local/bin/
RUN echo "$(md5sum /usr/lib/supertokens/config.yaml | awk '{ print $1 }')" >> /CONFIG_HASH
RUN ln -s /usr/local/bin/docker-entrypoint.sh /entrypoint.sh  # backwards compat

ENV SUPERTOKENS_HOST="0.0.0.0"
EXPOSE 3567
USER "supertokens"
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["supertokens", "start"]
