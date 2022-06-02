FROM amazoncorretto:18-alpine3.15

RUN apk update && apk upgrade

ENV CQUILL_MIGRATE_CQL_DIR=/cql
VOLUME /cql

COPY ./build/libs/cquill-migrate.jar /cquill-migrate.jar

CMD java -jar /cquill-migrate.jar
