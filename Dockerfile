FROM openjdk:8-jdk-alpine
MAINTAINER <rajeev_gupta@optum.com>
COPY /build/libs/*.jar /tmp/
COPY /src/main/resources/*.jks /tmp/
USER 1000:1000
#Running the application
ENTRYPOINT /bin/sh -c "java -Djava.security.egd=file:/dev/./urandom $JAVA_OPTS -Dspring.profiles.active=$ENV_PROFILE -Dspring.kafka.streams.applicationId=cdbedpstream-prod-$APP_NAME -jar /tmp/cdb-postgresql-stream-1.0.0.jar"


##TEST
##ENTRYPOINT /bin/sh -c "java -Djava.security.egd=file:/dev/./urandom $JAVA_OPTS -Dspring.profiles.active=$ENV_PROFILE -Dspring.kafka.streams.applicationId=cdbedpstream-testt-$APP_NAME -jar /tmp/cdb-postgresql-stream-1.0.0.jar"
