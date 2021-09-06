#
# Image stream-application-unum-style-app
#

FROM cc-java-base:latest

COPY target/stream-application-unum-style-app.jar .

RUN echo 'echo "JAVA_OPTIONS=${JAVA_OPTIONS}"; /usr/bin/java ${JAVA_OPTIONS} -jar stream-application-unum-style-app.jar' > entrypoint.sh

ENTRYPOINT ["sh", "entrypoint.sh"]