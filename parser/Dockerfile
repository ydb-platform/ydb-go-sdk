FROM openjdk:17-jdk-slim

RUN apt-get update && apt-get install -y \
    curl \
    sed \
    && rm -rf /var/lib/apt/lists/*

ENV ANTLR_VERSION=4.13.2 
ENV COMMIT_HASH=99ceb562485efe2cefe47db042606e38259640c6

ENV ANTLR_VERSION=4.13.2
RUN curl -O https://www.antlr.org/download/antlr-${ANTLR_VERSION}-complete.jar

ENV CLASSPATH=".:/antlr-${ANTLR_VERSION}-complete.jar:$CLASSPATH"
ENV ANTLR_JAR=/antlr-${ANTLR_VERSION}-complete.jar

RUN echo "alias antlr='java -jar /antlr-${ANTLR_VERSION}-complete.jar'" >> ~/.bashrc

WORKDIR /workspace

RUN curl -L -o YQLv1Antlr4.g4 https://raw.githubusercontent.com/ydb-platform/ydb/${COMMIT_HASH}/yql/essentials/sql/v1/SQLv1Antlr4.g.in

RUN sed -i "s/grammar SQLv1Antlr4/grammar YQLv1Antlr4/g; s/@GRAMMAR_STRING_CORE_SINGLE@/~('\\\'\' | '\\\\\\\\') | ('\\\\\\\\' .)/g; s/@GRAMMAR_STRING_CORE_DOUBLE@/~('\\\"\' | '\\\\\\\\') | ('\\\\\\\\' .)/g; s/@GRAMMAR_MULTILINE_COMMENT_CORE@/./g" YQLv1Antlr4.g4

CMD ["bash"]