FROM openjdk:17-jdk-slim

RUN apt-get update && apt-get install -y \
    curl \
    sed \
    && rm -rf /var/lib/apt/lists/*

ENV ANTLR_VERSION=4.13.2 
ENV COMMIT_HASH=2876ba42c43c18664efd1fa3db856b7211ea4232

ENV ANTLR_VERSION=4.13.2
RUN curl -O https://www.antlr.org/download/antlr-${ANTLR_VERSION}-complete.jar

ENV CLASSPATH=".:/antlr-${ANTLR_VERSION}-complete.jar:$CLASSPATH"
ENV ANTLR_JAR=/antlr-${ANTLR_VERSION}-complete.jar

RUN echo "alias antlr='java -jar /antlr-${ANTLR_VERSION}-complete.jar'" >> ~/.bashrc

WORKDIR /workspace

RUN curl -L -o SQLv1Antlr4.g.in https://raw.githubusercontent.com/ydb-platform/ydb/${COMMIT_HASH}/yql/essentials/sql/v1/SQLv1Antlr4.g.in

RUN cp SQLv1Antlr4.g.in YQLv1Antlr4.g.in

RUN mv SQLv1Antlr4.g.in SQLv1Antlr4.g4
RUN mv YQLv1Antlr4.g.in YQLv1Antlr4.g4

RUN sed -i "s/grammar SQLv1Antlr4/grammar YQLv1Antlr4/g" YQLv1Antlr4.g4 

#to fix
RUN sed -i "s/@GRAMMAR_STRING_CORE_SINGLE@/~('\\\'\' | '\\\\\\\\') | ('\\\\\\\\' .)/g" YQLv1Antlr4.g4 && \
    sed -i "s/@GRAMMAR_STRING_CORE_DOUBLE@/~('\\\"\' | '\\\\\\\\') | ('\\\\\\\\' .)/g" YQLv1Antlr4.g4 && \
    sed -i "s/@GRAMMAR_MULTILINE_COMMENT_CORE@/./g" YQLv1Antlr4.g4

RUN sed -i "s/@GRAMMAR_STRING_CORE_SINGLE@/~'\\\\\'\' | ('\\\\\'\' '\\\\\'\')/g" SQLv1Antlr4.g4 && \
    sed -i "s/@GRAMMAR_STRING_CORE_DOUBLE@/~'\\\"\' | ('\\\"\' '\\\"\')/g" SQLv1Antlr4.g4 && \
    sed -i "s/@GRAMMAR_MULTILINE_COMMENT_CORE@/MULTILINE_COMMENT | ./g" SQLv1Antlr4.g4

CMD ["bash"]