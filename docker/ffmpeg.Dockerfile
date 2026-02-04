#################################################################
FROM --platform=linux/amd64 scratch AS binaries
ADD binaries/mediamtx_linux_amd64.tar.gz /linux/amd64

#################################################################
FROM alpine
RUN apk update
RUN apk upgrade
RUN apk add --no-cache ffmpeg

ARG TARGETPLATFORM
COPY --from=binaries /$TARGETPLATFORM /

ENTRYPOINT [ "/mediamtx" ]
