#################################################################
FROM --platform=linux/amd64 scratch AS binaries
ADD binaries/mediamtx_linux_amd64.tar.gz /linux/amd64

#################################################################
FROM scratch

ARG TARGETPLATFORM
COPY --from=binaries /$TARGETPLATFORM /

ENTRYPOINT [ "/mediamtx" ]