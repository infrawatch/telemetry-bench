# --- build smart gateway ---
FROM centos:7 AS builder
ENV GOPATH=/go
ENV D=/go/src/github.com/redhat-nfvpe/telemetry-bench

WORKDIR $D
COPY . $D/

RUN yum install epel-release -y && \
        yum update-minimal -y --setopt=tsflags=nodocs --security --sec-severity=Important --sec-severity=Critical && \
        yum install qpid-proton-c-devel git golang --setopt=tsflags=nodocs -y && \
        yum clean all && \
        go get -u github.com/golang/dep/... && \
        /go/bin/dep ensure -v -vendor-only && \
        go build -o telemetry-bench cmd/telemetry-bench.go && \
        mv telemetry-bench /tmp/

# --- end build, create smart gateway layer ---
FROM centos:7

LABEL io.k8s.display-name="Telemetry Data Generation Tool" \
      io.k8s.description="A tool for generating load against the service assurance framework" \
      maintainer="Leif Madsen <leif@redhat.com>"

RUN yum install epel-release -y && \
        yum update-minimal -y --setopt=tsflags=nodocs --security --sec-severity=Important --sec-severity=Critical && \
        yum install qpid-proton-c --setopt=tsflags=nodocs -y && \
        yum clean all && \
        rm -rf /var/cache/yum

COPY --from=builder /tmp/telemetry-bench /

ENTRYPOINT ["/telemetry-bench"]
