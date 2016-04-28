FROM laincloud/centos-lain:20160428

RUN yum -y install cmake patch \
    && yum -y clean all \
    && git clone https://github.com/mozilla-services/heka /heka_build/src \
    && cd /heka_build/src \
    && git checkout versions/0.10 \
    && source ./env.sh \
    && go get golang.org/x/net/context \
    && git clone https://github.com/laincloud/hekalain.git externals/hekalain \
    && cp externals/hekalain/fixtures/plugin_loader.cmake cmake \
    && source ./build.sh \
    && cd /heka_build/src/build \
    && make install \
    && mkdir -p /heka_build/heka-lain-0.10 \
    && cp -R /heka_build/src/build/heka/{bin,include,lib,share} /heka_build/heka-lain-0.10 \
    && cd /heka_build \
    && tar -zvcf heka-lain-0.10.tgz heka-lain-0.10
