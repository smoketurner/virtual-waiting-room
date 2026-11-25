# The AWS Emulator the local stack runs against: DynamoDB and SSM. AWS
# publishes no image, only Linux packages, so this installs one.
FROM public.ecr.aws/amazonlinux/amazonlinux:2023-minimal
ARG TARGETARCH
ARG EMULATOR_VERSION=1.0.611
RUN case "$TARGETARCH" in arm64) ARCH=aarch64 ;; *) ARCH=x86_64 ;; esac \
 && curl -fsSLo /tmp/aws-emulator.rpm \
    "https://downloads.aws.com/aws-emulator/packages/aws-emulator/LTS/aws-emulator-${EMULATOR_VERSION}-1-${ARCH}.rpm" \
 && rpm -i /tmp/aws-emulator.rpm && rm /tmp/aws-emulator.rpm \
 && mkdir -p /home/emulator && chown 1000 /home/emulator
USER 1000
ENV HOME=/home/emulator EMULATOR_BIND_ADDRESS=0.0.0.0 EMULATOR_TELEMETRY=off
ENTRYPOINT ["aws-emulator", "--telemetry", "off"]
