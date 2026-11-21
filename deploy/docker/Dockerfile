# syntax=docker/dockerfile:1.7
# Multi-stage build for the KeyShift controller (with embedded dashboard) and
# the lab tools image. Final images are distroless, non-root, and read-only
# friendly.

FROM node:26-alpine AS web
WORKDIR /src/web
COPY web/package.json web/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY web/ ./
RUN npm run build

FROM golang:1.26-alpine AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOTOOLCHAIN=auto
COPY go.mod go.sum ./
RUN go mod download
COPY . .
COPY --from=web /src/web/dist ./web/dist
RUN go build -trimpath -ldflags "-s -w" -o /out/keyshift-controller ./cmd/keyshift-controller && \
    go build -trimpath -ldflags "-s -w" -o /out/keyshift ./cmd/keyshift && \
    go build -trimpath -ldflags "-s -w" -o /out/keyshift-demo ./examples/adaptive-caching && \
    go build -trimpath -ldflags "-s -w" -o /out/product-service ./examples/standalone && \
    mkdir -p /out/data

FROM gcr.io/distroless/static-debian12:nonroot AS controller
COPY --from=build /out/keyshift-controller /keyshift-controller
# Pre-create the policy directory owned by the runtime user; Docker named
# volumes inherit this ownership on first use.
COPY --from=build --chown=65532:65532 /out/data /data
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/keyshift-controller"]

FROM gcr.io/distroless/static-debian12:nonroot AS lab
COPY --from=build /out/keyshift /keyshift
COPY --from=build /out/keyshift-demo /keyshift-demo
COPY --from=build /out/product-service /product-service
USER nonroot:nonroot
ENTRYPOINT ["/keyshift"]
