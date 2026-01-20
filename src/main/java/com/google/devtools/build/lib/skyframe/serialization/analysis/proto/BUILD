load("@com_google_protobuf//bazel:java_proto_library.bzl", "java_proto_library")
load("@com_google_protobuf//bazel:proto_library.bzl", "proto_library")
load("@grpc-java//:java_grpc_library.bzl", "java_grpc_library")
load("//tools/build_rules:utilities.bzl", "java_library_srcs")

package(
    default_applicable_licenses = ["//:license"],
    default_visibility = ["//src:__subpackages__"],
)

proto_library(
    name = "analysis_cache_service_metadata_status_proto",
    srcs = ["analysis_cache_service_metadata_status.proto"],
)

java_proto_library(
    name = "analysis_cache_service_metadata_status_java_proto",
    deps = [":analysis_cache_service_metadata_status_proto"],
)

proto_library(
    name = "analysis_cache_service_proto",
    srcs = ["analysis_cache_service.proto"],
    deps = [":analysis_cache_service_metadata_status_proto"],
)

java_proto_library(
    name = "analysis_cache_service_java_proto",
    deps = [":analysis_cache_service_proto"],
)

java_grpc_library(
    name = "analysis_cache_service_java_grpc",
    srcs = [":analysis_cache_service_proto"],
    deps = [":analysis_cache_service_java_proto"],
)

java_library_srcs(
    name = "analysis_cache_service_java_proto_srcs",
    deps = [":analysis_cache_service_java_proto"],
)

java_library_srcs(
    name = "analysis_cache_service_metadata_status_java_proto_srcs",
    deps = [":analysis_cache_service_metadata_status_java_proto"],
)

filegroup(
    name = "dist_jars",
    srcs = [
        ":libanalysis_cache_service_java_grpc-src.jar",
        ":analysis_cache_service_java_proto_srcs",
        ":analysis_cache_service_metadata_status_java_proto_srcs",
    ],
)

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
    visibility = ["//src:__subpackages__"],
)
