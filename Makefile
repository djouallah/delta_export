PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

EXT_NAME=delta_export

EXT_CONFIG=${PROJ_DIR}extension_config.cmake

DEFAULT_TEST_EXTENSION_DEPS=json

include extension-ci-tools/makefiles/duckdb_extension.Makefile
