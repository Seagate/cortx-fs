#!/bin/bash
###############################################################################
# Builds script for all EOS-FS components.
#
# Dev workflow:
#   ./scripts/build.sh bootstrap -- Updates sub-modules and external repos.
#   ./scripts/build.sh config -- Generates out-of-tree cmake build folders
#   ./scripts/build.sh make -j -- Compiles files in them.
#   ./scripts/build.sh install -- Generates and installs RPMs for internal
#                                 components (i.e., does not install NFS Ganesha)
#
###############################################################################
set -e

###############################################################################
# CMD Inteface

eosfs_cmd_usage() {
	echo "usage: $PROG_NAME [-p <ganesha src path>] [-v <version>] [-b <build>] [-k {mero|redis}] [-e {mero|posix}]" 1>&2;
	echo "    -p    Path to NFS Ganesha source src dir, e.g. ~/nfs-gaensha/src" 1>&2;
	echo "    -v    EOS FS Version" 1>&2;
	echo "    -b    EOS FS Build Number" 1>&2;
	echo "    -k    Use \"mero\" or \"redis\" for Key Value Store" 1>&2;
	echo "    -e    Use \"mero\" or \"posix\" for Backend Store" 1>&2;
	exit 1;
}

eosfs_parse_cmd() {
    while getopts ":b:v:p:k:e:" o; do
        case "${o}" in
        b)
            export EOS_FS_BUILD_VERSION="${OPTARG}"
            ;;
        v)
            export EOS_FS_VERSION=${OPTARG}
            ;;
        p)
            export KVSFS_NFS_GANESHA_DIR="$(dirname ${OPTARG})"
            ;;
        k)
            export KVSNS_KVSAL_BACKEND=${OPTARG}
            ;;
        e)
            export KVSNS_EXTSTORE_BACKEND=${OPTARG}
            ;;
        *)
            eosfs_cmd_usage
            ;;
        esac
    done
}

###############################################################################
# Env

eosfs_set_env() {
    export KVSNS_SOURCE_ROOT=$PWD/kvsns
    export KVSFS_SOURCE_ROOT=$PWD/nfs-ganesha

    export EOS_FS_BUILD_ROOT=${EOS_FS_BUILD_ROOT:-/tmp/eos-fs}
    export EOS_FS_VERSION=${EOS_FS_VERSION:-"$(cat $PWD/VERSION)"}
    export EOS_FS_BUILD_VERSION=${EOS_FS_BUILD_VERSION:-"$(git rev-parse --short HEAD)"}

    export KVSNS_KVSAL_BACKEND=${KVSNS_KVSAL_BACKEND:-"mero"}
    export KVSNS_EXTSTORE_BACKEND=${KVSNS_EXTSTORE_BACKEND:-"mero"}

    export KVSFS_NFS_GANESHA_DIR=${KVSFS_NFS_GANESHA_DIR:-$PWD/../nfs-ganesha}
    export KVSFS_NFS_GANESHA_BUILD_DIR=${KVSFS_NFS_GANESHA_BUILD_DIR:-$KVSFS_NFS_GANESHA_DIR/build}
}


eosfs_print_env() {
    eosfs_set_env
    local myenv=(
        KVSNS_SOURCE_ROOT
        KVSFS_SOURCE_ROOT
        EOS_FS_BUILD_ROOT
        EOS_FS_BUILD_VERSION
        KVSNS_KVSAL_BACKEND
        KVSNS_EXTSTORE_BACKEND
        KVSFS_NFS_GANESHA_DIR
        KVSFS_NFS_GANESHA_BUILD_DIR
    )
    for i in ${myenv[@]}; do
        echo "$i=${!i}"
    done
}

###############################################################################
eosfs_bootstrap() {
    echo "Updating local sub-modules"
    git submodule update --init --recursive
    echo "Downloading NFS Ganesha sources"
    if [ ! -d $KVSFS_NFS_GANESHA_DIR ]; then
       git clone --depth 1 --recurse-submodules ssh://git@gitlab.mero.colo.seagate.com:6022/eos/fs/nfs-ganesha.git $KVSFS_NFS_GANESHA_DIR
    fi
}

###############################################################################
eosfs_usage() {
    echo -e "
EOSFS Build script.
Usage:
    env <build environment> $0 <action>

Where action is one of the following:
    env     - Show build environment.
    bootstrap - Fetch recent sub-modules, check local/external build deps.
    config  - Delete old build root, run configure, and build local deps.
    make    - Run make [...] command.
    purge   - Clean up all files generated by build/config steps.
    jenkins - Run CI build.
    install - Build RPMs and install them locally.
    uninstall - Uninstall RPMs from the local system.
    help    - Print usage.

Dev workflow:
    $0 bootstrap -- Update sources if you don't do git-pull manually.
    $0 config -- Initialize the build folders
    $0 make -j -- and build binaries from the sources.
    $0 install -- Install RPMs locally (internal components only).

External sources:
    NFS Ganesha.
    EOS-FS needs NFS Ganesha repo to build KVSFS-FSAL module.
    It also uses a generated config.h file, so that the repo
    needs to to be configured at least.
    Default location: $(dirname $PWD)/nfs-ganesha.
"
}

###############################################################################
_kvsfs_build() {
    echo "KVSFS_BUILD: $@"
    $KVSFS_SOURCE_ROOT/scripts/build.sh "$@"
}

_kvsns_build() {
    echo "KVSNS_BUILD: $@"
    $KVSNS_SOURCE_ROOT/scripts/build.sh "$@"
}

###############################################################################
eosfs_configure_ganesha() {
    rm -fR "$KVSFS_NFS_GANESHA_BUILD_DIR"
    mkdir -p "$KVSFS_NFS_GANESHA_BUILD_DIR"

    export KVSFS_NFS_GANESHA_DIR=$(realpath $KVSFS_NFS_GANESHA_DIR)
    export KVSFS_NFS_GANESHA_BUILD_DIR=$(realpath $KVSFS_NFS_GANESHA_BUILD_DIR)


    cd $KVSFS_NFS_GANESHA_BUILD_DIR
    echo "Configuring NFS Ganesha $KVSFS_NFS_GANESHA_DIR -> $KVSFS_NFS_GANESHA_BUILD_DIR"
    cmake "$KVSFS_NFS_GANESHA_DIR/src"
    cd -
    echo "Finished NFS Ganesha config"
}

eosfs_make_ganesha_rpms() {
    cd $KVSFS_NFS_GANESHA_BUILD_DIR >/dev/null
    echo "Bulding NFS Ganesha RPMS"
    make rpm
    cd - >/dev/null
    echo "Finished building NFS Ganesha RPMs"
}

eosfs_jenkins_build() {
    eosfs_parse_cmd "$@" &&
        eosfs_set_env &&
        eosfs_print_env &&
        eosfs_bootstrap &&
        mkdir -p $EOS_FS_BUILD_ROOT &&
        eosfs_configure_ganesha &&
        _kvsns_build reconf &&
        _kvsns_build make -j all &&
        _kvsfs_build reconf &&
        _kvsfs_build make -j all &&
        _kvsns_build rpms &&
        _kvsfs_build rpms &&
        eosfs_make_ganesha_rpms &&
        _kvsfs_build purge &&
        _kvsns_build purge &&
    echo "OK"
}

###############################################################################
eosfs_configure() {
    eosfs_set_env

    if [ ! -d "$KVSFS_NFS_GANESHA_DIR" ]; then
        echo "NFS Ganesha sources not found in $KVSFS_NFS_GANESHA_DIR."
        echo "Please git-clone it or set differnt location"
        exit 1
    fi

    if [ ! -d "$KVSFS_NFS_GANESHA_BUILD_DIR" ] ; then
        echo "NFS Ganesha build dir not found in $KVSFS_NFS_GANESHA_BUILD_DIR."
        echo "Please build it or set differnt location"
        exit 1
    fi

    mkdir -p $EOS_FS_BUILD_ROOT
    _kvsns_build reconf
    _kvsfs_build reconf
}

###############################################################################
eosfs_make() {
    eosfs_set_env
    _kvsns_build make "$@" && \
        _kvsfs_build make "$@"
}

###############################################################################
eosfs_local_install() {
    eosfs_set_env
    sudo echo "Checking sudo access"
    _kvsns_build install
    _kvsfs_build install
}

eosfs_local_uninstall() {
    eosfs_set_env
    sudo echo "Checking sudo access"
    _kvsfs_build uninstall
    _kvsns_build uninstall
}

###############################################################################
case $1 in
    env)
        eosfs_print_env;;
    bootstrap)
        eosfs_bootstrap;;
    config)
        eosfs_configure;;
    make)
        shift
        eosfs_make "$@" ;;
    purge)
        eosfs_purge;;
    jenkins)
        shift
        eosfs_jenkins_build "$@";;
    install)
        eosfs_local_install;;
    uninstall)
        eosfs_local_uninstall;;
    *)
        eosfs_usage;;
esac

###############################################################################
