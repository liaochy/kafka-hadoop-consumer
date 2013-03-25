# included in all the job scripts with source command
# should not be executable directly
# also should not be passed any arguments, since we need original $*

# resolve links - $0 may be a softlink

this="${BASH_SOURCE-$0}"
common_bin=$(cd -P -- "$(dirname -- "$this")" && pwd -P)
script="$(basename -- "$this")"
this="$common_bin/$script"

# convert relative path to absolute path
config_bin=`dirname "$this"`
script=`basename "$this"`
config_bin=`cd "$config_bin"; pwd`
this="$config_bin/$script"

# the root of the Hadoop installation
export JOB_PREFIX=`dirname "$this"`/..

DEFAULT_CONF_DIR="conf"
DEFAULT_LOG_DIR="log"
DEFAULT_PID_DIR="pids"

JOB_CONF_DIR="${JOB_CONF_DIR:-$JOB_PREFIX/$DEFAULT_CONF_DIR}"
JOB_LOG_DIR="${JOB_LOG_DIR:-$JOB_PREFIX/$DEFAULT_LOG_DIR}"
JOB_PID_DIR="${JOB_PID_DIR:-$JOB_PREFIX/$DEFAULT_PID_DIR}"

if [ -f "${JOB_CONF_DIR}/job-env.sh" ]; then
  . "${JOB_CONF_DIR}/job-env.sh"
fi

export JOB_HOME=${JOB_PREFIX}
export JOB_CONF_DIR=${JOB_CONF_DIR}
export JOB_LOG_DIR=${JOB_LOG_DIR}
export JOB_PID_DIR=${JOB_PID_DIR}