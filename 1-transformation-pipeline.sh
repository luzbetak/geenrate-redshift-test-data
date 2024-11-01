#!/usr/bin/env bash

# Exit on error, undefined variables, and pipe failures
set -euo pipefail

# Script configuration
readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly LOG_FILE="/var/log/deployment-$(date +%Y%m%d-%H%M%S).log"
readonly SPARK_DIR="40-apache-spark"
readonly SSH_USER="root"
readonly SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=5"

# Define server ranges
readonly SPARK_SERVERS=($(seq 10 16))  # Servers 192.168.1.10-16
readonly APP_SERVERS=($(seq 17 23))    # Servers 192.168.1.17-23

# Logging function
log() {
    local timestamp
    timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[${timestamp}] $*" | tee -a "${LOG_FILE}"
}

# Error handling
error() {
    log "ERROR: $*"
    exit 1
}

# Function to check if a server is reachable
check_server() {
    local server=$1
    ping -c 1 -W 2 "${server}" >/dev/null 2>&1
}

# Function to analyze git repository
analyze_git_repo() {
    log "Analyzing git repository for file sizes..."
    
    git rev-list master | while read -r rev; do
        git ls-tree -lr "$rev" | \
            cut -c54- | \
            sed -r 's/^ +//g;'
    done | \
    sort -u | \
    perl -e '
        while (<>) {
            chomp;
            @stuff=split("\t");
            $sums{$stuff[1]} += $stuff[0];
        }
        print "$sums{$_} $_\n" for (keys %sums);
    ' | \
    sort -rn | \
    head -n 20
}

# Function to deploy files to a server
deploy_to_server() {
    local ip=$1
    local files=$2
    local target_dir="~/${SPARK_DIR}"
    local server="${SSH_USER}@192.168.1.${ip}"
    
    log "Deploying to ${server}..."
    
    # Check if server is reachable
    if ! check_server "192.168.1.${ip}"; then
        log "WARNING: Server ${server} is not reachable, skipping..."
        return 1
    
    # Create directory if it doesn't exist
    ssh ${SSH_OPTS} "${server}" "mkdir -p ${target_dir}" || {
        error "Failed to create directory on ${server}"
    }
    
    # Copy files
    scp ${SSH_OPTS} "${files}" "${server}:${target_dir}" || {
        error "Failed to copy files to ${server}"
    }
    
    log "Successfully deployed to ${server}"
    return 0
}

# Function to deploy to a group of servers
deploy_to_server_group() {
    local -n servers=$1  # nameref to array
    local files=$2
    local success_count=0
    local total_servers=${#servers[@]}
    
    for ip in "${servers[@]}"; do
        if deploy_to_server "${ip}" "${files}"; then
            ((success_count++))
        fi
    done
    
    log "Deployment completed: ${success_count}/${total_servers} servers successful"
    
    # Return success only if all deployments succeeded
    [[ ${success_count} -eq ${total_servers} ]]
}

# Main function
main() {
    # Create log directory if it doesn't exist
    mkdir -p "$(dirname "${LOG_FILE}")"
    
    log "Starting deployment process..."
    
    # Analyze git repository if requested
    if [[ "${1:-}" == "--analyze" ]]; then
        analyze_git_repo
        exit 0
    
    # Deploy 'go' file to Spark servers
    log "Deploying 'go' file to Spark servers..."
    deploy_to_server_group SPARK_SERVERS "${SPARK_DIR}/go" || {
        error "Failed to deploy to all Spark servers"
    }
    
    # Deploy '21*' files to application servers
    log "Deploying '21*' files to application servers..."
    deploy_to_server_group APP_SERVERS "${SPARK_DIR}/21*" || {
        error "Failed to deploy to all application servers"
    }
    
    log "Deployment completed successfully"
}

# Run main function with all arguments
main "$@"
