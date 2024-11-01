#!/usr/bin/env bash

# [Previous code remains the same until deploy_to_server function]

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
    fi  # Added missing fi
    
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

# [Middle code remains the same until main function]

# Main function
main() {
    # Create log directory if it doesn't exist
    mkdir -p "$(dirname "${LOG_FILE}")"
    
    log "Starting deployment process..."
    
    # Analyze git repository if requested
    if [[ "${1:-}" == "--analyze" ]]; then
        analyze_git_repo
        exit 0
    fi  # Added missing fi
    
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
