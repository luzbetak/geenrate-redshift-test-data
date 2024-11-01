#!/bin/bash

# Exit on error, undefined variables, and pipe failures
set -euo pipefail

# Script configuration
readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly LOG_FILE="/var/log/spark-runner-$(date +%Y%m%d-%H%M%S).log"
readonly SPARK_DIR="/root/40-apache-spark"
readonly S3_BUCKET="s3a://luzbetak"

# Default configurations
readonly DEFAULT_DRIVER_MEMORY="4G"
readonly DEFAULT_EXECUTOR_MEMORY="4G"
readonly DEFAULT_MASTER="local[2]"
readonly HADOOP_PACKAGE="org.apache.hadoop:hadoop-aws:2.7.7"

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

# Display menu
show_menu() {
    clear
    echo "==============================================="
    echo "           Apache Spark Runner Menu            "
    echo "==============================================="
    echo "1. Run WordCount (Local Mode)"
    echo "2. Run Data Generator (Local Mode)"
    echo "3. Run WordCount (Cluster Mode)"
    echo "4. Run Data Generator (Cluster Mode)"
    echo "5. Clean and Restart Hadoop Services"
    echo "0. Exit"
    echo "==============================================="
}

# Clean and restart Hadoop services
restart_services() {
    log "Stopping Hadoop services..."
    stop-all.sh
    sleep 5
    
    log "Starting Hadoop services..."
    start-all.sh
    sleep 5
    
    log "Services restarted successfully"
}

# Run WordCount job
run_wordcount() {
    local mode=$1
    local master=$2
    local driver_mem=$3
    local executor_mem=$4
    
    log "Running WordCount in $mode mode..."
    
    spark-submit \
        --packages "${HADOOP_PACKAGE}" \
        --master "${master}" \
        --driver-memory "${driver_mem}" \
        --executor-memory "${executor_mem}" \
        "${SPARK_DIR}/wordcount.py" \
        "${S3_BUCKET}/input/alice29.txt" \
        "${S3_BUCKET}/output-$(date +%Y%m%d-%H%M%S)/"
}

# Run Data Generator
run_data_generator() {
    local mode=$1
    local master=$2
    local driver_mem=$3
    local executor_mem=$4
    local slice_start=$5
    local slice_end=$6
    
    log "Running Data Generator in $mode mode for slices $slice_start to $slice_end..."
    
    for i in $(seq "$slice_start" "$slice_end"); do
        n=$(printf "%03d" "$i")
        log "Processing slice: $n"
        
        nohup spark-submit \
            --packages "${HADOOP_PACKAGE}" \
            --master "${master}" \
            --driver-memory "${driver_mem}" \
            --executor-memory "${executor_mem}" \
            ${mode == "cluster" && "--num-executors 4"} \
            ${mode == "cluster" && "--executor-cores 4"} \
            "${SPARK_DIR}/21-generate-data.py" \
            "$i" \
            "100000001" \
            "${S3_BUCKET}/table_001/slice-$n" &
        
        sleep 10
    done
}

# Get cluster configuration
get_cluster_config() {
    local mode=$1
    local config
    
    case $mode in
        "local")
            config=(
                "local[2]"        # master
                "4G"              # driver memory
                "4G"              # executor memory
            )
            ;;
        "cluster")
            config=(
                "spark://172.31.9.145:7077"  # master
                "16G"                        # driver memory
                "16G"                        # executor memory
            )
            ;;
        *)
            error "Invalid mode: $mode"
            ;;
    esac
    
    echo "${config[@]}"
}

# Main execution loop
main() {
    mkdir -p "$(dirname "${LOG_FILE}")"
    
    while true; do
        show_menu
        read -r -p "Select an option (0-5): " choice
        
        case $choice in
            0)
                log "Exiting..."
                exit 0
                ;;
            1)
                read -r -p "Clean logs and restart services? (y/n): " restart
                [[ $restart == "y" ]] && restart_services
                
                config=($(get_cluster_config "local"))
                run_wordcount "local" "${config[@]}"
                ;;
            2)
                read -r -p "Enter start slice number: " slice_start
                read -r -p "Enter end slice number: " slice_end
                read -r -p "Clean logs and restart services? (y/n): " restart
                [[ $restart == "y" ]] && restart_services
                
                config=($(get_cluster_config "local"))
                run_data_generator "local" "${config[@]}" "$slice_start" "$slice_end"
                ;;
            3)
                read -r -p "Clean logs and restart services? (y/n): " restart
                [[ $restart == "y" ]] && restart_services
                
                config=($(get_cluster_config "cluster"))
                run_wordcount "cluster" "${config[@]}"
                ;;
            4)
                read -r -p "Enter start slice number: " slice_start
                read -r -p "Enter end slice number: " slice_end
                read -r -p "Clean logs and restart services? (y/n): " restart
                [[ $restart == "y" ]] && restart_services
                
                config=($(get_cluster_config "cluster"))
                run_data_generator "cluster" "${config[@]}" "$slice_start" "$slice_end"
                ;;
            5)
                restart_services
                ;;
            *)
                error "Invalid option"
                ;;
        esac
        
        echo
        read -r -p "Press Enter to continue..."
    done
}

# Run main function
main "$@"
