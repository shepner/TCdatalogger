"""Scheduler service for managing endpoint processing.

This module provides scheduling functionality for endpoint processors:
- Loads endpoint configurations
- Schedules endpoint processing at specified intervals
- Manages the scheduler lifecycle
- Supports manual and automated execution
- Ensures process isolation between endpoints
"""

import json
import logging
import time
from datetime import datetime
from multiprocessing import Process, Queue, Manager
from typing import Dict, Optional, List, Any
from queue import Empty
import os

import isodate
import schedule

from app.services.torncity.registry import EndpointRegistry


class SchedulerError(Exception):
    """Base exception for scheduler errors."""
    pass


class ConfigurationError(SchedulerError):
    """Raised when there are configuration issues."""
    pass


class ValidationError(SchedulerError):
    """Raised when configuration validation fails."""
    pass


class ProcessingError(SchedulerError):
    """Raised when endpoint processing fails."""
    pass


class EndpointScheduler:
    """Service for scheduling endpoint processing.
    
    This class:
    - Loads endpoint configurations
    - Creates and schedules processor instances
    - Manages the scheduler lifecycle
    - Supports manual and automated execution
    - Ensures process isolation between endpoints
    """
    
    def __init__(self, config: Dict, registry: EndpointRegistry):
        """Initialize the scheduler.
        
        Args:
            config: Application configuration
            registry: Endpoint processor registry
        """
        self.config = self._validate_config(config)
        self.registry = registry
        self._load_api_config()
        
        # Extract app config from main config
        self.app_config = self.config.get('app_config', {})
        
        # Setup multiprocessing resources
        self.manager = Manager()
        self.processes: Dict[str, Process] = {}
        self.status: Dict[str, Dict] = self.manager.dict()
        self.result_queue: Queue = Queue()
        
        # Initialize correlation ID counter
        self._correlation_counter = 0
        
    def _validate_config(self, config: Dict) -> Dict:
        """Validate scheduler configuration.
        
        Args:
            config: Configuration to validate
            
        Returns:
            Dict: Validated configuration
            
        Raises:
            ValidationError: If configuration is invalid
        """
        required_fields = [
            'tc_api_config_file',
            'tc_api_key_file',
            'gcp_credentials_file'
        ]
        
        for field in required_fields:
            if field not in config:
                raise ValidationError(f"Missing required config field: {field}")
        
        return config
        
    def _load_api_config(self) -> None:
        """Load the API endpoint configuration.
        
        Raises:
            ConfigurationError: If configuration cannot be loaded
        """
        try:
            with open(self.config["tc_api_config_file"], 'r') as f:
                self.api_config = json.load(f)
            self._validate_api_config(self.api_config)
        except Exception as e:
            raise ConfigurationError(f"Failed to load API configuration: {str(e)}")
            
    def _validate_api_config(self, config: Dict) -> None:
        """Validate API configuration structure.
        
        Args:
            config: API configuration to validate
            
        Raises:
            ValidationError: If configuration is invalid
        """
        if not isinstance(config, dict) or "endpoints" not in config:
            raise ValidationError("API config must contain 'endpoints' key")
            
        for endpoint in config.get("endpoints", []):
            required = ["name", "url", "table"]
            missing = [f for f in required if f not in endpoint]
            if missing:
                raise ValidationError(f"Endpoint missing required fields: {missing}")

    def _get_correlation_id(self) -> str:
        """Generate a unique correlation ID for request tracking."""
        self._correlation_counter += 1
        return f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}-{self._correlation_counter}"

    def _run_isolated_process(self, endpoint_config: Dict, steps: Optional[List[str]] = None,
                            correlation_id: str = None) -> None:
        """Run endpoint processing in an isolated process.
        
        Args:
            endpoint_config: Configuration for the endpoint
            steps: Optional list of steps to execute
            correlation_id: Request correlation ID
        """
        try:
            # Update process status
            self.status[endpoint_config["name"]] = {
                "status": "running",
                "start_time": datetime.utcnow().isoformat(),
                "correlation_id": correlation_id
            }
            
            # Get the processor class and create instance within this process
            processor_class = self.registry.get_processor(endpoint_config["name"])
            processor = processor_class(self.config, endpoint_config)
            
            result = {
                "endpoint": endpoint_config["name"],
                "correlation_id": correlation_id,
                "success": False,
                "error": None,
                "steps_completed": []
            }
            
            if steps is None:
                # Execute full processing
                result["success"] = processor.process()
            else:
                # Execute specific steps
                for step in steps:
                    if not hasattr(processor, step):
                        raise ProcessingError(f"Step '{step}' not found in processor")
                    
                    step_func = getattr(processor, step)
                    if step_func():
                        result["steps_completed"].append(step)
                    else:
                        result["error"] = f"Step '{step}' failed"
                        break
                
                result["success"] = len(result["steps_completed"]) == len(steps)
            
            # Update final status
            self.status[endpoint_config["name"]].update({
                "status": "completed",
                "end_time": datetime.utcnow().isoformat(),
                "success": result["success"]
            })
            
            # Put result in queue
            self.result_queue.put(result)
            
        except Exception as e:
            error_msg = str(e)
            logging.error(f"Process failed: {error_msg}", extra={
                "correlation_id": correlation_id,
                "endpoint": endpoint_config["name"]
            })
            
            # Update error status
            self.status[endpoint_config["name"]].update({
                "status": "failed",
                "end_time": datetime.utcnow().isoformat(),
                "error": error_msg
            })
            
            # Put error result in queue
            self.result_queue.put({
                "endpoint": endpoint_config["name"],
                "correlation_id": correlation_id,
                "success": False,
                "error": error_msg
            })

    def process_endpoint(self, endpoint_config: Dict, steps: Optional[List[str]] = None) -> str:
        """Process a single endpoint, optionally executing only specific steps.
        
        Args:
            endpoint_config: Configuration for the endpoint
            steps: Optional list of steps to execute (e.g., ['fetch', 'transform', 'validate', 'store'])
                  If None, executes all steps
        
        Returns:
            str: Correlation ID for tracking the request
            
        Raises:
            ProcessingError: If processing fails
        """
        correlation_id = self._get_correlation_id()
        
        try:
            # Cancel existing process if running
            if endpoint_config["name"] in self.processes:
                old_process = self.processes[endpoint_config["name"]]
                if old_process.is_alive():
                    old_process.terminate()
                    old_process.join()
            
            # Start new process
            process = Process(
                target=self._run_isolated_process,
                args=(endpoint_config, steps, correlation_id)
            )
            process.start()
            
            # Store process reference
            self.processes[endpoint_config["name"]] = process
            
            return correlation_id
            
        except Exception as e:
            raise ProcessingError(f"Failed to start process for {endpoint_config['name']}: {str(e)}")

    def schedule_endpoint(self, endpoint_config: Dict) -> None:
        """Schedule a single endpoint for processing.
        
        Args:
            endpoint_config: Configuration for the endpoint
            
        Raises:
            SchedulerError: If endpoint cannot be scheduled
        """
        try:
            # Get frequency from config
            frequency = endpoint_config.get("frequency", "PT15M")
            duration = isodate.parse_duration(frequency)
            minutes = int(duration.total_seconds() / 60)
            
            # Create a copy of the configuration to avoid pickling the entire scheduler
            endpoint_data = {
                "config": self.config.copy(),
                "endpoint_config": endpoint_config.copy()
            }
            
            def schedule_wrapper():
                """Wrapper to handle processor scheduling."""
                try:
                    # Get the processor class directly in this process
                    processor_class = self.registry.get_processor(endpoint_data["endpoint_config"]["name"])
                    processor = processor_class(endpoint_data["config"], endpoint_data["endpoint_config"])
                    processor.process()
                except Exception as e:
                    logging.error(f"Error scheduling endpoint {endpoint_data['endpoint_config']['name']}: {str(e)}")
            
            # Schedule the job
            schedule.every(minutes).minutes.do(schedule_wrapper)
            logging.info(
                f"Scheduled {endpoint_config['name']} to run every {minutes} minutes"
            )
            
        except Exception as e:
            raise SchedulerError(f"Failed to schedule endpoint {endpoint_config['name']}: {str(e)}")

    def schedule_all_endpoints(self) -> None:
        """Schedule all configured endpoints.
        
        Raises:
            SchedulerError: If endpoints cannot be scheduled
        """
        try:
            endpoints = self.api_config.get("endpoints", [])
            if not endpoints:
                raise ConfigurationError("No endpoints configured")
            
            for endpoint in endpoints:
                self.schedule_endpoint(endpoint)
                
        except Exception as e:
            raise SchedulerError(f"Failed to schedule endpoints: {str(e)}")

    def run(self) -> None:
        """Run the scheduler.
        
        This method:
        1. Schedules all endpoints
        2. Runs the scheduler loop
        3. Handles scheduler lifecycle
        
        Raises:
            SchedulerError: If scheduler fails
        """
        try:
            # Schedule all endpoints
            self.schedule_all_endpoints()
            
            # Run the scheduler loop
            while True:
                try:
                    schedule.run_pending()
                    
                    # Check for completed processes
                    self._check_process_results()
                    
                    time.sleep(1)
                except Exception as e:
                    logging.error(f"Error in scheduler loop: {str(e)}")
                    # Continue running even if individual jobs fail
                    
        except KeyboardInterrupt:
            logging.info("Scheduler shutting down...")
            self.shutdown()
        except Exception as e:
            raise SchedulerError(f"Scheduler failed: {str(e)}")

    def _check_process_results(self) -> None:
        """Check for and handle completed process results."""
        try:
            while True:
                try:
                    result = self.result_queue.get_nowait()
                    self._handle_process_result(result)
                except Empty:
                    break
        except Exception as e:
            logging.error(f"Error checking process results: {str(e)}")

    def _handle_process_result(self, result: Dict[str, Any]) -> None:
        """Handle a completed process result.
        
        Args:
            result: Process result dictionary
        """
        endpoint = result["endpoint"]
        correlation_id = result["correlation_id"]
        
        logging.info(
            f"Process completed for endpoint {endpoint}",
            extra={
                "correlation_id": correlation_id,
                "success": result["success"],
                "error": result.get("error"),
                "steps_completed": result.get("steps_completed", [])
            }
        )
        
        # Clean up process reference
        if endpoint in self.processes:
            process = self.processes[endpoint]
            if not process.is_alive():
                process.join()
                del self.processes[endpoint]

    def shutdown(self) -> None:
        """Shutdown the scheduler gracefully."""
        # Cancel all scheduled jobs
        schedule.clear()
        
        # Terminate all running processes
        for process in self.processes.values():
            if process.is_alive():
                process.terminate()
                process.join()
        
        # Clear process dictionary
        self.processes.clear()
        
        # Clear status dictionary
        self.status.clear()

    def list_jobs(self) -> Dict[str, str]:
        """List all scheduled jobs.
        
        Returns:
            Dict[str, str]: Mapping of job names to their schedules
        """
        return {
            job.job_func.__name__: str(job.schedule_info)
            for job in schedule.jobs
        }

    def get_endpoint_status(self, endpoint_name: str) -> Dict:
        """Get the status of a specific endpoint.
        
        Args:
            endpoint_name: Name of the endpoint
            
        Returns:
            Dict: Status information including:
                - status: Current status (running/completed/failed)
                - start_time: When the process started
                - end_time: When the process ended (if completed)
                - success: Whether processing was successful (if completed)
                - error: Error message (if failed)
                - correlation_id: Request correlation ID
        """
        return dict(self.status.get(endpoint_name, {}))

    def _create_processor_config(self, endpoint_config: Dict) -> Dict:
        """Create configuration for a processor instance.
        
        Args:
            endpoint_config: Endpoint-specific configuration
            
        Returns:
            Dict: Complete processor configuration
        """
        return {
            'gcp_credentials_file': self.config['gcp_credentials_file'],
            'endpoint': endpoint_config['name'],
            'storage_mode': endpoint_config.get('storage_mode', 'append'),
            'tc_api_key_file': self.config['tc_api_key_file'],
            'endpoint_config': endpoint_config,
            'url': endpoint_config.get('url'),
            'table': endpoint_config.get('table'),
            'frequency': endpoint_config.get('frequency'),
            'api_key': endpoint_config.get('api_key', 'default'),
            'app_config': self.app_config
        } 