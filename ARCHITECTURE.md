# TCdatalogger Architecture

This document outlines the architectural decisions, coding standards, and program structure directives for the TCdatalogger project.

## Core Principles

1. **Modularity First**: Each component should have a single responsibility and be independently testable.
2. **Configuration Over Code**: Business logic and configuration should be separated.
3. **Fail Fast**: Validate inputs early and provide clear error messages.
4. **Observability**: Comprehensive logging and monitoring at all levels.
5. **Security First**: Implement security best practices at every layer.
6. **Generic Implementation**: 
   - Prefer reusable, configurable solutions
   - Use interfaces for common patterns
   - Extract shared functionality
   - Make assumptions explicit
   - Support multiple integration patterns
   - Allow custom implementations
   - Use standard protocols
   - Make endpoints configurable

## Scheduler Architecture

1. **Central Control**
   - Scheduler acts as the central orchestrator
   - Manages execution timing of all components
   - Maintains process isolation between endpoints
   - Supports both automated and manual execution

2. **Process Isolation**
   - Each endpoint processor runs independently
   - Long-running processes don't block other endpoints
   - Separate error handling per processor
   - Independent resource management
   - Example implementation:
     ```python
     class EndpointScheduler:
         def process_endpoint(self, endpoint_config: dict) -> None:
             """Process a single endpoint asynchronously."""
             processor = self.create_processor(endpoint_config)
             # Run in separate process/thread
             async_process = AsyncProcessor(processor)
             async_process.execute()
     ```

3. **Manual Testing Support**
   - Each endpoint can be tested independently
   - Support for testing individual processing steps:
     - API data fetching
     - Data transformation
     - Schema validation
     - Storage operations
   - Example test structure:
     ```python
     class TestEndpointProcessor:
         def test_fetch_data(self):
             """Test only the data fetching step."""
             processor = CrimesProcessor(config)
             data = processor.fetch_data()
             assert data is not None

         def test_transform(self):
             """Test only data transformation."""
             processor = CrimesProcessor(config)
             transformed = processor.transform_data(sample_data)
             assert transformed.shape[0] > 0
     ```

4. **Execution Flexibility**
   - Support for different execution modes:
     - Scheduled automatic execution
     - Manual single endpoint processing
     - Manual step-by-step processing
     - Batch processing of selected endpoints
   - Configuration-driven execution
   - Runtime parameter override support

## Directory Structure

```
TCdatalogger/
├── app/                  # Main application code
│   ├── core/             # Core application logic
│   │   ├── common.py     # Shared utilities
│   │   └── main.py       # Application entry point
│   ├── services/         # External service integrations
│   │   ├── google/       # Google Cloud services
│   │   │   └── client.py # BigQuery client
│   │   └── torncity/     # Torn City API
│   │       └── client.py # API client
│   └── models/           # Data models and schemas
├── config/               # Configuration (not in repo)
├── tests/                # Test suite
└── docker/               # Docker-related files
```

## Code Organization Standards

1. **Service Integration**
   - Each API endpoint has its own dedicated processor class in `services/torncity/endpoints/`
   - Base processor class provides common functionality (auth, rate limiting, etc.)
   - Each endpoint processor implements its own data transformation logic
   - Service clients (e.g., `client.py`) must be generic and minimal:
     - Focus on core connectivity and transport
     - No business logic or specialized processing
     - No data transformation or type handling
     - All specialization occurs in endpoint processors
   - Example structure:
     ```
     services/torncity/
     ├── client.py          # Base API client (generic transport only)
     ├── base.py            # Base endpoint processor
     └── endpoints/         # Endpoint-specific processors
         ├── crimes.py      # Crimes endpoint processor
         ├── members.py     # Members endpoint processor
         ├── items.py       # Items endpoint processor
         ├── currency.py    # Currency endpoint processor
         └── basic.py       # Basic endpoint processor
     ```

2. **Configuration Management**
   - All configuration should be externalized
   - Use environment variables for deployment-specific settings
   - Sensitive data must never be committed to the repository

3. **Error Handling**
   - Custom exceptions for different error categories
   - Comprehensive error messages with context
   - Proper error propagation to the appropriate layer

4. **Logging Standards**
   - Use structured logging
   - Include correlation IDs for request tracking
   - Log appropriate level (DEBUG, INFO, WARNING, ERROR)

## Data Flow

1. **Input Processing**
   - Each endpoint processor validates its own configuration
   - Endpoint-specific validation rules and requirements
   - Independent credential management per endpoint

2. **Data Collection**
   - Each endpoint processor implements its own data fetching logic
   - Endpoint-specific rate limiting and retry strategies
   - Custom error handling per endpoint type

3. **Data Transformation**
   - Each endpoint processor defines its own data transformation rules
   - Endpoint-specific schema validation
   - Custom data normalization logic per endpoint
   - Example:
     ```python
     class CrimesEndpointProcessor(BaseEndpointProcessor):
         def transform_data(self, data: dict) -> pd.DataFrame:
             """Transform crimes data into normalized rows."""
             # Crimes-specific transformation logic
             pass

     class MembersEndpointProcessor(BaseEndpointProcessor):
         def transform_data(self, data: dict) -> pd.DataFrame:
             """Transform members data into normalized rows."""
             # Members-specific transformation logic
             pass
     ```

4. **Data Storage**
   - Each endpoint processor can define custom storage requirements
   - Endpoint-specific table schemas
   - Custom data update strategies (append/replace)
   - BigQuery table management per endpoint

## Data Structure Standards

1. **Nested Data Handling**
   - Nested data structures, such as dictionaries, lists, and arrays, are normalized into separate rows rather than columns
   - Each nested data element becomes a new row
   - Parent data is duplicated across child rows
   - Example structure:
     ```json
     Input JSON:
     {
         "id": 123,
         "name": "Crime A",
         "slots": [
             {"position": 1, "user_id": 456},
             {"position": 2, "user_id": 789}
         ],
         "rewards": {
             "money": 1000,
             "items": [
                 {"id": 1, "quantity": 2},
                 {"id": 2, "quantity": 1}
             ]
         }
     }
     
     Results in multiple rows:
     1. Slots table:
        - Row 1: crime_id=123, name="Crime A", position=1, user_id=456
        - Row 2: crime_id=123, name="Crime A", position=2, user_id=789
     
     2. Reward Items table:
        - Row 1: crime_id=123, name="Crime A", money=1000, item_id=1, quantity=2
        - Row 2: crime_id=123, name="Crime A", money=1000, item_id=2, quantity=1
     ```

2. **Data Normalization Benefits**
   - Maintains proper data normalization
   - Preserves relationships between entities
   - Allows for efficient querying of nested data
   - Eliminates need for complex JSON parsing in queries
   - Supports proper indexing and filtering

3. **Storage Modes**
   - Supported modes:
     - `append`: Add new records to existing data (default)
     - `replace`: Replace all existing data with new data
   - Implementation requirements:
     ```python
     def upload_data(data: pd.DataFrame, table_id: str, mode: str = "append") -> bool:
         """Upload data with specified storage mode."""
         try:
             if mode == "replace":
                 # Use temporary table for atomic replacement
                 temp_table = create_temp_table()
                 upload_to_temp(data, temp_table)
                 replace_table(temp_table, table_id)
             else:
                 # Append directly to target table
                 append_to_table(data, table_id)
             return True
         except Exception as e:
             logging.error(f"Upload failed: {str(e)}")
             return False
     ```
   - Safety considerations:
     - Use temporary tables for replace mode
     - Perform atomic swaps
     - Validate before replacing
     - Keep backup if needed
   - Error handling:
     - Handle atomic operation failures
     - Handle duplicate records
     - Implement retry logic

4. **Timing and Frequency**
   - Uses ISO 8601 duration format for frequencies
   - Format: P[n]Y[n]M[n]DT[n]H[n]M[n]S
   - Examples:
     - PT15M = 15 minutes
     - PT1H = 1 hour
     - P1D = 1 day
     - PT1H30M = 1 hour and 30 minutes

5. **Schema Management**
   - Version all schema changes
   - Document schema modifications
   - Use safe schema evolution practices:
     ```sql
     -- Example of safe schema modification
     BEGIN;
     -- Create backup
     CREATE TABLE users_backup AS SELECT * FROM users;
     
     -- Modify with validation
     ALTER TABLE users 
     ADD COLUMN email_verified BOOLEAN 
     DEFAULT FALSE 
     NOT NULL;
     
     -- Verify modification
     SELECT COUNT(*) FROM users WHERE email_verified IS NULL;
     COMMIT;
     ```
   - Implement schema validation:
     - Required field validation
     - Type compatibility checks
     - Constraint verification
     - Index coverage analysis
   - Track schema versions in metadata
   - Maintain backward compatibility
   - Document breaking changes
   - Include rollback procedures

## Development Guidelines

1. **Code Style**
   - Follow PEP 8 guidelines
   - Use type hints for all functions
   - Document all public interfaces with docstrings
   - Use meaningful variable and function names
   - Keep functions focused and small
   - Document complex logic
   - Follow language-specific conventions

2. **Testing Requirements**
   - Unit tests for all business logic
   - Integration tests for service interactions
   - Mocked external services in tests
   - Document test scenarios
   - Include edge cases
   - Maintain test coverage metrics
   - Support for isolated endpoint testing:
     - Individual endpoint execution
     - Step-by-step process testing
     - Configuration parameter testing
   - Test fixtures for each processing stage:
     - Sample API responses
     - Transformation test cases
     - Schema validation scenarios
   - Support for manual testing:
     - Command-line execution tools
     - Test configuration overrides
     - Diagnostic logging options
   - Performance testing support:
     - Single endpoint benchmarks
     - Multi-endpoint concurrent testing
     - Resource utilization monitoring

3. **Performance Considerations**
   - Batch operations where possible
   - Implement appropriate caching
   - Monitor memory usage
   - Profile critical paths
   - Optimize database queries
   - Use connection pooling
   - Implement rate limiting

4. **Security Practices**
   - Regular dependency updates
   - Secure credential handling
   - Input validation and sanitization
   - Encrypt sensitive data
   - Use HTTPS/TLS for transport
   - Implement proper authentication
   - Monitor API usage
   - Regular security audits
   - Use principle of least privilege
   - Implement role-based access control

## Logging and Monitoring

1. **Logging Standards**
   - Use appropriate log levels (ERROR, WARNING, INFO, DEBUG)
   - Include essential context (timestamp, level, component, process ID)
   - Structure log messages consistently
   - Avoid logging sensitive information
   - Use machine-parseable format
   - Implement log rotation

2. **System Metrics**
   - CPU usage monitoring
   - Memory utilization tracking
   - Disk space monitoring
   - Network I/O metrics
   - Error rates and patterns
   - Request latencies
   - Database performance

3. **Application Metrics**
   - Request rates
   - Error rates
   - Response times
   - Active users/connections
   - Data pipeline metrics
   - Processing time
   - Data quality metrics

## Error Handling

1. **General Principles**
   - Fail fast and explicitly
   - Provide meaningful error messages
   - Include error recovery mechanisms
   - Log errors with stack traces
   - Handle resource cleanup
   - Implement retry logic for transient failures

2. **Exception Handling**
   - Use specific exception types
   - Implement proper exception hierarchies
   - Log all errors with appropriate context
   - Clean up resources in finally blocks
   - Document error conditions in docstrings

3. **Database Error Handling**
   - Implement transaction management
   - Handle deadlocks appropriately
   - Validate input parameters
   - Log database errors
   - Implement retry logic

## Container Guidelines

1. **Docker Best Practices**
   - Use official base images
   - Keep images updated
   - Run as non-root user
   - Implement health checks
   - Use multi-stage builds
   - Minimize layer size
   - Clean up build artifacts

2. **Security Measures**
   - Scan for vulnerabilities
   - Limit container capabilities
   - Use read-only file systems
   - Monitor container resources
   - Regular security updates
   - Proper secret management

## Version Control

1. **Git Best Practices**
   - Use clear, descriptive commit messages
   - Make atomic commits
   - Follow branch naming conventions
   - Regular rebasing to avoid conflicts
   - Don't commit sensitive information
   - Delete branches after merging

2. **Branch Strategy**
   - feature/description
   - bugfix/description
   - hotfix/description
   - release/version
   - Keep main/master branch stable

## Licensing

1. **License Management**
   - Use MIT License for public repositories
   - Include copyright notice
   - Include permission notice
   - Include warranty disclaimer
   - Keep license text unmodified

2. **Third-Party Dependencies**
   - Check compatibility with project license
   - Document all licenses in README
   - Keep track of license obligations
   - Review license changes in updates
   - Include attribution notices

## Deployment Strategy

1. **Container Guidelines**
   - Minimal base image
   - Multi-stage builds
   - Health checks implementation

2. **Monitoring**
   - Application metrics
   - Resource utilization
   - Error rates and patterns

3. **Backup and Recovery**
   - Data backup strategy
   - Recovery procedures
   - Failover handling

## Future Considerations

1. **Scalability**
   - Horizontal scaling preparation
   - Load balancing readiness
   - Database partitioning strategy

2. **Feature Roadmap**
   - Additional API endpoints
   - Enhanced monitoring
   - Automated testing improvements

## Contributing

When contributing to this project:

1. **Branch Strategy**
   - Feature branches from main
   - Pull request requirements
   - Code review guidelines

2. **Documentation**
   - Update relevant documentation
   - Include change rationale
   - Document breaking changes

3. **Quality Assurance**
   - Test coverage requirements
   - Performance impact consideration
   - Security review process 