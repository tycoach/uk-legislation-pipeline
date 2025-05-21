import os
import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime


class Config:
    """
    Configuration management for the ETL pipeline.
    
    Handles:
    - Loading configuration from environment variables
    - Providing defaults for missing values
    - Validation of configuration values
    """
    
    # Default configuration values
    DEFAULT_CONFIG = {
        # Database settings
        "db_type": "postgresql",
        "db_host": "localhost",
        "db_port": 5432,
        "db_name": "legislation_db",
        "db_user": "etl_user",
        "db_password": "etl_password",
        
        # Vector database settings
        "vector_db_host": "localhost",
        "vector_db_port": 6333,
        "vector_db_grpc_port": 6334,
        
        # ETL pipeline parameters
        "legislation_time_period": "August/2024",
        "legislation_category": "planning",
        
        # Performance settings
        "batch_size": 100,
        "max_workers": 8,
        "checkpoint_interval": 50,
        
        # Paths
        "cache_dir": "/data/cache",
        "checkpoint_dir": "/data/checkpoints",
        
        # Embedding settings
        "embedding_model": "sentence-transformers/all-MiniLM-L6-v2",
        "embedding_chunk_size": 200,
        
        # Logging settings
        "log_level": "INFO",
        "log_file": "/data/logs/etl.log",
        
        # Search settings
        "search_results_limit": 4
    }
    
    def __init__(self, load_from_env: bool = True):
        """
        Initialize configuration manager.
        
        Args:
            load_from_env: Whether to load configuration from environment variables
        """
        self.logger = logging.getLogger(__name__)
        
        # Start with default configuration
        self.config = self.DEFAULT_CONFIG.copy()
        
        # Load from environment variables if enabled
        if load_from_env:
            self._load_from_env()
            
        # Validate configuration
        self._validate()
        
        self.logger.debug(f"Configuration loaded: {json.dumps(self.config, indent=2)}")

    def _load_from_env(self) -> None:
        """
        Load configuration from environment variables.
        
        Environment variables should follow the pattern:
        - DB_HOST for db_host
        - VECTOR_DB_PORT for vector_db_port
        etc.
        """
        # Map of environment variable names to config keys
        env_mapping = {
            # Database settings
            "DB_TYPE": "db_type",
            "DB_HOST": "db_host",
            "DB_PORT": "db_port",
            "DB_NAME": "db_name",
            "DB_USER": "db_user",
            "DB_PASSWORD": "db_password",
            "SQLITE_PATH": "sqlite_path",
            
            # Vector database settings
            "VECTOR_DB_HOST": "vector_db_host",
            "VECTOR_DB_PORT": "vector_db_port",
            "VECTOR_DB_GRPC_PORT": "vector_db_grpc_port",
            
            # ETL pipeline parameters
            "LEGISLATION_TIME_PERIOD": "legislation_time_period",
            "LEGISLATION_CATEGORY": "legislation_category",
            
            # Performance settings
            "BATCH_SIZE": "batch_size",
            "MAX_WORKERS": "max_workers",
            "CHECKPOINT_INTERVAL": "checkpoint_interval",
            
            # Paths
            "CACHE_DIR": "cache_dir",
            "CHECKPOINT_DIR": "checkpoint_dir",
            
            # Embedding settings
            "EMBEDDING_MODEL": "embedding_model",
            "EMBEDDING_CHUNK_SIZE": "embedding_chunk_size",
            
            # Logging settings
            "LOG_LEVEL": "log_level",
            "LOG_FILE": "log_file",
            
            # Search settings
            "SEARCH_RESULTS_LIMIT": "search_results_limit"
        }
        
        # Load values from environment variables
        for env_var, config_key in env_mapping.items():
            value = os.environ.get(env_var)
            if value is not None:
                # Convert to appropriate type
                if config_key in self.config and isinstance(self.config[config_key], int):
                    try:
                        value = int(value)
                    except ValueError:
                        self.logger.warning(f"Invalid integer value for {env_var}: {value}")
                        continue
                elif config_key in self.config and isinstance(self.config[config_key], float):
                    try:
                        value = float(value)
                    except ValueError:
                        self.logger.warning(f"Invalid float value for {env_var}: {value}")
                        continue
                elif config_key in self.config and isinstance(self.config[config_key], bool):
                    value = value.lower() in ('true', 'yes', '1', 'y')
                
                # Update config
                self.config[config_key] = value
                self.logger.debug(f"Loaded {config_key}={value} from environment variable {env_var}")

    def _validate(self) -> None:
        """
        Validate configuration values.
        """
        # Validate time period format
        time_period = self.config.get("legislation_time_period")
        if time_period:
            try:
                month, year = time_period.split('/')
                month_num = datetime.strptime(month, '%B').month
                year_num = int(year)
            except (ValueError, AttributeError) as e:
                self.logger.warning(f"Invalid time period format: {time_period}. Expected format: 'Month/Year'")
        
        # Validate positive integers
        for key in ["batch_size", "max_workers", "checkpoint_interval", "db_port", "vector_db_port"]:
            if self.config.get(key) is not None and (not isinstance(self.config[key], int) or self.config[key] <= 0):
                self.logger.warning(f"Invalid value for {key}: {self.config[key]}. Expected positive integer.")
                # Set to default
                self.config[key] = self.DEFAULT_CONFIG[key]
        
        # Validate directories
        for key in ["cache_dir", "checkpoint_dir"]:
            directory = self.config.get(key)
            if directory:
                os.makedirs(directory, exist_ok=True)

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value.
        
        Args:
            key: Configuration key
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        return self.config.get(key, default)
    
    def set(self, key: str, value: Any) -> None:
        """
        Set a configuration value.
        
        Args:
            key: Configuration key
            value: Configuration value
        """
        self.config[key] = value
    
    def as_dict(self) -> Dict[str, Any]:
        """
        Get the entire configuration as a dictionary.
        
        Returns:
            Configuration dictionary
        """
        return self.config.copy()
    
    def save_to_file(self, filepath: str) -> bool:
        """
        Save configuration to a JSON file.
        
        Args:
            filepath: Path to save the configuration file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with open(filepath, 'w') as f:
                json.dump(self.config, f, indent=2)
            return True
        except Exception as e:
            self.logger.error(f"Error saving configuration to {filepath}: {str(e)}")
            return False
    
    def load_from_file(self, filepath: str) -> bool:
        """
        Load configuration from a JSON file.
        
        Args:
            filepath: Path to the configuration file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with open(filepath, 'r') as f:
                loaded_config = json.load(f)
            
            # Update config
            self.config.update(loaded_config)
            
            # Validate configuration
            self._validate()
            
            return True
        except Exception as e:
            self.logger.error(f"Error loading configuration from {filepath}: {str(e)}")
            return False