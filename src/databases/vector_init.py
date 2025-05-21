import os
import sys
import time
import logging
import subprocess
import socket
from typing import Dict, Any, Optional

# Import qdrant_client for vector database operations
try:
    import qdrant_client
    from qdrant_client.http import models
    from qdrant_client.http.exceptions import UnexpectedResponse
    QDRANT_AVAILABLE = True
except ImportError:
    QDRANT_AVAILABLE = False


def init_vector_database(
    host: str = None,
    port: int = None,
    grpc_port: int = None,
    vector_size: int = 384,  # Default for all-MiniLM-L6-v2 model
    collection_name: str = "legislation_embeddings",
    recreate_collection: bool = False
) -> bool:
    """
    Initialize vector database for the ETL pipeline.
    
    This function:
    1. Checks if Qdrant server is running
    2. Creates the collection if it doesn't exist
    
    Args:
        host: Qdrant host
        port: Qdrant HTTP port
        grpc_port: Qdrant gRPC port
        vector_size: Size of embedding vectors
        collection_name: Name of the collection
        recreate_collection: Whether to recreate the collection if it exists
        
    Returns:
        True if successful, False otherwise
    """
    logger = logging.getLogger(__name__)
    
    # Check if Qdrant client is available
    if not QDRANT_AVAILABLE:
        logger.error("Qdrant client not available. Install with: pip install qdrant-client")
        return False
    
    # Set connection parameters from environment variables if not provided
    host = host or os.environ.get('VECTOR_DB_HOST', 'localhost')
    port = port or int(os.environ.get('VECTOR_DB_PORT', 6333))
    grpc_port = grpc_port or int(os.environ.get('VECTOR_DB_GRPC_PORT', 6334))
    
    logger.info(f"Initializing Qdrant vector database at {host}:{port}")
    
    # Check if Qdrant is running
    if not _is_qdrant_running(host, port):
        logger.info("Qdrant is not running. Attempting to start...")
        if not _start_qdrant():
            logger.error("Failed to start Qdrant")
            return False
        
        # Wait for Qdrant to start
        max_retries = 10
        retry_delay = 2
        
        for attempt in range(max_retries):
            if _is_qdrant_running(host, port):
                logger.info("Qdrant started successfully")
                break
                
            logger.info(f"Waiting for Qdrant to start (attempt {attempt+1}/{max_retries})...")
            time.sleep(retry_delay)
            retry_delay *= 1.5  # Exponential backoff
        else:
            logger.error("Qdrant did not start in time")
            return False
    
    # Initialize client
    try:
        client = qdrant_client.QdrantClient(
            host=host,
            port=port,
            grpc_port=grpc_port,
            timeout=60
        )
        
        # Check if collection exists
        collections = client.get_collections().collections
        collection_names = [c.name for c in collections]
        
        if collection_name in collection_names:
            if recreate_collection:
                logger.info(f"Recreating collection: {collection_name}")
                client.delete_collection(collection_name)
            else:
                logger.info(f"Collection already exists: {collection_name}")
                return True
        
        # Create collection
        logger.info(f"Creating collection: {collection_name}")
        client.create_collection(
            collection_name=collection_name,
            vectors_config=models.VectorParams(
                size=vector_size,
                distance=models.Distance.COSINE
            )
        )
        
        # Create payload indexes for efficient filtering
        client.create_payload_index(
            collection_name=collection_name,
            field_name="legislation_id",
            field_schema=models.PayloadSchemaType.KEYWORD
        )
        
        client.create_payload_index(
            collection_name=collection_name,
            field_name="section_idx",
            field_schema=models.PayloadSchemaType.INTEGER
        )
        
        logger.info(f"Vector database collection initialized: {collection_name}")
        return True
        
    except Exception as e:
        logger.error(f"Error initializing vector database: {str(e)}")
        return False


def _is_qdrant_running(host: str, port: int) -> bool:
    """
    Check if Qdrant server is running.
    
    Args:
        host: Qdrant host
        port: Qdrant HTTP port
        
    Returns:
        True if running, False otherwise
    """
    try:
        # Try to connect to Qdrant
        client = qdrant_client.QdrantClient(
            host=host,
            port=port,
            timeout=5
        )
        
        # Test connection with a simple request
        client.get_collections()
        return True
    except Exception:
        return False


def _is_port_in_use(host: str, port: int) -> bool:
    """
    Check if a port is in use.
    
    Args:
        host: Host address
        port: Port number
        
    Returns:
        True if port is in use, False otherwise
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((host, port))
        s.shutdown(socket.SHUT_RDWR)
        return True
    except:
        return False
    finally:
        s.close()


def _start_qdrant() -> bool:
    """
    Start Qdrant server.
    
    Returns:
        True if successful, False otherwise
    """
    logger = logging.getLogger(__name__)
    
    try:
        # Check if we're in a Docker container
        in_docker = os.path.exists('/.dockerenv')
        
        if in_docker:
            # In Docker, start Qdrant in the background
            qdrant_dir = "/qdrant_data"
            os.makedirs(qdrant_dir, exist_ok=True)
            
            subprocess.Popen([
                "qdrant",
                "--config", "/qdrant_config/config.yaml"
            ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        else:
            # On local machine
            if sys.platform.startswith('linux'):
                subprocess.run(["sudo", "systemctl", "start", "qdrant"], check=True)
            else:
                # For other platforms, assume Qdrant is installed via Docker
                subprocess.run([
                    "docker", "run", "-d",
                    "-p", "6333:6333",
                    "-p", "6334:6334",
                    "-v", "qdrant_data:/qdrant/storage",
                    "--name", "qdrant",
                    "qdrant/qdrant"
                ], check=True)
                
        return True
    except subprocess.SubprocessError as e:
        logger.error(f"Failed to start Qdrant: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Error starting Qdrant: {str(e)}")
        return False


if __name__ == "__main__":
    # Setup basic logging
    logging.basicConfig(level=logging.INFO)
    
    # Initialize vector database with parameters from environment variables
    if init_vector_database():
        print("Vector database initialization successful")
        sys.exit(0)
    else:
        print("Vector database initialization failed")
        sys.exit(1)