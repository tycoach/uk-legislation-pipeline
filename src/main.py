#!/usr/bin/env python3
import os
import sys
import time
import logging
import argparse
import concurrent.futures
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime

# Import utility modules
from utils.config import Config
from utils.logging import setup_logging
from utils.checkpoint import CheckpointManager

# Import database initialization modules
from databases.sql_init import init_sql_database
from databases.vector_init import init_vector_database

# Import ETL components
from extractors.legislation_scraper import LegislationScraper
from text_transformers.cleaner import LegislationCleaner
from text_transformers.embeddings import EmbeddingsGenerator
from loaders.sql_loader import SQLLoader
from loaders.vector_loader import VectorLoader


class ETLPipeline:
    """
    Main ETL pipeline for UK legislation data.
    
    This class orchestrates the entire ETL process:
    1. Extract: Download legislation from legislation.gov.uk
    2. Transform: Clean data and generate embeddings
    3. Load: Store in SQL and vector databases
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the ETL pipeline.
        
        Args:
            config_path: Path to configuration file (optional)
        """
        # Load configuration
        self.config = Config(load_from_env=True)
        if config_path and os.path.exists(config_path):
            self.config.load_from_file(config_path)
            
        # Setup logging
        self.logger = setup_logging(
            level=self.config.get("log_level", "INFO"),
            log_file=self.config.get("log_file")
        )
        
        # Initialize checkpoint manager
        self.checkpoint_manager = CheckpointManager(
            checkpoint_dir=self.config.get("checkpoint_dir", "/data/checkpoints"),
            interval=self.config.get("checkpoint_interval", 50)
        )
        
        # Initialize ETL components
        self._init_components()
        
        self.logger.info("ETL pipeline initialized")
        
    def _init_components(self) -> None:
        """Initialize ETL pipeline components."""
        # Initialize extractor
        self.scraper = LegislationScraper(
            cache_dir=self.config.get("cache_dir", "/data/cache")
        )
        
        # Initialize transformers
        self.cleaner = LegislationCleaner()
        self.embeddings_generator = EmbeddingsGenerator(
            batch_size=self.config.get("batch_size", 64),
            max_seq_length=self.config.get("max_seq_length", 256),
            device=self.config.get("device")
        )
        
        # Initialize loaders
        self.sql_loader = SQLLoader(
            db_type=self.config.get("db_type", "postgresql"),
            host=self.config.get("db_host"),
            port=self.config.get("db_port"),
            dbname=self.config.get("db_name"),
            user=self.config.get("db_user"),
            password=self.config.get("db_password"),
            sqlite_path=self.config.get("sqlite_path")
        )
        
        self.vector_loader = VectorLoader(
            host=self.config.get("vector_db_host"),
            port=self.config.get("vector_db_port"),
            grpc_port=self.config.get("vector_db_grpc_port")
        )
        
    def _init_databases(self) -> bool:
        """
        Initialize databases for the ETL pipeline.
        
        Returns:
            True if successful, False otherwise
        """
        self.logger.info("Initializing databases")
        
        # Initialize SQL database
        sql_success = init_sql_database(
            db_type=self.config.get("db_type", "postgresql"),
            host=self.config.get("db_host"),
            port=self.config.get("db_port"),
            dbname=self.config.get("db_name"),
            user=self.config.get("db_user"),
            password=self.config.get("db_password"),
            sqlite_path=self.config.get("sqlite_path")
        )
        
        if not sql_success:
            self.logger.error("Failed to initialize SQL database")
            return False
            
        # Initialize vector database
        vector_success = init_vector_database(
            host=self.config.get("vector_db_host"),
            port=self.config.get("vector_db_port"),
            grpc_port=self.config.get("vector_db_grpc_port"),
            collection_name=self.config.get("vector_collection", "legislation_embeddings")
        )
        
        if not vector_success:
            self.logger.error("Failed to initialize vector database")
            return False
            
        self.logger.info("Databases initialized successfully")
        return True
        
    def run(self) -> bool:
        """
        Run the ETL pipeline.
        
        Returns:
            True if successful, False otherwise
        """
        start_time = time.time()
        self.logger.info("Starting ETL pipeline")
        
        # Initialize databases
        if not self._init_databases():
            return False
            
        # Get pipeline parameters
        time_period = self.config.get("legislation_time_period", "August/2024")
        category = self.config.get("legislation_category", "planning")
        batch_size = self.config.get("batch_size", 100)
        max_workers = self.config.get("max_workers", 8)
        max_items = self.config.get("max_items")  # None means no limit
        
        self.logger.info(f"Processing legislation for time period: {time_period}, category: {category}")
        self.logger.info(f"Batch size: {batch_size}, Max workers: {max_workers}")
        
        # Get current stage from checkpoint if available
        current_stage = self.checkpoint_manager.state.get("current_stage")
        
        try:
            # EXTRACT phase
            if not current_stage or current_stage == "extract":
                self.checkpoint_manager.update_stage("extract")
                self._run_extract_phase(time_period, category, batch_size, max_items)
            
            # TRANSFORM phase
            if not current_stage or current_stage in ["extract", "transform"]:
                self.checkpoint_manager.update_stage("transform")
                self._run_transform_phase(max_workers)
            
            # LOAD phase
            if not current_stage or current_stage in ["extract", "transform", "load"]:
                self.checkpoint_manager.update_stage("load")
                self._run_load_phase()
            
            # Pipeline complete
            self.checkpoint_manager.update_stage("complete")
            
            end_time = time.time()
            duration = end_time - start_time
            
            self.logger.info(f"ETL pipeline completed successfully in {duration:.2f} seconds")
            self.logger.info(f"Total items processed: {self.checkpoint_manager.get_processed_count()}")
            
            # Update stats
            self.checkpoint_manager.update_stats("total_duration", duration)
            self.checkpoint_manager.update_stats("completed_at", datetime.now().isoformat())
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error in ETL pipeline: {str(e)}", exc_info=True)
            self.checkpoint_manager.record_error(str(e))
            return False
            
    def _run_extract_phase(self, 
                         time_period: str, 
                         category: str, 
                         batch_size: int,
                         max_items: Optional[int]) -> None:
        """
        Run the Extract phase of the ETL pipeline.
        
        Args:
            time_period: Time period for legislation
            category: Category of legislation
            batch_size: Size of batches to process
            max_items: Maximum number of items to process
        """
        self.logger.info("Starting Extract phase")
        
        # Get processed IDs from checkpoint
        processed_ids = set(self.checkpoint_manager.get_processed_ids())
        
        # Create cache directory for raw legislation data
        raw_data_dir = os.path.join(self.config.get("cache_dir", "/data/cache"), "raw")
        os.makedirs(raw_data_dir, exist_ok=True)
        
        # Search for legislation
        search_results = self.scraper.search_legislation(time_period, category)
        
        if max_items:
            search_results = search_results[:max_items]
            
        self.logger.info(f"Found {len(search_results)} legislation items")
        
        # Update stats
        self.checkpoint_manager.update_stats("total_items", len(search_results))
        
        # Process in batches
        for i in range(0, len(search_results), batch_size):
            batch = search_results[i:i+batch_size]
            self.logger.info(f"Processing batch {i//batch_size + 1}/{(len(search_results)-1)//batch_size + 1} ({len(batch)} items)")
            
            # Update batch info in checkpoint
            self.checkpoint_manager.update_batch({
                "batch_index": i//batch_size + 1,
                "batch_size": len(batch),
                "start_index": i,
                "end_index": i + len(batch)
            })
            
            # Fetch legislation content
            for item in batch:
                item_id = item.get('id')
                
                # Skip if already processed
                if item_id in processed_ids:
                    self.logger.debug(f"Skipping already processed item: {item_id}")
                    continue
                
                try:
                    # Fetch content
                    legislation_data = self.scraper.fetch_legislation_content(item)
                    
                    if legislation_data:
                        # Ensure 'id' is set
                        if 'id' not in legislation_data:
                            legislation_data['id'] = legislation_data.get('legislation_id') or item.get('id')  # fallback to scraped id
                        # Save raw data to cache
                        raw_data_path = os.path.join(raw_data_dir, f"{legislation_data['id']}.json")
                        with open(raw_data_path, 'w') as f:
                            import json
                            json.dump(legislation_data, f)
                        
                        # Mark as processed
                        self.checkpoint_manager.mark_processed(legislation_data['id'])
                    
                except Exception as e:
                    self.logger.error(f"Error fetching legislation {item_id}: {str(e)}")
        
        self.logger.info(f"Extract phase completed. Processed {self.checkpoint_manager.get_processed_count()} items")
    
    def _run_transform_phase(self, max_workers: int) -> None:
        """
        Run the Transform phase of the ETL pipeline.
        
        Args:
            max_workers: Maximum number of workers for parallel processing
        """
        self.logger.info("Starting Transform phase")
        
        # Create directories for transformed data
        raw_data_dir = os.path.join(self.config.get("cache_dir", "/data/cache"), "raw")
        clean_data_dir = os.path.join(self.config.get("cache_dir", "/data/cache"), "clean")
        embedded_data_dir = os.path.join(self.config.get("cache_dir", "/data/cache"), "embedded")
        
        os.makedirs(clean_data_dir, exist_ok=True)
        os.makedirs(embedded_data_dir, exist_ok=True)
        
        # Get list of raw data files
        raw_files = [f for f in os.listdir(raw_data_dir) if f.endswith('.json')]
        self.logger.info(f"Found {len(raw_files)} raw legislation files to transform")
        
        # Process files in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Clean data
            self.logger.info("Cleaning legislation data")
            clean_futures = {executor.submit(self._clean_legislation, os.path.join(raw_data_dir, f), clean_data_dir): f for f in raw_files}
            
            for future in concurrent.futures.as_completed(clean_futures):
                file = clean_futures[future]
                try:
                    result = future.result()
                    if result:
                        self.logger.debug(f"Cleaned legislation: {file}")
                except Exception as e:
                    self.logger.error(f"Error cleaning legislation {file}: {str(e)}")
            
            # Generate embeddings
            self.logger.info("Generating embeddings")
            clean_files = [f for f in os.listdir(clean_data_dir) if f.endswith('.json')]
            
            embedding_futures = {executor.submit(self._generate_embeddings, os.path.join(clean_data_dir, f), embedded_data_dir): f for f in clean_files}
            
            for future in concurrent.futures.as_completed(embedding_futures):
                file = embedding_futures[future]
                try:
                    result = future.result()
                    if result:
                        self.logger.debug(f"Generated embeddings for legislation: {file}")
                except Exception as e:
                    self.logger.error(f"Error generating embeddings for {file}: {str(e)}")
        
        self.logger.info("Transform phase completed")
    
    def _clean_legislation(self, raw_file_path: str, output_dir: str) -> bool:
        """
        Clean legislation data and save to output directory.
        
        Args:
            raw_file_path: Path to raw legislation data file
            output_dir: Output directory for cleaned data
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Load raw data
            with open(raw_file_path, 'r') as f:
                import json
                legislation_data = json.load(f)
            
            # Clean data
            cleaned_data = self.cleaner.clean(legislation_data)
            
            # Save cleaned data
            output_path = os.path.join(output_dir, os.path.basename(raw_file_path))
            with open(output_path, 'w') as f:
                json.dump(cleaned_data, f, indent=2)
            
            return True
        except Exception as e:
            self.logger.error(f"Error cleaning legislation {raw_file_path}: {str(e)}")
            return False
    
    def _generate_embeddings(self, clean_file_path: str, output_dir: str) -> bool:
        """
        Generate embeddings for legislation data and save to output directory.
        
        Args:
            clean_file_path: Path to cleaned legislation data file
            output_dir: Output directory for data with embeddings
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Load cleaned data
            with open(clean_file_path, 'r') as f:
                import json
                cleaned_data = json.load(f)
            
            # Generate embeddings
            embedding_chunk_size = self.config.get("embedding_chunk_size", 200)
            data_with_embeddings = self.embeddings_generator.generate_embeddings(
                cleaned_data, 
                chunk_size=embedding_chunk_size
            )
            
            # Save data with embeddings
            output_path = os.path.join(output_dir, os.path.basename(clean_file_path))
            with open(output_path, 'w') as f:
                json.dump(data_with_embeddings, f, indent=2)
            
            return True
        except Exception as e:
            self.logger.error(f"Error generating embeddings for {clean_file_path}: {str(e)}")
            return False
    
    def _run_load_phase(self) -> None:
        """Run the Load phase of the ETL pipeline."""
        self.logger.info("Starting Load phase")
        
        # Get embedded data files
        embedded_data_dir = os.path.join(self.config.get("cache_dir", "/data/cache"), "embedded")
        embedded_files = [f for f in os.listdir(embedded_data_dir) if f.endswith('.json')]
        
        self.logger.info(f"Found {len(embedded_files)} embedded legislation files to load")
        
        # Process each file
        success_count = 0
        for file in embedded_files:
            try:
                # Load data with embeddings
                with open(os.path.join(embedded_data_dir, file), 'r') as f:
                    import json
                    legislation_data = json.load(f)
                
                # Store in SQL database
                sql_success = self.sql_loader.store_legislation(legislation_data)
                
                # Store embeddings in vector database
                vector_success = self.vector_loader.store_embeddings(legislation_data)
                
                if sql_success and vector_success:
                    success_count += 1
                    self.logger.debug(f"Successfully loaded legislation: {file}")
                else:
                    self.logger.warning(f"Partial failure loading legislation: {file} - SQL: {sql_success}, Vector: {vector_success}")
                
            except Exception as e:
                self.logger.error(f"Error loading legislation {file}: {str(e)}")
        
        self.logger.info(f"Load phase completed. Successfully loaded {success_count}/{len(embedded_files)} items")
        
        # Update stats
        self.checkpoint_manager.update_stats("loaded_count", success_count)


def main():
    """Main entry point for the ETL pipeline."""
    parser = argparse.ArgumentParser(description="UK Legislation ETL Pipeline")
    parser.add_argument("--config", help="Path to configuration file")
    args = parser.parse_args()
    
    # Run pipeline
    pipeline = ETLPipeline(config_path=args.config)
    success = pipeline.run()
    
    # Exit with appropriate status code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()