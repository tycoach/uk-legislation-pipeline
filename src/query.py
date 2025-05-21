#!/usr/bin/env python3
import os
import sys
import argparse
import logging
from typing import Dict, Any, List, Optional
import numpy as np

# Import utility modules
from utils.config import Config
from utils.logging import setup_logging

# Import components needed for querying
from text_transformers.embeddings import EmbeddingsGenerator
from loaders.sql_loader import SQLLoader
from loaders.vector_loader import VectorLoader


class LegislationSearchCLI:
    """
    CLI application for searching UK legislation data.
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the search CLI.
        
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
        
        # Initialize components
        self._init_components()
        
    def _init_components(self) -> None:
        """Initialize components needed for searching."""
        # Initialize embeddings generator
        self.embeddings_generator = EmbeddingsGenerator(
            batch_size=1,  # Only need to process one query at a time
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
        
    def search(self, query: str, limit: int = 4) -> List[Dict[str, Any]]:
        """
        Search for legislation related to the query.
        
        Args:
            query: Search query
            limit: Maximum number of results to return
            
        Returns:
            List of search results with relevant information
        """
        self.logger.info(f"Searching for: {query}")
        
        try:
            # Generate query embedding
            query_embedding = self.embeddings_generator.generate_query_embedding(query)
            
            # Search vector database
            vector_results = self.vector_loader.search(
                query_vector=query_embedding.tolist(),
                limit=limit
            )
            
            if not vector_results:
                self.logger.info("No results found")
                return []
                
            # Get embedding IDs
            embedding_ids = [result['embedding_id'] for result in vector_results]
            
            # Get additional information from SQL database
            embedding_info = self.sql_loader.get_embedding_info(embedding_ids)
            
            # Combine results
            results = []
            for i, vector_result in enumerate(vector_results):
                # Find matching info from SQL
                info = next((info for info in embedding_info if info['embedding_id'] == vector_result['embedding_id']), {})
                
                result = {
                    'legislation_id': vector_result['legislation_id'],
                    'title': info.get('title', 'Unknown Title'),
                    'section_title': vector_result['section_title'],
                    'text': vector_result['text'],
                    'score': vector_result['score'],
                    'rank': i + 1
                }
                results.append(result)
                
            return results
                
        except Exception as e:
            self.logger.error(f"Error searching: {str(e)}")
            return []
            
    def display_results(self, results: List[Dict[str, Any]]) -> None:
        """
        Display search results in a formatted way.
        
        Args:
            results: List of search results
        """
        if not results:
            print("No results found.")
            return
            
        print(f"\nFound {len(results)} results:\n")
        
        for result in results:
            # Print horizontal separator
            print("\n" + "=" * 80 + "\n")
            
            # Print result
            print(f"Rank: {result['rank']} (Score: {result['score']:.4f})")
            print(f"Legislation: {result['title']}")
            if result.get('section_title'):
                print(f"Section: {result['section_title']}")
            
            print("\nParagraph:")
            print(result['text'])
            
        # Final separator
        print("\n" + "=" * 80)


def main():
    """Main entry point for the search CLI."""
    parser = argparse.ArgumentParser(description="UK Legislation Search CLI")
    parser.add_argument("query", nargs="*", help="Search query")
    parser.add_argument("--config", help="Path to configuration file")
    parser.add_argument("--limit", type=int, default=4, help="Maximum number of results to return")
    args = parser.parse_args()
    
    # Join query terms
    query = " ".join(args.query)
    
    if not query:
        print("Error: Search query is required.")
        parser.print_help()
        sys.exit(1)
    
    # Initialize CLI
    cli = LegislationSearchCLI(config_path=args.config)
    
    # Search and display results
    results = cli.search(query, limit=args.limit)
    cli.display_results(results)


if __name__ == "__main__":
    main()