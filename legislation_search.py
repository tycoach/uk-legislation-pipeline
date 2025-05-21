#!/usr/bin/env python3
"""
Legislation Search CLI - A tool to search legislation documents using semantic similarity

Usage:
  legislation_search.py [options] [QUERY]

Options:
  -h, --help            Show this help message
  -n, --num-results N   Number of results to show [default: 4]
  -l, --list            List all available legislation documents
  -d, --details ID      Show details for a specific legislation ID
  -c, --color           Use colored output (looks better in most terminals)
  -p, --plain           Use plain output (no colors or formatting)
  -v, --verbose         Show more detailed information in results
"""

import sys
import os
import textwrap
import argparse
from typing import List, Dict, Any, Optional

try:
    import numpy as np
    from sentence_transformers import SentenceTransformer
    from qdrant_client import QdrantClient
    from qdrant_client.models import Filter, FieldCondition, MatchValue
except ImportError:
    print("Required packages not found. Please install them using:")
    print("pip install qdrant-client sentence-transformers numpy")
    sys.exit(1)

# Define constants
COLLECTION_NAME = "legislation_embeddings"
MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"

# ANSI color codes (for nice terminal output)
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

# Global flag for colored output
use_colors = True

def c(text, color):
    """Apply color to text if colors are enabled"""
    if use_colors:
        return f"{color}{text}{Colors.ENDC}"
    return text

class LegislationSearch:
    def __init__(self, qdrant_host: str = "localhost", qdrant_port: int = 6333):
        """Initialize the legislation search tool"""
        self.client = QdrantClient(host=qdrant_host, port=qdrant_port)
        print(f"Loading embedding model {MODEL_NAME}...")
        self.model = SentenceTransformer(MODEL_NAME)
        print("Connecting to vector database...")
        
        try:
            collection_info = self.client.get_collection(COLLECTION_NAME)
            print(f"Connected to collection with {collection_info.vectors_count} embeddings")
        except Exception as e:
            print(f"Error connecting to Qdrant: {e}")
            sys.exit(1)

    def search(self, query: str, limit: int = 4, verbose: bool = False) -> None:
        """Search for legislation matching the query"""
        print(f"Searching for: {c(query, Colors.YELLOW)}")
        
        # Generate embedding for the query
        query_embedding = self.model.encode(query)
        
        # Normalize the vector
        query_embedding = query_embedding / np.linalg.norm(query_embedding)
        
        # Search
        results = self.client.search(
            collection_name=COLLECTION_NAME,
            query_vector=query_embedding.tolist(),
            limit=limit,
            with_payload=True,
        )
        
        if not results:
            print(c("No results found.", Colors.RED))
            return
        
        # Group results by legislation ID to avoid duplicates
        grouped_results = {}
        for result in results:
            leg_id = result.payload.get('legislation_id')
            if leg_id not in grouped_results or result.score > grouped_results[leg_id]['score']:
                grouped_results[leg_id] = {
                    'score': result.score,
                    'payload': result.payload,
                    'point_id': result.id
                }
        
        # Display results
        print(c(f"\nFound {len(grouped_results)} relevant legislation documents:", Colors.GREEN))
        print("-" * 80)
        
        for i, (leg_id, result) in enumerate(sorted(grouped_results.items(), 
                                             key=lambda x: x[1]['score'], 
                                             reverse=True), 1):
            score = result['score']
            payload = result['payload']
            
            # Format and display the result
            title = payload.get('section_title', 'Untitled Section')
            
            # Get text and format it nicely
            text = payload.get('text', '')
            wrapped_text = textwrap.fill(text[:500], width=80)
            if len(text) > 500:
                wrapped_text += "..."
            
            print(c(f"Result {i} - Similarity: {score:.4f}", Colors.BOLD))
            print(c(f"Legislation ID: {leg_id}", Colors.BLUE))
            print(c(f"Section: {title}", Colors.CYAN))
            print(f"\n{wrapped_text}\n")
            
            if verbose:
                print(c("Additional Information:", Colors.BOLD))
                print(f"  Section Index: {payload.get('section_idx', 'N/A')}")
                print(f"  Section Type: {payload.get('section_type', 'N/A')}")
                print(f"  Section Number: {payload.get('section_number', 'N/A')}")
                print(f"  Chunk Index: {payload.get('chunk_idx', 'N/A')}")
                print(f"  Point ID: {result['point_id']}")
            
            print("-" * 80)

    def list_legislation(self) -> None:
        """List all legislation documents in the database"""
        print("Fetching legislation documents...")
        
        # Get unique legislation IDs
        legislation_ids = set()
        offset = None
        
        while True:
            batch, offset = self.client.scroll(
                collection_name=COLLECTION_NAME,
                limit=100,
                offset=offset,
                with_payload=["legislation_id", "section_title"],
                with_vectors=False,
            )
            
            for point in batch:
                legislation_ids.add(point.payload.get('legislation_id'))
                
            if offset is None:
                break
        
        if not legislation_ids:
            print(c("No legislation documents found.", Colors.RED))
            return
        
        print(c(f"\nFound {len(legislation_ids)} legislation documents:", Colors.GREEN))
        print("-" * 80)
        
        # Get more details about each legislation
        for i, leg_id in enumerate(sorted(legislation_ids), 1):
            # Get first point for this legislation
            results = self.client.search(
                collection_name=COLLECTION_NAME,
                query_vector=[0] * self.model.get_sentence_embedding_dimension(),  # Dummy vector
                limit=1,
                with_payload=True,
                filter=Filter(
                    must=[FieldCondition(key="legislation_id", match=MatchValue(value=leg_id))]
                )
            )
            
            if results:
                title = results[0].payload.get('section_title', 'Untitled')
                print(c(f"{i}. {leg_id}", Colors.BLUE))
                print(f"   Title: {title}")
                print()
            else:
                print(c(f"{i}. {leg_id}", Colors.BLUE))
                print()

    def show_legislation_details(self, legislation_id: str) -> None:
        """Show details for a specific legislation"""
        print(f"Fetching details for legislation: {c(legislation_id, Colors.BLUE)}")
        
        # Get all chunks for this legislation
        filter_condition = Filter(
            must=[FieldCondition(key="legislation_id", match=MatchValue(value=legislation_id))]
        )
        
        points = self.client.scroll(
            collection_name=COLLECTION_NAME,
            filter=filter_condition,
            limit=100,
            with_payload=True,
            with_vectors=False,
        )
        
        batch, _ = points
        
        if not batch:
            print(c(f"No data found for legislation ID: {legislation_id}", Colors.RED))
            return
        
        print(c(f"\nFound {len(batch)} sections/chunks for legislation {legislation_id}:", Colors.GREEN))
        print("-" * 80)
        
        # Group by section
        sections = {}
        for point in batch:
            section_idx = point.payload.get('section_idx', 0)
            
            if section_idx not in sections:
                sections[section_idx] = {
                    'title': point.payload.get('section_title', 'Untitled Section'),
                    'section_type': point.payload.get('section_type', ''),
                    'section_number': point.payload.get('section_number', ''),
                    'chunks': []
                }
            
            sections[section_idx]['chunks'].append({
                'chunk_idx': point.payload.get('chunk_idx', 0),
                'text': point.payload.get('text', ''),
                'point_id': point.id
            })
        
        # Display sections and chunks
        for section_idx, section_data in sorted(sections.items()):
            print(c(f"Section {section_idx}: {section_data['title']}", Colors.BOLD + Colors.CYAN))
            if section_data['section_type']:
                print(f"Type: {section_data['section_type']}")
            if section_data['section_number']:
                print(f"Number: {section_data['section_number']}")
            
            print(c(f"Chunks: {len(section_data['chunks'])}", Colors.YELLOW))
            
            # Show text from all chunks in the section
            all_text = '\n'.join([chunk['text'] for chunk in sorted(
                section_data['chunks'], 
                key=lambda x: x['chunk_idx']
            )])
            
            print("\n" + textwrap.fill(all_text, width=80) + "\n")
            print("-" * 80)


def main():
    """Main CLI entrypoint"""
    global use_colors
    
    parser = argparse.ArgumentParser(
        description="Legislation Search CLI - Search legislation documents using semantic similarity"
    )
    parser.add_argument("query", nargs="?", help="The search query (optional if using --list or --details)")
    parser.add_argument("-n", "--num-results", type=int, default=4, help="Number of results to show (default: 4)")
    parser.add_argument("-l", "--list", action="store_true", help="List all available legislation documents")
    parser.add_argument("-d", "--details", metavar="ID", help="Show details for a specific legislation ID")
    parser.add_argument("-c", "--color", action="store_true", help="Use colored output (default)")
    parser.add_argument("-p", "--plain", action="store_true", help="Use plain output (no colors)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Show more detailed information in results")
    
    args = parser.parse_args()
    
    # Handle color options
    if args.plain:
        use_colors = False
    
    # Set environment variables for the Qdrant connection
    qdrant_host = os.environ.get("QDRANT_HOST", "localhost")
    qdrant_port = int(os.environ.get("QDRANT_PORT", "6333"))
    
    # Initialize search tool
    search_tool = LegislationSearch(qdrant_host, qdrant_port)
    
    # Execute the requested command
    if args.list:
        search_tool.list_legislation()
    elif args.details:
        search_tool.show_legislation_details(args.details)
    elif args.query:
        search_tool.search(args.query, limit=args.num_results, verbose=args.verbose)
    else:
        parser.print_help()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nSearch cancelled by user.")
        sys.exit(0)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)