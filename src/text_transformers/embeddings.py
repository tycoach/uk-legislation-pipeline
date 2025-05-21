import os
import logging
import numpy as np
from typing import Dict, Any, List, Optional
from sentence_transformers import SentenceTransformer
import re
import torch
from tqdm import tqdm


class EmbeddingsGenerator:
    """
    Generates embeddings for legislation text using sentence-transformers.
    Uses the all-MiniLM-L6-v2 model as specified in requirements.
    """

    MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"

    def __init__(
        self,
        batch_size: int = 64,
        max_seq_length: int = 256,
        device: Optional[str] = None,
        use_progress_bar: bool = True,
    ) -> None:
        """
        Initialize the embeddings generator.

        Args:
            batch_size: Batch size for embedding generation
            max_seq_length: Maximum sequence length for the model
            device: Device to use for computation ('cuda', 'cpu', or None for auto-detection)
            use_progress_bar: Whether to show progress bar during embedding generation
        """
        self.logger = logging.getLogger(__name__)
        self.batch_size = batch_size
        self.max_seq_length = max_seq_length
        self.use_progress_bar = use_progress_bar

        if device is None:
            device = "cuda" if torch.cuda.is_available() else "cpu"

        self.device = device
        self.logger.info(f"Using device: {self.device} for embeddings generation")

        try:
            self.model = SentenceTransformer(self.MODEL_NAME, device=device)
            self.model.max_seq_length = max_seq_length
            self.logger.info(f"Loaded model: {self.MODEL_NAME}")
        except Exception as e:
            self.logger.error(f"Error loading model {self.MODEL_NAME}: {str(e)}")
            raise

    def _split_text_into_chunks(self, text: str, chunk_size: int = 200) -> List[str]:
        paragraphs = [p.strip() for p in text.split("\n") if p.strip()]
        chunks: List[str] = []
        current_chunk: List[str] = []
        current_size = 0

        for paragraph in paragraphs:
            if len(paragraph.split()) > chunk_size:
                sentences = re.split(r"(?<=[.!?])\s+", paragraph)
                for sentence in sentences:
                    sentence_size = len(sentence.split())
                    if current_size + sentence_size <= chunk_size:
                        current_chunk.append(sentence)
                        current_size += sentence_size
                    else:
                        if current_chunk:
                            chunks.append(" ".join(current_chunk))
                        current_chunk = [sentence]
                        current_size = sentence_size
            else:
                paragraph_size = len(paragraph.split())
                if current_size + paragraph_size <= chunk_size:
                    current_chunk.append(paragraph)
                    current_size += paragraph_size
                else:
                    if current_chunk:
                        chunks.append(" ".join(current_chunk))
                    current_chunk = [paragraph]
                    current_size = paragraph_size

        if current_chunk:
            chunks.append(" ".join(current_chunk))

        return chunks

    def _generate_chunk_embeddings(self, chunks: List[str]) -> List[np.ndarray]:
        all_embeddings: List[np.ndarray] = []

        iterator = range(0, len(chunks), self.batch_size)
        if self.use_progress_bar:
            iterator = tqdm(iterator, desc="Generating embeddings")

        for i in iterator:
            batch = chunks[i : i + self.batch_size]
            with torch.no_grad():
                batch_embeddings = self.model.encode(batch, convert_to_numpy=True)

            # Normalize embeddings to unit vectors (for cosine similarity)
            norms = np.linalg.norm(batch_embeddings, axis=1, keepdims=True)
            batch_embeddings = batch_embeddings / np.clip(norms, a_min=1e-10, a_max=None)

            all_embeddings.extend(batch_embeddings)

        return all_embeddings

    # def generate_embeddings(
    #     self, legislation_data: Dict[str, Any], chunk_size: int = 200
    # ) -> Dict[str, Any]:
    #     """
    #     Generate embeddings for the given legislation data.

    #     """
    #     if "content" not in legislation_data:
    #         self.logger.error("Missing content in legislation data")
    #         return legislation_data

    #     try:
    #         legislation_data["embeddings"] = []

    #         for section_idx, section in enumerate(legislation_data["content"]):
    #             section_text = section.get("text", "")
    #             if not section_text:
    #                 continue

    #             text_chunks = self._split_text_into_chunks(section_text, chunk_size)
    #             chunk_embeddings = self._generate_chunk_embeddings(text_chunks)

    #             for chunk_idx, (chunk, embedding) in enumerate(zip(text_chunks, chunk_embeddings)):
    #                 # Ensure point ID is a valid unsigned integer
    #                 point_id = point_id_counter
    #                 point_id_counter += 1


    #                 embedding_data = {
    #                     "section_idx": section_idx,
    #                     "section_type": section.get("section_type", ""),
    #                     "section_number": section.get("section_number", ""),
    #                     "section_title": section.get("section_title", ""),
    #                     "chunk_idx": chunk_idx,
    #                     "text": chunk,
    #                     "vector": embedding.tolist(),
    #                     "point_id": point_id,  # Use the fixed point_id here
    #                     "metadata": {
    #                     "legislation_id": legislation_data.get("id", ""),
    #                     "section_idx": section_idx,
    #                     "chunk_idx": chunk_idx
    #                     }
    #                 }
    #                 legislation_data["embeddings"].append(embedding_data)

    #         self.logger.info(f"Generated {len(legislation_data['embeddings'])} embeddings")
    #         return legislation_data

    #     except Exception as e:
    #         self.logger.error(f"Error generating embeddings: {str(e)}")
    #         return legislation_data

    def generate_embeddings(
        self, legislation_data: Dict[str, Any], chunk_size: int = 200
    ) -> Dict[str, Any]:
        """
        Generate embeddings for the given legislation data.
        """
        legislation_id = legislation_data.get("id", "unknown_id")
        self.logger.info(f"Starting embedding generation for legislation: {legislation_id}")
        
        if "content" not in legislation_data:
            self.logger.error(f"Missing content in legislation data: {legislation_id}")
            return legislation_data

        try:
            legislation_data["embeddings"] = []
            point_id_counter = 0  # Initialize counter here before use
            
            # Log the content structure
            content_count = len(legislation_data["content"])
            self.logger.info(f"Found {content_count} content sections in legislation: {legislation_id}")
            
            for section_idx, section in enumerate(legislation_data["content"]):
                section_text = section.get("text", "")
                if not section_text:
                    self.logger.warning(f"Empty text in section {section_idx} for legislation: {legislation_id}")
                    continue
                    
                # Log section text length for debugging
                self.logger.debug(f"Section {section_idx} has {len(section_text)} characters")

                text_chunks = self._split_text_into_chunks(section_text, chunk_size)
                self.logger.debug(f"Split section {section_idx} into {len(text_chunks)} chunks")
                
                chunk_embeddings = self._generate_chunk_embeddings(text_chunks)
                self.logger.debug(f"Generated {len(chunk_embeddings)} embeddings for section {section_idx}")

                for chunk_idx, (chunk, embedding) in enumerate(zip(text_chunks, chunk_embeddings)):
                    # Use simple integer IDs
                    point_id = point_id_counter
                    point_id_counter += 1

                    # Keep section/chunk information as metadata
                    embedding_data = {
                        "section_idx": section_idx,
                        "section_type": section.get("section_type", ""),
                        "section_number": section.get("section_number", ""),
                        "section_title": section.get("section_title", ""),
                        "chunk_idx": chunk_idx,
                        "text": chunk,
                        "vector": embedding.tolist(),
                        "point_id": point_id,  # Use numeric point_id
                        # Add metadata for search/filtering if needed
                        "metadata": {
                            "legislation_id": legislation_id,
                            "section_idx": section_idx,
                            "chunk_idx": chunk_idx
                        }
                    }
                    legislation_data["embeddings"].append(embedding_data)
                    
            embedding_count = len(legislation_data["embeddings"])
            self.logger.info(f"Generated total of {embedding_count} embeddings for legislation: {legislation_id}")
            
            return legislation_data

        except Exception as e:
            self.logger.error(f"Error generating embeddings for {legislation_id}: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            return legislation_data

    def generate_query_embedding(self, query: str) -> np.ndarray:
        try:
            with torch.no_grad():
                query_embedding = self.model.encode(query, convert_to_numpy=True)

            norm = np.linalg.norm(query_embedding, keepdims=True)
            query_embedding = query_embedding / max(norm, 1e-10)

            return query_embedding

        except Exception as e:
            self.logger.error(f"Error generating query embedding: {str(e)}")
            return np.zeros(self.model.get_sentence_embedding_dimension())

    def batch_process_legislation(
        self, legislation_list: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        processed_docs: List[Dict[str, Any]] = []

        for doc in legislation_list:
            processed_doc = self.generate_embeddings(doc)
            processed_docs.append(processed_doc)

        return processed_docs
