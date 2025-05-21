import os
import logging
import time
import json
from typing import Dict, Any, List, Optional

try:
    import qdrant_client
    from qdrant_client.http import models
    from qdrant_client.http.exceptions import UnexpectedResponse
    QDRANT_AVAILABLE = True
except ImportError:
    QDRANT_AVAILABLE = False


class VectorLoader:
    """
    Loads and queries embeddings in Qdrant vector database.
    """

    COLLECTION_NAME = "legislation_embeddings"

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        grpc_port: Optional[int] = None,
        vector_size: int = 384,
        recreate_collection: bool = False,
    ):
        self.logger = logging.getLogger(__name__)

        if not QDRANT_AVAILABLE:
            self.logger.error("Qdrant client not available. Please install with `pip install qdrant-client`")
            raise ImportError("Qdrant client not available")

        self.host = host or os.environ.get("VECTOR_DB_HOST", "localhost")
        self.port = port or int(os.environ.get("VECTOR_DB_PORT", 6333))
        self.grpc_port = grpc_port or int(os.environ.get("VECTOR_DB_GRPC_PORT", 6334))
        self.vector_size = vector_size
        self.recreate_collection = recreate_collection

        self.client = None
        self._connect()
        self._init_collection()

    def _connect(self) -> None:
        max_retries = 5
        retry_delay = 2

        for attempt in range(max_retries):
            try:
                self.logger.info(f"Connecting to Qdrant at {self.host}:{self.port}")
                self.client = qdrant_client.QdrantClient(
                    host=self.host,
                    port=self.port,
                    grpc_port=self.grpc_port,
                    timeout=60,
                )
                self.client.get_collections()  # Test connection
                self.logger.info("Qdrant connection established successfully")
                break
            except Exception as e:
                self.logger.warning(f"Attempt {attempt + 1}/{max_retries} failed: {str(e)}")
                if attempt < max_retries - 1:
                    self.logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    self.logger.error("Failed to connect to Qdrant after retries")
                    raise

    def _init_collection(self) -> None:
        if not self.client:
            self.logger.error("No Qdrant connection available")
            return

        try:
            collections = self.client.get_collections().collections
            collection_names = [c.name for c in collections]

            if self.COLLECTION_NAME in collection_names:
                if self.recreate_collection:
                    self.logger.info(f"Recreating collection {self.COLLECTION_NAME}")
                    self.client.delete_collection(self.COLLECTION_NAME)
                else:
                    self.logger.info(f"Collection {self.COLLECTION_NAME} already exists")
                    return

            self.logger.info(f"Creating collection {self.COLLECTION_NAME}")
            self.client.create_collection(
                collection_name=self.COLLECTION_NAME,
                vectors_config=models.VectorParams(
                    size=self.vector_size, distance=models.Distance.COSINE
                ),
            )
            self.client.create_payload_index(
                collection_name=self.COLLECTION_NAME,
                field_name="legislation_id",
                field_schema=models.PayloadSchemaType.KEYWORD,
            )
            self.client.create_payload_index(
                collection_name=self.COLLECTION_NAME,
                field_name="section_idx",
                field_schema=models.PayloadSchemaType.INTEGER,
            )
            self.logger.info(f"Collection {self.COLLECTION_NAME} initialized")

        except Exception as e:
            self.logger.error(f"Error initializing Qdrant collection: {str(e)}")
            raise

    def store_embeddings(self, legislation_data: Dict[str, Any]) -> bool:
        if not self.client:
            self._connect()

        if "embeddings" not in legislation_data or not legislation_data["embeddings"]:
            self.logger.error("No embeddings found in legislation data")
            return False

        legislation_id = legislation_data.get("id")
        if not legislation_id:
            self.logger.error("Missing legislation ID")
            return False

        try:
            self._delete_legislation_embeddings(legislation_id)

            points = []
            for embedding_data in legislation_data["embeddings"]:
                if "vector" not in embedding_data:
                    continue

                # Use the numeric point_id from EmbeddingsGenerator if available
                if "point_id" in embedding_data and isinstance(embedding_data["point_id"], int):
                    point_id = embedding_data["point_id"]
                else:
                    # Generate a deterministic numeric ID based on the legislation ID and section/chunk indices
                    import hashlib
                    hash_str = f"{legislation_id}_s{embedding_data.get('section_idx', 0)}_c{embedding_data.get('chunk_idx', 0)}"
                    hash_bytes = hashlib.md5(hash_str.encode()).digest()[:4]  # First 4 bytes
                    point_id = int.from_bytes(hash_bytes, byteorder='big')  # Convert to integer

                point = models.PointStruct(
                    id=point_id,  # Use numeric ID
                    vector=embedding_data["vector"],
                    payload={
                        "legislation_id": legislation_id,
                        "section_idx": embedding_data.get("section_idx", 0),
                        "section_type": embedding_data.get("section_type", ""),
                        "section_number": embedding_data.get("section_number", ""),
                        "section_title": embedding_data.get("section_title", ""),
                        "chunk_idx": embedding_data.get("chunk_idx", 0),
                        "text": embedding_data.get("text", ""),
                        "original_id": f"{legislation_id}_s{embedding_data.get('section_idx', 0)}_c{embedding_data.get('chunk_idx', 0)}"  # Store the original ID in the payload for reference
                    },
                )
                points.append(point)

            batch_size = 500
            for i in range(0, len(points), batch_size):
                batch = points[i : i + batch_size]
                self.client.upsert(collection_name=self.COLLECTION_NAME, points=batch)
                self.logger.info(
                    f"Uploaded {len(batch)} embeddings for legislation {legislation_id} (batch {i // batch_size + 1})"
                )

            return True

        except Exception as e:
            self.logger.error(f"Error storing embeddings for legislation {legislation_id}: {str(e)}")
            return False
    def _delete_legislation_embeddings(self, legislation_id: str) -> bool:
        try:
            self.client.delete(
                collection_name=self.COLLECTION_NAME,
                points_selector=models.FilterSelector(
                    filter=models.Filter(
                        must=[
                            models.FieldCondition(
                                key="legislation_id",
                                match=models.MatchValue(value=legislation_id),
                            )
                        ]
                    )
                ),
            )
            self.logger.info(f"Deleted existing embeddings for legislation {legislation_id}")
            return True
        except Exception as e:
            self.logger.error(f"Error deleting embeddings for legislation {legislation_id}: {str(e)}")
            return False

    def batch_store_embeddings(self, legislation_list: List[Dict[str, Any]]) -> int:
        success_count = 0
        for legislation in legislation_list:
            if self.store_embeddings(legislation):
                success_count += 1
        self.logger.info(f"Stored embeddings for {success_count}/{len(legislation_list)} legislation documents")
        return success_count

    def search(
        self,
        query_vector: List[float],
        limit: int = 4,
        legislation_id: Optional[str] = None,
        section_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        if not self.client:
            self._connect()

        try:
            filter_conditions = []

            if legislation_id:
                filter_conditions.append(
                    models.FieldCondition(
                        key="legislation_id", match=models.MatchValue(value=legislation_id)
                    )
                )

            if section_type:
                filter_conditions.append(
                    models.FieldCondition(
                        key="section_type", match=models.MatchValue(value=section_type)
                    )
                )

            search_filter = None
            if filter_conditions:
                search_filter = models.Filter(must=filter_conditions)

            search_results = self.client.search(
                collection_name=self.COLLECTION_NAME,
                query_vector=query_vector,
                limit=limit,
                with_payload=True,
                filter=search_filter,
            )

            results = []
            for result in search_results:
                results.append(
                    {
                        "embedding_id": result.id,
                        "score": result.score,
                        "legislation_id": result.payload.get("legislation_id"),
                        "section_idx": result.payload.get("section_idx"),
                        "section_type": result.payload.get("section_type"),
                        "section_number": result.payload.get("section_number"),
                        "section_title": result.payload.get("section_title"),
                        "chunk_idx": result.payload.get("chunk_idx"),
                        "text": result.payload.get("text"),
                    }
                )

            return results

        except Exception as e:
            self.logger.error(f"Error searching vector database: {str(e)}")
            return []

    def count_embeddings(self, legislation_id: Optional[str] = None) -> int:
        if not self.client:
            self._connect()

        try:
            search_filter = None
            if legislation_id:
                search_filter = models.Filter(
                    must=[
                        models.FieldCondition(
                            key="legislation_id", match=models.MatchValue(value=legislation_id)
                        )
                    ]
                )

            if search_filter:
                count = 0
                batch_size = 1000
                offset = None

                while True:
                    scroll_response = self.client.scroll(
                        collection_name=self.COLLECTION_NAME,
                        filter=search_filter,
                        limit=batch_size,
                        offset=offset,
                        with_payload=False,
                        with_vectors=False,
                    )
                    batch_points, offset = scroll_response
                    count += len(batch_points)

                    if not offset:
                        break

                return count
            else:
                collection_info = self.client.get_collection(self.COLLECTION_NAME)
                return collection_info.vectors_count

        except Exception as e:
            self.logger.error(f"Error counting embeddings: {str(e)}")
            return 0

    def get_collection_info(self) -> Dict[str, Any]:
        if not self.client:
            self._connect()

        try:
            collection_info = self.client.get_collection(self.COLLECTION_NAME)

            return {
                "name": collection_info.name,
                "vectors_count": collection_info.vectors_count,
                "points_count": collection_info.points_count,
                "vector_size": collection_info.config.params.vectors.size,
                "distance": str(collection_info.config.params.vectors.distance),
                "status": "ready",
            }
        except UnexpectedResponse:
            return {"name": self.COLLECTION_NAME, "status": "not_found"}
        except Exception as e:
            self.logger.error(f"Error getting collection info: {str(e)}")
            return {"name": self.COLLECTION_NAME, "status": "error", "error": str(e)}

    def close(self) -> None:
        if self.client:
            self.client = None
            self.logger.info("Vector database connection closed")
