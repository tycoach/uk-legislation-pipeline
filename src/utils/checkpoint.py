import os
import json
import time
import logging
import shutil
from typing import Dict, Any, List, Optional
from datetime import datetime


class CheckpointManager:
    """
    Manages checkpoints for the ETL pipeline to enable resilient processing.
    
    This class handles:
    - Saving progress at regular intervals
    - Loading from last checkpoint after a failure
    - Tracking processed items to avoid duplication
    """
    
    def __init__(self, 
                 checkpoint_dir: str = "/data/checkpoints",
                 pipeline_id: str = None,
                 interval: int = 50):
        """
        Initialize checkpoint manager.
        
        Args:
            checkpoint_dir: Directory to store checkpoints
            pipeline_id: Unique identifier for the pipeline run
            interval: Save checkpoint every N items
        """
        self.logger = logging.getLogger(__name__)
        self.checkpoint_dir = checkpoint_dir
        
        # Generate pipeline ID if not provided
        if pipeline_id is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.pipeline_id = f"pipeline_{timestamp}"
        else:
            self.pipeline_id = pipeline_id
            
        self.interval = interval
        
        # Ensure checkpoint directory exists
        os.makedirs(self.checkpoint_dir, exist_ok=True)
        
        # Initialize state
        self.checkpoint_path = os.path.join(self.checkpoint_dir, f"{self.pipeline_id}.json")
        self.temp_checkpoint_path = f"{self.checkpoint_path}.tmp"
        self.state = {
            "pipeline_id": self.pipeline_id,
            "start_time": datetime.now().isoformat(),
            "last_update": None,
            "items_processed": 0,
            "processed_ids": [],
            "current_batch": None,
            "current_stage": None,
            "stats": {},
            "error": None
        }
        
        # Try to load existing checkpoint
        self._load_checkpoint()
        
        self.logger.info(f"Checkpoint manager initialized with pipeline ID: {self.pipeline_id}")
        self.logger.info(f"Checkpoint file: {self.checkpoint_path}")
        self.logger.info(f"Items processed so far: {self.state['items_processed']}")

    def _load_checkpoint(self) -> bool:
        """
        Load checkpoint from file if it exists.
        
        Returns:
            True if checkpoint was loaded, False otherwise
        """
        if not os.path.exists(self.checkpoint_path):
            self.logger.info("No checkpoint found, starting fresh")
            return False
            
        try:
            with open(self.checkpoint_path, 'r') as f:
                loaded_state = json.load(f)
            
            # Update state with loaded values
            self.state.update(loaded_state)
            
            # Convert lists back to sets if needed
            if "processed_ids" in loaded_state and isinstance(loaded_state["processed_ids"], list):
                self.state["processed_ids"] = loaded_state["processed_ids"]
            
            self.logger.info(f"Loaded checkpoint from {self.checkpoint_path}")
            self.logger.info(f"Resuming from stage: {self.state['current_stage']}")
            self.logger.info(f"Items processed: {self.state['items_processed']}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading checkpoint: {str(e)}")
            self.logger.info("Starting with fresh state")
            
            # Try to load from temporary file if main file is corrupted
            if os.path.exists(self.temp_checkpoint_path):
                try:
                    with open(self.temp_checkpoint_path, 'r') as f:
                        loaded_state = json.load(f)
                    
                    # Update state with loaded values
                    self.state.update(loaded_state)
                    
                    self.logger.info(f"Loaded checkpoint from temporary file {self.temp_checkpoint_path}")
                    return True
                    
                except Exception as e_temp:
                    self.logger.error(f"Error loading from temporary checkpoint: {str(e_temp)}")
            
            return False

    def save(self, force: bool = False) -> bool:
        """
        Save checkpoint to file.
        
        Args:
            force: Whether to force save regardless of interval
            
        Returns:
            True if checkpoint was saved, False otherwise
        """
        # Update last update time
        self.state["last_update"] = datetime.now().isoformat()
        
        # Save only if forced or at regular intervals
        if not force and self.state["items_processed"] % self.interval != 0:
            return False
            
        try:
            # First write to temporary file
            with open(self.temp_checkpoint_path, 'w') as f:
                json.dump(self.state, f, indent=2)
            
            # Then atomically replace the main checkpoint file
            shutil.move(self.temp_checkpoint_path, self.checkpoint_path)
            
            self.logger.debug(f"Saved checkpoint at {self.state['items_processed']} items")
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving checkpoint: {str(e)}")
            return False

    def update_stage(self, stage: str) -> None:
        """
        Update the current processing stage.
        
        Args:
            stage: New processing stage
        """
        self.state["current_stage"] = stage
        self.logger.info(f"Pipeline stage updated: {stage}")
        self.save(force=True)

    def update_batch(self, batch_info: Dict[str, Any]) -> None:
        """
        Update information about the current batch.
        
        Args:
            batch_info: Information about the current batch
        """
        self.state["current_batch"] = batch_info
        self.save()

    def mark_processed(self, item_id: str) -> None:
        """
        Mark an item as processed.
        
        Args:
            item_id: ID of the processed item
        """
        if item_id not in self.state["processed_ids"]:
            self.state["processed_ids"].append(item_id)
            self.state["items_processed"] += 1
            self.save()

    def mark_batch_processed(self, item_ids: List[str]) -> None:
        """
        Mark a batch of items as processed.
        
        Args:
            item_ids: List of item IDs
        """
        new_items = [item_id for item_id in item_ids if item_id not in self.state["processed_ids"]]
        self.state["processed_ids"].extend(new_items)
        self.state["items_processed"] += len(new_items)
        self.save()

    def is_processed(self, item_id: str) -> bool:
        """
        Check if an item has already been processed.
        
        Args:
            item_id: ID of the item to check
            
        Returns:
            True if the item has been processed, False otherwise
        """
        return item_id in self.state["processed_ids"]

    def update_stats(self, key: str, value: Any) -> None:
        """
        Update statistics for the pipeline run.
        
        Args:
            key: Statistic name
            value: Statistic value
        """
        self.state["stats"][key] = value
        self.save()

    def increment_stat(self, key: str, increment: int = 1) -> None:
        """
        Increment a statistic counter.
        
        Args:
            key: Statistic name
            increment: Amount to increment by
        """
        if key not in self.state["stats"]:
            self.state["stats"][key] = 0
        self.state["stats"][key] += increment
        self.save()

    def record_error(self, error: str, stage: str = None) -> None:
        """
        Record an error in the pipeline.
        
        Args:
            error: Error message
            stage: Pipeline stage where the error occurred
        """
        self.state["error"] = {
            "message": error,
            "stage": stage or self.state["current_stage"],
            "time": datetime.now().isoformat()
        }
        self.save(force=True)
        self.logger.error(f"Error recorded in stage {stage or self.state['current_stage']}: {error}")

    def clear_error(self) -> None:
        """Clear any recorded error."""
        if self.state["error"]:
            self.state["error"] = None
            self.save(force=True)
    
    def get_state(self) -> Dict[str, Any]:
        """
        Get the current state.
        
        Returns:
            Current state dictionary
        """
        return self.state.copy()
    
    def get_processed_count(self) -> int:
        """
        Get the number of processed items.
        
        Returns:
            Number of processed items
        """
        return self.state["items_processed"]
    
    def get_processed_ids(self) -> List[str]:
        """
        Get list of processed item IDs.
        
        Returns:
            List of processed item IDs
        """
        return self.state["processed_ids"].copy()
    
    def reset(self) -> None:
        """
        Reset the checkpoint state to start fresh.
        """
        # Keep the pipeline ID but reset everything else
        pipeline_id = self.state["pipeline_id"]
        
        self.state = {
            "pipeline_id": pipeline_id,
            "start_time": datetime.now().isoformat(),
            "last_update": None,
            "items_processed": 0,
            "processed_ids": [],
            "current_batch": None,
            "current_stage": None,
            "stats": {},
            "error": None
        }
        
        self.save(force=True)
        self.logger.info(f"Checkpoint state reset for pipeline {pipeline_id}")