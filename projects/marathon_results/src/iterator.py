import pandas as pd
from pathlib import Path
from tqdm.notebook import tqdm
from typing import Callable, List, Any, Optional

class CheckpointIterator:
    """
    A reusable iterator that processes items with checkpointing support.
    
    Features:
    - Progress tracking with tqdm
    - Periodic saves to CSV
    - Resume from previous checkpoint
    - Handles errors gracefully
    """
    
    def __init__(
        self,
        items: List[Any],
        item_key: Callable[[Any],List[Any]],
        process_func: Callable[[Any], dict],
        output_path: str,
        checkpoint_every: int = 10,
        key_fields: Optional[List[str]] = None
    ):
        """
        Args:
            items: List of items to process
            item_key: Function that operates on an item and returns its key
            process_func: Function that takes an item and returns a dict/record
            output_path: Path to save CSV results
            checkpoint_every: Save after this many items (default: 10)
            key_fields: Fields that uniquely identify a record (for resuming)
        """
        self.items = items
        self.item_key = item_key
        self.process_func = process_func
        self.output_path = Path(output_path)
        self.checkpoint_every = checkpoint_every
        self.key_fields = key_fields or []
        
        self.results = []
        self.completed_keys = set()
        self._load_existing()
    
    def _load_existing(self):
        """Load existing results if the file exists."""
        if self.output_path.exists():
            existing_df = pd.read_csv(self.output_path)
            self.results = existing_df.to_dict('records')
            
            # Track completed items by key fields
            if self.key_fields:
                self.completed_keys = {
                    tuple(record[field] for field in self.key_fields)
                    for record in self.results
                }
            
            print(f"Loaded {len(self.results)} existing records from {self.output_path}")
        else:
            print(f"Starting fresh - no existing file at {self.output_path}")
    
    def _save_checkpoint(self):
        """Save current results to CSV."""
        df = pd.DataFrame(self.results)
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(self.output_path, index=False)
    
    def _get_item_key(self, item):
        """Extract key from item based on key_fields."""
        if self.item_key:
            return self.item_key(item)
        
        if not self.key_fields:
            return None
        
        # If item is a dict/record, use key_fields directly
        if isinstance(item, dict):
            return tuple(item[field] for field in self.key_fields)
        
        # If item is a tuple/list, assume it matches key_fields order
        if isinstance(item, (tuple, list)):
            return tuple(item[:len(self.key_fields)])
        
        # Otherwise, use the item itself as the key
        return (item,)
    
    def _is_completed(self, item):
        """Check if item has already been processed."""
        if not self.key_fields:
            return False
        
        key = self._get_item_key(item)
        return key in self.completed_keys
    
    def process(self):
        """
        Process all items with progress tracking and checkpointing.
        
        Returns:
            DataFrame with all results
        """
        # Filter out already completed items
        items_to_process = [
            item for item in self.items
            if not self._is_completed(item)
        ]
        
        print(f"Processing {len(items_to_process)} items ({len(self.items) - len(items_to_process)} already completed)")
        
        # Process items with progress bar
        for i, item in enumerate(tqdm(items_to_process, desc="Processing"), start=1):
            try:
                # Call the user-provided function
                result = self.process_func(item)
                
                if result is not None:
                    self.results.append(result)
                    
                    # Track completion
                    if self.key_fields:
                        key = self._get_item_key(result)
                        self.completed_keys.add(key)
                
                # Periodic checkpoint
                if i % self.checkpoint_every == 0:
                    self._save_checkpoint()
                    
            except Exception as e:
                print(f"Error processing item {item}: {e}")
                # Continue processing other items
        
        # Final save
        self._save_checkpoint()
        
        return pd.DataFrame(self.results)


# Example usage:
if __name__ == "__main__":
    # Example 1: Simple list processing
    def square_number(n):
        return {"number": n, "square": n ** 2}
    
    numbers = list(range(1, 101))
    iterator = CheckpointIterator(
        items=numbers,
        process_func=square_number,
        output_path="data/squares.csv",
        checkpoint_every=10,
        key_fields=["number"]
    )
    result_df = iterator.process()
    
    
    # Example 2: Processing city/state pairs (like your weather use case)
    def fetch_data_for_city(city_state_tuple):
        city, state = city_state_tuple
        # Simulate some processing
        return {
            "city": city,
            "state": state,
            "population": 100000,  # placeholder
            "data": "some data"
        }
    
    cities = [("New York", "NY"), ("Los Angeles", "CA"), ("Chicago", "IL")]
    iterator = CheckpointIterator(
        items=cities,
        process_func=fetch_data_for_city,
        output_path="data/city_data.csv",
        checkpoint_every=5,
        key_fields=["city", "state"]
    )
    result_df = iterator.process()
