"""
Task persistence service.

Handles saving and loading arbitrage tasks to/from disk.
"""

import json
import time
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime

from ..models import ArbitrageTask
from ..config import settings
from ..config.logging_config import get_logger

logger = get_logger("services.task_persistence")


class TaskPersistence:
    """
    Persists arbitrage tasks to disk.
    
    Tasks are saved to a JSON file in the data directory.
    """
    
    def __init__(self, data_dir: Optional[Path] = None):
        """
        Initialize task persistence.
        
        Args:
            data_dir: Directory to store task file
        """
        self.data_dir = Path(data_dir) if data_dir else settings.data_dir
        self.tasks_file = self.data_dir / "tasks.json"
        
        # Ensure directory exists
        self.data_dir.mkdir(parents=True, exist_ok=True)
    
    def save_tasks(self, tasks: Dict[str, ArbitrageTask]) -> bool:
        """
        Save all tasks to disk.
        
        Args:
            tasks: Dictionary of task_id -> ArbitrageTask
        
        Returns:
            True if saved successfully
        """
        try:
            task_list = []
            for task_id, task in tasks.items():
                task_data = task.to_dict()
                # Add metadata
                task_data["_saved_at"] = datetime.now().isoformat()
                task_list.append(task_data)
            
            data = {
                "version": 1,
                "updated_at": datetime.now().isoformat(),
                "tasks": task_list
            }
            
            # Write to temp file first, then rename (atomic write)
            temp_file = self.tasks_file.with_suffix(".tmp")
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            # Windows compatibility: Retry logic for replace
            max_retries = 5
            for i in range(max_retries):
                try:
                    temp_file.replace(self.tasks_file)
                    break
                except PermissionError:
                    if i == max_retries - 1:
                        # On final failure, try unlink then rename (non-atomic but works)
                        try:
                            if self.tasks_file.exists():
                                self.tasks_file.unlink()
                            temp_file.rename(self.tasks_file)
                        except Exception as e:
                            logger.error(f"Final save attempt failed: {e}")
                            raise e
                    else:
                        time.sleep(0.1)
                except Exception as e:
                    raise e
            
            logger.info(f"Saved {len(task_list)} tasks to {self.tasks_file}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save tasks: {e}")
            return False
    
    def load_tasks(self) -> List[Dict[str, Any]]:
        """
        Load tasks from disk.
        
        Returns:
            List of task dictionaries
        """
        if not self.tasks_file.exists():
            logger.info("No tasks file found, starting fresh")
            return []
        
        try:
            with open(self.tasks_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            tasks = data.get("tasks", [])
            logger.info(f"Loaded {len(tasks)} tasks from {self.tasks_file}")
            return tasks
            
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in tasks file: {e}")
            return []
        except Exception as e:
            logger.error(f"Failed to load tasks: {e}")
            return []
    
    def add_task(self, task: ArbitrageTask, existing_tasks: Dict[str, ArbitrageTask]) -> bool:
        """
        Add a single task and save.
        
        Args:
            task: Task to add
            existing_tasks: Current tasks dictionary (will be updated)
        
        Returns:
            True if saved successfully
        """
        existing_tasks[task.task_id] = task
        return self.save_tasks(existing_tasks)
    
    def remove_task(self, task_id: str, existing_tasks: Dict[str, ArbitrageTask]) -> bool:
        """
        Remove a task and save.
        
        Args:
            task_id: ID of task to remove
            existing_tasks: Current tasks dictionary (will be updated)
        
        Returns:
            True if saved successfully
        """
        if task_id in existing_tasks:
            del existing_tasks[task_id]
        return self.save_tasks(existing_tasks)
    
    def create_task_from_dict(self, data: Dict[str, Any]) -> ArbitrageTask:
        """
        Create an ArbitrageTask from a dictionary.
        
        Args:
            data: Task data dictionary
        
        Returns:
            ArbitrageTask instance
        """
        task = ArbitrageTask(
            task_id=data.get("task_id", ""),
            exchange_a=data.get("exchange_a", "mt5"),
            exchange_b=data.get("exchange_b", "okx"),
            symbol_a=data.get("symbol_a", ""),
            symbol_b=data.get("symbol_b", ""),
            config=data.get("config", {})
        )
        
        # Restore additional state if present
        if "total_pnl" in data:
            task.total_pnl = float(data["total_pnl"])
        if "total_trades" in data:
            task.total_trades = int(data["total_trades"])
        if "win_trades" in data:
            task.win_trades = int(data["win_trades"])
        if "error_count" in data:
            task.error_count = int(data["error_count"])
            
        if data.get("created_at"):
            try:
                task.created_at = datetime.fromisoformat(data["created_at"])
            except:
                pass
        
        return task


# Global instance
task_persistence = TaskPersistence()
