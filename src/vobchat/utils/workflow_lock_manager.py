"""Workflow execution lock management for VobChat SSE"""

import threading
import time
import logging
from typing import Dict, Optional
from contextlib import contextmanager
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class WorkflowExecution:
    """Track active workflow execution"""
    thread_id: str
    start_time: float
    execution_id: str
    lock: threading.Lock
    
class WorkflowLockManager:
    """Manages concurrent workflow execution locks per thread"""
    
    def __init__(self):
        self._thread_locks: Dict[str, WorkflowExecution] = {}
        self._manager_lock = threading.Lock()
        self._execution_timeout = 300  # 5 minutes default timeout
        
    def is_workflow_running(self, thread_id: str) -> bool:
        """Check if a workflow is currently running for this thread"""
        with self._manager_lock:
            if thread_id not in self._thread_locks:
                return False
                
            execution = self._thread_locks[thread_id]
            
            # Check if execution has timed out
            if time.time() - execution.start_time > self._execution_timeout:
                logger.warning(f"Workflow execution for thread {thread_id} timed out, releasing lock")
                self._cleanup_execution(thread_id)
                return False
                
            # Check if lock is still held
            if not execution.lock.locked():
                logger.info(f"Workflow execution for thread {thread_id} completed, cleaning up")
                self._cleanup_execution(thread_id)
                return False
                
            return True
    
    @contextmanager
    def acquire_workflow_lock(self, thread_id: str, execution_id: Optional[str] = None):
        """Acquire exclusive workflow execution lock for a thread"""
        if execution_id is None:
            execution_id = f"{thread_id}_{int(time.time() * 1000)}"
            
        logger.info(f"Attempting to acquire workflow lock for thread {thread_id} (execution: {execution_id})")
        
        # Check if workflow is already running
        if self.is_workflow_running(thread_id):
            existing_execution = self._thread_locks[thread_id]
            logger.warning(f"Workflow already running for thread {thread_id} (execution: {existing_execution.execution_id})")
            raise RuntimeError(f"Workflow already running for thread {thread_id}")
        
        # Create new execution lock
        execution_lock = threading.Lock()
        
        # Try to acquire the lock with timeout
        lock_acquired = execution_lock.acquire(timeout=5)
        if not lock_acquired:
            logger.error(f"Failed to acquire workflow lock for thread {thread_id}")
            raise RuntimeError(f"Failed to acquire workflow lock for thread {thread_id}")
        
        try:
            # Register the execution
            with self._manager_lock:
                execution = WorkflowExecution(
                    thread_id=thread_id,
                    start_time=time.time(),
                    execution_id=execution_id,
                    lock=execution_lock
                )
                self._thread_locks[thread_id] = execution
                
            logger.info(f"Acquired workflow lock for thread {thread_id} (execution: {execution_id})")
            yield execution
            
        finally:
            # Always release and cleanup
            try:
                execution_lock.release()
                logger.info(f"Released workflow lock for thread {thread_id} (execution: {execution_id})")
            except Exception as e:
                logger.error(f"Error releasing workflow lock for thread {thread_id}: {e}")
            
            self._cleanup_execution(thread_id)
    
    def _cleanup_execution(self, thread_id: str):
        """Clean up execution tracking for a thread"""
        with self._manager_lock:
            if thread_id in self._thread_locks:
                execution = self._thread_locks[thread_id]
                logger.debug(f"Cleaning up workflow execution for thread {thread_id} (execution: {execution.execution_id})")
                del self._thread_locks[thread_id]
    
    def force_release_lock(self, thread_id: str) -> bool:
        """Force release a workflow lock (use with caution)"""
        logger.warning(f"Force releasing workflow lock for thread {thread_id}")
        
        with self._manager_lock:
            if thread_id in self._thread_locks:
                execution = self._thread_locks[thread_id]
                try:
                    if execution.lock.locked():
                        execution.lock.release()
                except Exception as e:
                    logger.error(f"Error force releasing lock for thread {thread_id}: {e}")
                    
                self._cleanup_execution(thread_id)
                return True
        
        return False
    
    def get_active_executions(self) -> Dict[str, str]:
        """Get currently active workflow executions"""
        active = {}
        current_time = time.time()
        
        with self._manager_lock:
            for thread_id, execution in list(self._thread_locks.items()):
                # Clean up timed out executions
                if current_time - execution.start_time > self._execution_timeout:
                    logger.warning(f"Cleaning up timed out execution for thread {thread_id}")
                    self._cleanup_execution(thread_id)
                    continue
                    
                if execution.lock.locked():
                    active[thread_id] = execution.execution_id
                else:
                    # Lock was released, clean up
                    self._cleanup_execution(thread_id)
        
        return active
    
    def set_timeout(self, timeout_seconds: int):
        """Set the workflow execution timeout"""
        self._execution_timeout = timeout_seconds
        logger.info(f"Workflow execution timeout set to {timeout_seconds} seconds")

# Global singleton instance
workflow_lock_manager = WorkflowLockManager()