"""Async event loop management for VobChat SSE functionality"""

import asyncio
import threading
import logging
from typing import Any, Callable, Optional, Dict
from concurrent.futures import ThreadPoolExecutor
import functools

logger = logging.getLogger(__name__)

class AsyncManager:
    """Centralized async event loop management"""
    
    _instance = None
    _event_loop: Optional[asyncio.AbstractEventLoop] = None
    _loop_thread: Optional[threading.Thread] = None
    _executor: Optional[ThreadPoolExecutor] = None
    _loop_lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(AsyncManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not hasattr(self, '_initialized'):
            self._initialized = True
            self._start_event_loop()
    
    def _start_event_loop(self):
        """Start a dedicated event loop in a background thread"""
        with self._loop_lock:
            if self._event_loop is not None and not self._event_loop.is_closed():
                return  # Loop already running
                
            def loop_runner():
                """Run the event loop in a dedicated thread"""
                try:
                    self._event_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(self._event_loop)
                    logger.info("AsyncManager: Started dedicated event loop")
                    self._event_loop.run_forever()
                except Exception as e:
                    logger.error(f"AsyncManager: Error in event loop: {e}")
                finally:
                    logger.info("AsyncManager: Event loop stopped")
            
            self._loop_thread = threading.Thread(target=loop_runner, daemon=True, name="AsyncManager-Loop")
            self._loop_thread.start()
            
            # Wait for loop to be ready
            import time
            max_wait = 5  # seconds
            waited = 0
            while (self._event_loop is None or not self._event_loop.is_running()) and waited < max_wait:
                time.sleep(0.1)
                waited += 0.1
            
            if self._event_loop is None:
                raise RuntimeError("Failed to start async event loop")
                
            # Create thread pool executor
            self._executor = ThreadPoolExecutor(max_workers=10, thread_name_prefix="AsyncManager-Worker")
            logger.info("AsyncManager: Initialization complete")
    
    def run_async(self, coro, timeout: Optional[float] = None) -> Any:
        """Run an async coroutine safely in the managed event loop"""
        if self._event_loop is None or self._event_loop.is_closed():
            raise RuntimeError("Event loop not available")
        
        try:
            future = asyncio.run_coroutine_threadsafe(coro, self._event_loop)
            return future.result(timeout=timeout)
        except Exception as e:
            logger.error(f"AsyncManager: Error running coroutine: {e}")
            raise
    
    def run_async_generator(self, async_gen, callback: Callable[[Any], None], timeout: Optional[float] = None):
        """Run an async generator and call callback for each yielded value"""
        async def consume_generator():
            try:
                async for item in async_gen:
                    callback(item)
            except Exception as e:
                logger.error(f"AsyncManager: Error in async generator: {e}")
                raise
        
        return self.run_async(consume_generator(), timeout=timeout)
    
    def submit_task(self, coro) -> asyncio.Future:
        """Submit a coroutine as a task to the event loop (fire and forget)"""
        if self._event_loop is None or self._event_loop.is_closed():
            raise RuntimeError("Event loop not available")
        
        return asyncio.run_coroutine_threadsafe(coro, self._event_loop)
    
    def run_in_thread(self, func: Callable, *args, **kwargs):
        """Run a blocking function in a thread pool"""
        if self._executor is None:
            raise RuntimeError("Thread pool executor not available")
        
        return self._executor.submit(func, *args, **kwargs)
    
    def is_healthy(self) -> bool:
        """Check if the async manager is healthy"""
        return (
            self._event_loop is not None 
            and not self._event_loop.is_closed() 
            and self._event_loop.is_running()
            and self._loop_thread is not None 
            and self._loop_thread.is_alive()
        )
    
    def shutdown(self):
        """Gracefully shutdown the async manager"""
        logger.info("AsyncManager: Starting shutdown")
        
        if self._executor:
            self._executor.shutdown(wait=True)
            self._executor = None
        
        if self._event_loop and not self._event_loop.is_closed():
            self._event_loop.call_soon_threadsafe(self._event_loop.stop)
            
        if self._loop_thread and self._loop_thread.is_alive():
            self._loop_thread.join(timeout=5)
            
        logger.info("AsyncManager: Shutdown complete")

# Global singleton instance
async_manager = AsyncManager()

def async_safe(timeout: Optional[float] = None):
    """Decorator to run async functions safely using the managed event loop"""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if asyncio.iscoroutinefunction(func):
                coro = func(*args, **kwargs)
                return async_manager.run_async(coro, timeout=timeout)
            else:
                # Run sync function in thread pool
                return async_manager.run_in_thread(func, *args, **kwargs).result(timeout=timeout)
        return wrapper
    return decorator