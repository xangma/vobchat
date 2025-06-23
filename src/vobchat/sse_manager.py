# src/vobchat/sse_manager.py

import json
import logging
import threading
import time
import queue
import os
from typing import Dict, Any, Optional, List
from flask import Response
from vobchat.utils.redis_pool import redis_pool_manager

logger = logging.getLogger(__name__)

def make_json_safe(data: Any) -> Any:
    """Recursively convert data to JSON-safe format"""
    if isinstance(data, dict):
        return {k: make_json_safe(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [make_json_safe(item) for item in data]
    elif isinstance(data, (str, int, float, bool, type(None))):
        return data
    else:
        # Convert non-serializable objects to string
        return str(data)

class SSEEvent:
    """Base class for SSE events"""
    
    def __init__(self, event_type: str, data: Dict[str, Any], thread_id: str):
        self.event_type = event_type
        self.data = data
        self.thread_id = thread_id
        self.timestamp = time.time()
    
    def to_sse_format(self) -> str:
        """Convert event to SSE format"""
        try:
            data_json = json.dumps(self.data)
            return f"event: {self.event_type}\ndata: {data_json}\n\n"
        except Exception as e:
            logger.error(f"Error serializing SSE event data: {e}", exc_info=True)
            # Return a safe fallback event
            return f"event: error\ndata: {{\"error\": \"Failed to serialize event data\"}}\n\n"

class MessageEvent(SSEEvent):
    """Event for streaming AI messages"""
    
    def __init__(self, content: str, thread_id: str, is_partial: bool = False):
        super().__init__("message", {
            "content": content,
            "is_partial": is_partial,
            "thread_id": thread_id
        }, thread_id)

class InterruptEvent(SSEEvent):
    """Event for workflow interrupts (button choices, etc.)"""
    
    def __init__(self, interrupt_data: Dict[str, Any], thread_id: str):
        # Ensure interrupt_data is JSON serializable
        safe_data = make_json_safe(interrupt_data)
        super().__init__("interrupt", {
            "data": safe_data,
            "thread_id": thread_id
        }, thread_id)
    

class StateUpdateEvent(SSEEvent):
    """Event for workflow state updates"""
    
    def __init__(self, state_updates: Dict[str, Any], thread_id: str):
        # Ensure state_updates is JSON serializable
        safe_data = make_json_safe(state_updates)
        super().__init__("state_update", {
            "data": safe_data,
            "thread_id": thread_id
        }, thread_id)
    

class ErrorEvent(SSEEvent):
    """Event for errors"""
    
    def __init__(self, error: str, thread_id: str):
        super().__init__("error", {
            "error": error,
            "thread_id": thread_id
        }, thread_id)

class RedisSSEManager:
    """Redis-backed SSE manager that works across multiple Gunicorn workers"""
    
    def __init__(self):
        # Local worker state
        self.event_queues: Dict[str, queue.Queue] = {}  # client_id -> queue
        self.lock = threading.Lock()
        
        # Redis connection for shared state using connection pool
        self.redis_client = redis_pool_manager.get_sync_client()
        
        # Connection lifecycle management
        self.client_last_seen: Dict[str, float] = {}  # client_id -> timestamp
        self.heartbeat_interval = 30  # seconds
        self.connection_timeout = 90  # seconds
        
        # Redis key prefixes
        self.client_key_prefix = "sse:clients:"  # {thread_id} -> {client_id}
        self.worker_key_prefix = "sse:workers:"  # {client_id} -> {worker_id}
        
        # Worker ID for this process
        self.worker_id = f"worker_{os.getpid()}"
        
        print(f"DEBUG: Initialized Redis SSE Manager for worker {self.worker_id}")
        
        # Redis pub/sub for cross-worker event delivery
        self.pubsub_channel = "sse_events"
        # Create a dedicated client for pub/sub to avoid connection conflicts
        self.pubsub_client = redis_pool_manager.get_sync_client()
        self.pubsub = self.pubsub_client.pubsub()
        self.pubsub.subscribe(self.pubsub_channel)
        
        # Start background thread for pub/sub message handling
        self._pubsub_listener_started = False
        self._start_pubsub_listener()
        
        # Start cleanup thread for stale connections
        self._start_cleanup_thread()
    
    def _start_pubsub_listener(self):
        """Start background thread to listen for Redis pub/sub messages"""
        if self._pubsub_listener_started:
            print(f"DEBUG: Pub/sub listener already started for worker {self.worker_id}")
            return
            
        def listen_for_events():
            try:
                print(f"DEBUG: Started pub/sub listener for worker {self.worker_id}")
                for message in self.pubsub.listen():
                    if message['type'] == 'message':
                        try:
                            event_data = json.loads(message['data'])
                            target_worker = event_data.get('target_worker')
                            if target_worker == self.worker_id:
                                # This event is for us, deliver it locally
                                client_id = event_data.get('client_id')
                                event_type = event_data.get('event_type')
                                thread_id = event_data.get('thread_id')
                                
                                print(f"DEBUG: Received cross-worker event {event_type} for client {client_id}")
                                
                                # Reconstruct the SSE event
                                if event_type == 'message':
                                    event = MessageEvent(
                                        event_data['content'], 
                                        thread_id, 
                                        event_data.get('is_partial', False)
                                    )
                                elif event_type == 'interrupt':
                                    event = InterruptEvent(event_data['data'], thread_id)
                                elif event_type == 'state_update':
                                    event = StateUpdateEvent(event_data['data'], thread_id)
                                elif event_type == 'error':
                                    event = ErrorEvent(event_data['error'], thread_id)
                                else:
                                    continue
                                
                                # Deliver to local client
                                self._deliver_local_event(client_id, event)
                        except Exception as e:
                            print(f"DEBUG: Error processing pub/sub message: {e}")
            except Exception as e:
                print(f"DEBUG: Pub/sub listener error: {e}")
        
        import threading
        listener_thread = threading.Thread(target=listen_for_events, daemon=True)
        listener_thread.start()
        self._pubsub_listener_started = True
    
    def _start_cleanup_thread(self):
        """Start background thread to clean up stale connections"""
        def cleanup_stale_connections():
            while True:
                try:
                    time.sleep(30)  # Check every 30 seconds
                    current_time = time.time()
                    stale_clients = []
                    
                    with self.lock:
                        for client_id, last_seen in list(self.client_last_seen.items()):
                            if current_time - last_seen > self.connection_timeout:
                                stale_clients.append(client_id)
                                logger.warning(f"Client {client_id} timed out (last seen: {current_time - last_seen:.1f}s ago)")
                    
                    # Remove stale clients
                    for client_id in stale_clients:
                        self.remove_client(client_id)
                        
                except Exception as e:
                    logger.error(f"Error in cleanup thread: {e}")
        
        cleanup_thread = threading.Thread(target=cleanup_stale_connections, daemon=True, name="SSE-Cleanup")
        cleanup_thread.start()
        logger.info("Started SSE connection cleanup thread")
    
    def _deliver_local_event(self, client_id: str, event: SSEEvent):
        """Deliver an event to a local client"""
        if client_id in self.event_queues:
            try:
                self.event_queues[client_id].put_nowait(event)
                print(f"DEBUG: Successfully delivered cross-worker {event.event_type} event to client {client_id}")
            except queue.Full:
                print(f"DEBUG: Event queue full for client {client_id}")
        else:
            print(f"DEBUG: No local event queue found for client {client_id}")
    
    def add_client(self, client_id: str, thread_id: str):
        """Add a new SSE client using Redis for shared state"""
        with self.lock:
            # Store client mapping in Redis
            client_key = f"{self.client_key_prefix}{thread_id}"
            worker_key = f"{self.worker_key_prefix}{client_id}"
            
            # Clear any existing clients for this thread
            self.clear_thread_clients(thread_id)
            
            # Add new client to Redis
            self.redis_client.set(client_key, client_id, ex=300)  # 5 min expiry
            self.redis_client.set(worker_key, self.worker_id, ex=300)  # 5 min expiry
            
            # Create local event queue
            self.event_queues[client_id] = queue.Queue(maxsize=1000)  # Limit queue size
            
            # Track client for lifecycle management
            self.client_last_seen[client_id] = time.time()
            
            print(f"DEBUG: Added SSE client {client_id} for thread {thread_id} in worker {self.worker_id}")
            logger.info(f"Added SSE client {client_id} for thread {thread_id}")
    
    def remove_client(self, client_id: str):
        """Remove an SSE client"""
        with self.lock:
            # Find thread_id for this client
            worker_key = f"{self.worker_key_prefix}{client_id}"
            stored_worker = self.redis_client.get(worker_key)
            
            if stored_worker == self.worker_id:
                # Remove from Redis
                self.redis_client.delete(worker_key)
                
                # Find and remove client key
                for key in self.redis_client.scan_iter(match=f"{self.client_key_prefix}*"):
                    if self.redis_client.get(key) == client_id:
                        self.redis_client.delete(key)
                        break
                
                print(f"DEBUG: Removed SSE client {client_id} from Redis")
            
            # Remove local queue
            if client_id in self.event_queues:
                # Signal queue to close by putting None
                try:
                    self.event_queues[client_id].put_nowait(None)
                except queue.Full:
                    pass
                del self.event_queues[client_id]
                print(f"DEBUG: Removed local queue for client {client_id}")
            
            # Remove from lifecycle tracking
            if client_id in self.client_last_seen:
                del self.client_last_seen[client_id]
    
    def clear_thread_clients(self, thread_id: str):
        """Remove all existing clients for a thread"""
        client_key = f"{self.client_key_prefix}{thread_id}"
        existing_client = self.redis_client.get(client_key)
        
        if existing_client:
            print(f"DEBUG: Clearing existing client {existing_client} for thread {thread_id}")
            worker_key = f"{self.worker_key_prefix}{existing_client}"
            self.redis_client.delete(client_key)
            self.redis_client.delete(worker_key)
    
    def broadcast_event(self, event: SSEEvent):
        """Broadcast event to all clients listening to the thread"""
        print(f"DEBUG: Broadcasting {event.event_type} event to thread {event.thread_id}")
        
        # Find client for this thread in Redis
        client_key = f"{self.client_key_prefix}{event.thread_id}"
        client_id = self.redis_client.get(client_key)
        
        if not client_id:
            print(f"DEBUG: No Redis client found for thread {event.thread_id}")
            logger.warning(f"No clients found for thread {event.thread_id} - event not delivered")
            return
        
        # Check if client is on this worker
        worker_key = f"{self.worker_key_prefix}{client_id}"
        stored_worker = self.redis_client.get(worker_key)
        
        if stored_worker != self.worker_id:
            print(f"DEBUG: Client {client_id} is on worker {stored_worker}, not {self.worker_id}")
            print(f"DEBUG: Sending cross-worker event via Redis pub/sub")
            
            # Send event via Redis pub/sub for cross-worker delivery
            event_message = {
                'target_worker': stored_worker,
                'client_id': client_id,
                'event_type': event.event_type,
                'thread_id': event.thread_id
            }
            
            # Add event-specific data
            if hasattr(event, 'data') and hasattr(event.data, 'get'):
                if event.event_type == 'message':
                    event_message['content'] = event.data.get('content', '')
                    event_message['is_partial'] = event.data.get('is_partial', False)
                elif event.event_type in ['interrupt', 'state_update']:
                    event_message['data'] = event.data.get('data', {})
                elif event.event_type == 'error':
                    event_message['error'] = event.data.get('error', '')
            
            try:
                self.redis_client.publish(self.pubsub_channel, json.dumps(event_message))
                print(f"DEBUG: Published cross-worker event to Redis channel")
                logger.info(f"Sent {event.event_type} event to client {client_id} via pub/sub")
            except Exception as pub_error:
                print(f"DEBUG: Error publishing to Redis: {pub_error}")
                logger.error(f"Failed to publish event via Redis: {pub_error}")
            return
        
        # Client is on this worker, deliver locally
        if client_id in self.event_queues:
            try:
                self.event_queues[client_id].put_nowait(event)
                print(f"DEBUG: Successfully queued {event.event_type} event for client {client_id}")
                logger.debug(f"Sent {event.event_type} event to client {client_id}")
            except queue.Full:
                print(f"DEBUG: Event queue full for client {client_id}")
                logger.warning(f"Event queue full for client {client_id}")
        else:
            print(f"DEBUG: No local event queue found for client {client_id}")
    
    @property
    def clients(self):
        """Compatibility property for legacy code - returns Redis clients"""
        result = {}
        for key in self.redis_client.scan_iter(match=f"{self.client_key_prefix}*"):
            thread_id = key.replace(self.client_key_prefix, "")
            client_id = self.redis_client.get(key)
            if client_id:
                result[client_id] = thread_id
        return result
    
    def get_clients_summary(self) -> Dict[str, List[str]]:
        """Get a summary of clients by thread"""
        summary: Dict[str, List[str]] = {}
        for key in self.redis_client.scan_iter(match=f"{self.client_key_prefix}*"):
            thread_id = key.replace(self.client_key_prefix, "")
            client_id = self.redis_client.get(key)
            if client_id:
                if thread_id not in summary:
                    summary[thread_id] = []
                summary[thread_id].append(str(client_id))
        return summary
    
    def update_client_heartbeat(self, client_id: str):
        """Update the last seen timestamp for a client"""
        with self.lock:
            if client_id in self.event_queues:
                self.client_last_seen[client_id] = time.time()
                # Also update Redis TTL
                worker_key = f"{self.worker_key_prefix}{client_id}"
                self.redis_client.expire(worker_key, 300)  # Reset to 5 min
    
    def get_connection_status(self) -> Dict[str, Any]:
        """Get detailed connection status"""
        with self.lock:
            current_time = time.time()
            return {
                'total_connections': len(self.event_queues),
                'connections_by_thread': self.get_clients_summary(),
                'worker_id': self.worker_id,
                'stale_connections': [
                    {
                        'client_id': cid,
                        'idle_time': current_time - last_seen
                    }
                    for cid, last_seen in self.client_last_seen.items()
                    if current_time - last_seen > 60  # More than 1 minute idle
                ]
            }

class SimpleSSEManager:
    """Simple, synchronous SSE manager that works with Flask - LEGACY"""
    
    def __init__(self):
        self.clients: Dict[str, str] = {}  # client_id -> thread_id
        self.client_threads: Dict[str, str] = {}  # client_id -> thread_id
        self.event_queues: Dict[str, queue.Queue] = {}  # client_id -> queue
        self.lock = threading.Lock()
    
    def clear_thread_clients(self, thread_id: str):
        """Remove all existing clients for a thread"""
        with self.lock:
            clients_to_remove = [
                client_id for client_id, tid in self.clients.items()
                if tid == thread_id
            ]
            for client_id in clients_to_remove:
                print(f"DEBUG: Clearing old client {client_id} for thread {thread_id}")
                if client_id in self.clients:
                    del self.clients[client_id]
                if client_id in self.client_threads:
                    del self.client_threads[client_id]
                if client_id in self.event_queues:
                    # Signal the queue to close
                    try:
                        self.event_queues[client_id].put_nowait(None)
                    except queue.Full:
                        pass
                    del self.event_queues[client_id]
            if clients_to_remove:
                print(f"DEBUG: Cleared {len(clients_to_remove)} old clients for thread {thread_id}")

    def add_client(self, client_id: str, thread_id: str):
        """Add a new SSE client"""
        with self.lock:
            self.clients[client_id] = thread_id
            self.client_threads[client_id] = thread_id
            self.event_queues[client_id] = queue.Queue()
            print(f"DEBUG: Added SSE client {client_id} for thread {thread_id}")
            print(f"DEBUG: Total clients now: {len(self.clients)}, All mappings: {dict(self.clients)}")
            logger.info(f"Added SSE client {client_id} for thread {thread_id}")
    
    def remove_client(self, client_id: str):
        """Remove an SSE client"""
        with self.lock:
            if client_id in self.clients:
                thread_id = self.clients[client_id]
                print(f"DEBUG: Removing SSE client {client_id} for thread {thread_id}")
                import traceback
                print(f"DEBUG: Remove client called from: {traceback.format_stack()[-2].strip()}")
                del self.clients[client_id]
                del self.client_threads[client_id]
                if client_id in self.event_queues:
                    del self.event_queues[client_id]
                print(f"DEBUG: Client {client_id} removed, {len(self.clients)} clients remaining")
                logger.info(f"Removed SSE client {client_id} for thread {thread_id}")
            else:
                print(f"DEBUG: Attempted to remove non-existent client {client_id}")
    
    def broadcast_event(self, event: SSEEvent):
        """Broadcast event to all clients listening to the thread"""
        with self.lock:
            print(f"DEBUG: Broadcasting {event.event_type} event to thread {event.thread_id}")
            print(f"DEBUG: Current clients: {dict(self.clients)}")
            
            clients_to_notify = [
                client_id for client_id, thread_id in self.clients.items()
                if thread_id == event.thread_id
            ]
            
            print(f"DEBUG: Found {len(clients_to_notify)} clients for thread {event.thread_id}: {clients_to_notify}")
            
            for client_id in clients_to_notify:
                if client_id in self.event_queues:
                    try:
                        self.event_queues[client_id].put_nowait(event)
                        print(f"DEBUG: Successfully queued {event.event_type} event for client {client_id}")
                        logger.debug(f"Sent {event.event_type} event to client {client_id}")
                    except queue.Full:
                        print(f"DEBUG: Event queue full for client {client_id}")
                        logger.warning(f"Event queue full for client {client_id}")
                else:
                    print(f"DEBUG: No event queue found for client {client_id}")
            
            if not clients_to_notify:
                print(f"DEBUG: No clients found for thread {event.thread_id} - event not delivered")
                logger.warning(f"No clients found for thread {event.thread_id} - event not delivered")
    
    def broadcast_event_sync(self, event: SSEEvent):
        """Synchronous version of broadcast_event"""
        self.broadcast_event(event)
    
    def update_client_heartbeat(self, client_id: str):
        """Update heartbeat for client (no-op for simple manager)"""
        pass  # Simple manager doesn't track heartbeats
    
    def get_connection_status(self) -> Dict[str, Any]:
        """Get connection status"""
        with self.lock:
            return {
                'total_connections': len(self.clients),
                'connections_by_thread': dict(self.client_threads),
                'worker_id': 'simple-manager'
            }

# Global SSE manager instance
# Use Redis-backed manager for multi-worker support
try:
    sse_manager = RedisSSEManager()
    print("DEBUG: Using Redis-backed SSE manager")
except Exception as e:
    print(f"DEBUG: Failed to initialize Redis SSE manager: {e}")
    print("DEBUG: Falling back to simple SSE manager")
    sse_manager = SimpleSSEManager()

def create_sse_response(client_id: str) -> Response:
    """Create an SSE response for a client"""
    
    def event_stream():
        try:
            print(f"DEBUG: Starting SSE stream for client {client_id}")
            # Send initial connection event
            connected_data = {'client_id': client_id, 'timestamp': time.time()}
            connected_event = f"event: connected\ndata: {json.dumps(connected_data)}\n\n"
            print(f"DEBUG: Sending connected event: {connected_event.strip()}")
            yield connected_event
            
            # Keep connection alive and yield events from queue
            last_heartbeat = time.time()
            
            while client_id in sse_manager.clients:
                try:
                    # Check for events in client queue
                    if client_id in sse_manager.event_queues:
                        queue_obj = sse_manager.event_queues[client_id]
                        
                        # Try to get event with very short timeout to avoid worker blocking
                        try:
                            event = queue_obj.get(timeout=0.1)  # Much shorter timeout
                            # Check for shutdown signal
                            if event is None:
                                print(f"DEBUG: Shutdown signal received for client {client_id}")
                                break
                            event_data = event.to_sse_format()
                            print(f"DEBUG: Sending SSE event: {event.event_type} - {event_data[:100]}...")
                            yield event_data
                            continue  # Skip sleep if we got an event
                        except queue.Empty:
                            pass  # No events, continue to heartbeat check
                    
                    # Send heartbeat more frequently and yield control
                    current_time = time.time()
                    if current_time - last_heartbeat > 10:  # Send heartbeat every 10 seconds
                        heartbeat_event = f"event: heartbeat\ndata: {json.dumps({'timestamp': current_time})}\n\n"
                        print(f"DEBUG: Sending heartbeat: {heartbeat_event.strip()}")
                        yield heartbeat_event
                        last_heartbeat = current_time
                        
                        # Update client heartbeat in SSE manager if using Redis manager
                        if hasattr(sse_manager, 'update_client_heartbeat'):
                            sse_manager.update_client_heartbeat(client_id)
                    
                    # Very short sleep to yield control to other threads/processes
                    time.sleep(0.1)
                        
                except Exception as e:
                    print(f"DEBUG: Error in event stream loop for client {client_id}: {e}")
                    logger.error(f"Error in event stream for client {client_id}: {e}", exc_info=True)
                    # Break on error to avoid infinite loops
                    break
                    
        except Exception as e:
            print(f"DEBUG: Error in SSE stream setup for client {client_id}: {e}")
            logger.error(f"Error in SSE stream for client {client_id}: {e}", exc_info=True)
            try:
                error_event = f"event: error\ndata: {json.dumps({'error': str(e)})}\n\n"
                print(f"DEBUG: Sending error event: {error_event.strip()}")
                yield error_event
            except Exception as error_e:
                print(f"DEBUG: Failed to send error event: {error_e}")
        finally:
            # Clean up client when connection closes
            print(f"DEBUG: SSE stream generator finished for client {client_id}")
            print(f"DEBUG: About to clean up SSE client {client_id}")
            sse_manager.remove_client(client_id)
            logger.info(f"SSE connection closed for client {client_id}")
    
    return Response(
        event_stream(),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Cache-Control',
            'X-Accel-Buffering': 'no'  # Disable nginx buffering
        }
    )