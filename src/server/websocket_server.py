"""
WebSocket server for frontend communication.
"""

import asyncio
import json
from datetime import datetime
from typing import Set, Dict, Any, Optional, Callable, Awaitable

from ..config import settings
from ..config.logging_config import get_logger

logger = get_logger("server.websocket")

# Try to import websockets
try:
    import websockets
    from websockets.server import WebSocketServerProtocol
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    websockets = None
    WebSocketServerProtocol = Any
    WEBSOCKETS_AVAILABLE = False


class WebSocketServer:
    """
    WebSocket server for real-time frontend communication.
    
    Features:
    - Multiple client connections
    - Message broadcasting
    - Request/response handling
    """
    
    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8766,
        message_handler: Optional[Callable[[Any, Dict], Awaitable[None]]] = None
    ):
        """
        Initialize WebSocket server.
        
        Args:
            host: Server host
            port: Server port
            message_handler: Async function to handle incoming messages
        """
        if not WEBSOCKETS_AVAILABLE:
            raise ImportError("websockets library not installed. Run: pip install websockets")
        
        self.host = host
        self.port = port
        self.message_handler = message_handler
        
        self.clients: Set[WebSocketServerProtocol] = set()
        self.server = None
        self._running = False
    
    async def start(self):
        """Start the WebSocket server."""
        logger.info(f"Starting WebSocket server on {self.host}:{self.port}")
        
        self.server = await websockets.serve(
            self._handle_client,
            self.host,
            self.port,
            ping_interval=30,
            ping_timeout=10
        )
        
        self._running = True
        logger.info(f"WebSocket server running on ws://{self.host}:{self.port}/ws")
    
    async def stop(self):
        """Stop the WebSocket server."""
        self._running = False
        
        # Close all client connections
        if self.clients:
            await asyncio.gather(
                *[client.close() for client in self.clients],
                return_exceptions=True
            )
            self.clients.clear()
        
        # Close server
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            self.server = None
        
        logger.info("WebSocket server stopped")
    
    async def _handle_client(self, websocket: WebSocketServerProtocol):
        """Handle a client connection."""
        client_id = id(websocket)
        # Get remote address - may be None for some connection types
        try:
            client_addr = websocket.remote_address
        except Exception:
            client_addr = "unknown"
        
        logger.info(f"Client connected: {client_addr} (id={client_id})")
        self.clients.add(websocket)
        
        try:
            async for message in websocket:
                try:
                    data = json.loads(message)
                    
                    if self.message_handler:
                        await self.message_handler(websocket, data)
                    
                except json.JSONDecodeError as e:
                    logger.warning(f"Invalid JSON from {client_addr}: {e}")
                    await self._send(websocket, {
                        "type": "error",
                        "payload": {"message": "Invalid JSON"}
                    })
                    
        except websockets.exceptions.ConnectionClosed as e:
            logger.info(f"Client disconnected: {client_addr} (code={e.code})")
            
        except Exception as e:
            logger.error(f"Client error {client_addr}: {e}")
            
        finally:
            self.clients.discard(websocket)
            logger.debug(f"Client removed: {client_addr}")
    
    async def broadcast(self, message: Dict[str, Any]):
        """
        Broadcast message to all connected clients.
        
        Args:
            message: Message dictionary to broadcast
        """
        if not self.clients:
            return
        
        data = json.dumps(message)
        
        # Send to all clients concurrently
        await asyncio.gather(
            *[self._send_raw(client, data) for client in self.clients],
            return_exceptions=True
        )
    
    async def send_to(self, websocket: WebSocketServerProtocol, message: Dict[str, Any]):
        """
        Send message to a specific client.
        
        Args:
            websocket: Target client
            message: Message dictionary
        """
        await self._send(websocket, message)
    
    async def _send(self, websocket: WebSocketServerProtocol, message: Dict[str, Any]):
        """Send JSON message to client."""
        try:
            await websocket.send(json.dumps(message))
        except Exception as e:
            logger.warning(f"Send error: {e}")
    
    async def _send_raw(self, websocket: WebSocketServerProtocol, data: str):
        """Send raw string to client."""
        try:
            await websocket.send(data)
        except Exception:
            pass  # Ignore errors in broadcast
    
    @property
    def client_count(self) -> int:
        """Get number of connected clients."""
        return len(self.clients)
    
    @property
    def is_running(self) -> bool:
        """Check if server is running."""
        return self._running and self.server is not None
