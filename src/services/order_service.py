"""
Order Service - Handles order execution and tracking.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any

from ..models import Order, OrderSide, OrderStatus
from ..gateway import BaseGateway
from ..config.logging_config import get_logger

logger = get_logger("services.order")


class OrderService:
    """
    Order execution and management service.
    
    Handles:
    - Order execution across exchanges
    - Order tracking and history
    - Position reconciliation
    """
    
    def __init__(self, data_dir: Path):
        """
        Initialize order service.
        
        Args:
            data_dir: Directory for order logs
        """
        self.data_dir = data_dir
        self.orders_file = data_dir / "orders.jsonl"
        
        # Order tracking
        self.pending_orders: Dict[str, Order] = {}
        self.order_history: List[Order] = []
        
        # Load history
        self._load_history()
    
    def _load_history(self):
        """Load order history from file."""
        if not self.orders_file.exists():
            return
        
        try:
            with open(self.orders_file, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        data = json.loads(line.strip())
                        # Reconstruct Order object (simplified)
                        self.order_history.append(data)
                    except json.JSONDecodeError:
                        continue
        except Exception as e:
            logger.error(f"Error loading order history: {e}")
    
    async def execute_arbitrage_orders(
        self,
        gateway_a: BaseGateway,
        gateway_b: BaseGateway,
        direction: str,
        volume: float,
        dry_run: bool = True,
        qty_multiplier: float = 100.0
    ) -> Dict[str, Any]:
        """
        Execute arbitrage order pair.
        
        Args:
            gateway_a: First exchange gateway (e.g., MT5)
            gateway_b: Second exchange gateway (e.g., OKX)
            direction: "long" (buy A, sell B) or "short" (sell A, buy B)
            volume: Order volume (base on gateway_a)
            dry_run: If True, simulate orders without sending
            qty_multiplier: Multiplier for gateway_b volume
        
        Returns:
            Result dictionary with order details
        """
        result = {
            "success": False,
            "direction": direction,
            "volume": volume,
            "qty_multiplier": qty_multiplier,
            "order_a": None,
            "order_b": None,
            "error": None,
            "dry_run": dry_run
        }
        
        # Calculate volumes
        volume_a = volume
        volume_b = volume * qty_multiplier

        if dry_run:
            logger.info(f"[DRY_RUN] Simulating arbitrage orders: {direction} A:{volume_a} B:{volume_b}")
        
        try:
            if direction == "long":
                # Buy on A, Sell on B
                order_a = await gateway_a.place_order(OrderSide.BUY, volume_a, dry_run=dry_run)
                order_b = await gateway_b.place_order(OrderSide.SELL, volume_b, dry_run=dry_run)
            else:
                # Sell on A, Buy on B
                order_a = await gateway_a.place_order(OrderSide.SELL, volume_a, dry_run=dry_run)
                order_b = await gateway_b.place_order(OrderSide.BUY, volume_b, dry_run=dry_run)
            
            result["order_a"] = order_a.to_dict()
            result["order_b"] = order_b.to_dict()
            
            # Check if both filled
            if order_a.status == OrderStatus.FILLED and order_b.status == OrderStatus.FILLED:
                result["success"] = True
                logger.info(f"Arbitrage orders executed: {direction} A:{volume_a} B:{volume_b}")
            else:
                result["error"] = "One or both orders not filled"
                logger.warning(f"Partial fill: A={order_a.status.value}, B={order_b.status.value}")
            
            # Save to history
            self._save_order(order_a)
            self._save_order(order_b)
            
        except Exception as e:
            result["error"] = str(e)
            logger.error(f"Arbitrage order error: {e}")
        
        return result
    
    async def close_arbitrage_position(
        self,
        gateway_a: BaseGateway,
        gateway_b: BaseGateway,
        direction: str,
        volume: float,
        dry_run: bool = True,
        qty_multiplier: float = 100.0
    ) -> Dict[str, Any]:
        """
        Close arbitrage position.
        
        Args:
            gateway_a: First exchange gateway
            gateway_b: Second exchange gateway
            direction: Current position direction
            volume: Position volume to close
            dry_run: If True, simulate orders without sending
            qty_multiplier: Multiplier for gateway_b volume
        
        Returns:
            Result dictionary
        """
        # Close is opposite of open
        close_direction = "short" if direction == "long" else "long"
        return await self.execute_arbitrage_orders(
            gateway_a, gateway_b, close_direction, volume, dry_run=dry_run, qty_multiplier=qty_multiplier
        )
    
    def _save_order(self, order: Order):
        """Save order to history file."""
        try:
            self.order_history.append(order.to_dict())
            
            with open(self.orders_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(order.to_dict()) + "\n")
                
        except Exception as e:
            logger.error(f"Error saving order: {e}")
    
    def get_recent_orders(self, limit: int = 50) -> List[Dict]:
        """Get recent orders."""
        return self.order_history[-limit:]
    
    def get_orders_by_date(self, date: str) -> List[Dict]:
        """Get orders for a specific date (YYYY-MM-DD)."""
        return [
            o for o in self.order_history
            if o.get("created_at", "").startswith(date)
        ]
    
    def calculate_pnl(self, orders: List[Dict]) -> Dict[str, float]:
        """
        Calculate PnL from order pairs.
        
        This is a simplified calculation.
        Real PnL would need position tracking.
        """
        # Placeholder implementation
        return {
            "realized_pnl": 0.0,
            "commission": sum(o.get("commission", 0) for o in orders),
            "trade_count": len(orders) // 2
        }

    async def close_positions(self, gateway_a: BaseGateway, gateway_b: BaseGateway, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Close all positions on both gateways.
        
        Args:
            gateway_a: First exchange gateway
            gateway_b: Second exchange gateway
            params: Parameters (dryRun, etc.)
            
        Returns:
            Result dictionary
        """
        dry_run = params.get("dryRun", True)
        result = {"status": "unknown", "closed_a": None, "closed_b": None}
        
        try:
            # 1. Check positions
            pos_a = await gateway_a.get_position()
            pos_b = await gateway_b.get_position()
            
            # 2. Close A if exists
            if pos_a and pos_a.volume > 0:
                # Close by placing opposite order
                side = OrderSide.SELL if pos_a.side.value == "long" else OrderSide.BUY
                result["closed_a"] = await gateway_a.place_order(side, pos_a.volume, dry_run=dry_run)
            
            # 3. Close B if exists
            if pos_b and pos_b.volume > 0:
                side = OrderSide.SELL if pos_b.side.value == "long" else OrderSide.BUY
                result["closed_b"] = await gateway_b.place_order(side, pos_b.volume, dry_run=dry_run)
                
            result["status"] = "closed"
            
        except Exception as e:
            result["status"] = "failed"
            result["error"] = str(e)
            logger.error(f"Error closing positions: {e}")
            
        return result
