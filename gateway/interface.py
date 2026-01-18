from typing import Protocol, Iterable, Dict, Any

class MarketDataGateway(Protocol):
    symbol: str

    def get_orderbook(self, limit: int = 5) -> Dict[str, Any]:
        ...

    def get_ticker(self) -> Dict[str, Any]:
        ...

    def start_ws(self) -> None:
        ...

    def stop_ws(self) -> None:
        ...

    def latest_ws_snapshot(self) -> Dict[str, Any] | None:
        ...

class TradingGateway(Protocol):
    symbol: str

    def place_limit_order(self, side: str, amount: float, price: float, post_only: bool = True) -> Dict[str, Any]:
        ...

    def place_market_order(self, side: str, volume: float, *args, **kwargs) -> Dict[str, Any]:
        ...
