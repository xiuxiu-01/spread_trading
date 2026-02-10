import asyncio
from src.services.order_service import OrderService
from gateway.okx_gateway import OKXGateway

async def main():
    okx = OKXGateway(symbol='XAU/USDT')
    order_service = OrderService(data_dir='data')  # 如需更改路径请调整
    result = await order_service.close_positions(okx, okx, {"dryRun": False})
    print(result)

if __name__ == "__main__":
    asyncio.run(main())
