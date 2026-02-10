from gateway.okx_gateway import OKXGateway

if __name__ == "__main__":
    okx = OKXGateway(symbol='XAU/USDT')  # symbol参数无影响，只用来初始化
    balance = okx.client.fetch_balance()
    print('OKX 资金情况:')
    for asset, info in balance.items():
        print(asset, info)
