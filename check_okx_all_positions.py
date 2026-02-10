from gateway.okx_gateway import OKXGateway

if __name__ == "__main__":
    okx = OKXGateway(symbol='XAU/USDT')  # symbol参数无影响，只用来初始化
    all_positions = okx.client.fetch_positions()
    print('OKX 全部持仓:')
    for pos in all_positions:
        print(pos)
