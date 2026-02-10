from gateway.okx_gateway import OKXGateway

if __name__ == "__main__":
    okx = OKXGateway(symbol='XAU/USDT')
    positions = okx.get_positions()
    print('OKX XAU/USDT 持仓:')
    for pos in positions:
        print(pos)
