import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).parent))
from old.mt5_client import init_mt5, shutdown_mt5, stream_ticks

if __name__ == '__main__':
    try:
        init_mt5()
        print('Streaming MT5 ticks for XAU... (Ctrl+C to stop)')
        for t in stream_ticks('XAU', interval_sec=0.25):
            print(t)
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print('Error:', e)
    finally:
        shutdown_mt5()
