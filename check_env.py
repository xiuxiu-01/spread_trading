
import os
from src.config.settings import settings

print("All OKX keys:")
for k, v in os.environ.items():
    if k.startswith("OKX_"):
        print(f"{k}: {'*' * len(v) if v else 'Empty'}")

conf = settings.get_exchange('okx')
if conf:
    print(f"OKX Config Loaded: Enabled={conf.enabled}, KeyLength={len(conf.api_key)}")
    print(f"OKX Password present: {bool(conf.password)}")
else:
    print("OKX Config NOT loaded")
