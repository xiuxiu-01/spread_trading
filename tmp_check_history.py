import json
from math import isnan

path = "data/history.jsonl"
with open(path, "r", encoding="utf-8") as f:
    for idx, line in enumerate(f):
        data = json.loads(line)
        for side in ("mt5", "okx"):
            bar = data.get(side, {})
            for key in ("open", "high", "low", "close"):
                val = bar.get(key)
                if val is None:
                    print(f"{side} {key} is None at idx {idx} ts {data.get('ts')}")
                    raise SystemExit(1)
                if isinstance(val, float) and isnan(val):
                    print(f"{side} {key} is NaN at idx {idx} ts {data.get('ts')}")
                    raise SystemExit(1)
print("history OK")
