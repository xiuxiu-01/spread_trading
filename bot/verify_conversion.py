
def simulate_conversion(amount, contract_size):
    num_contracts = amount / contract_size
    using_int = int(num_contracts)
    using_round = int(round(num_contracts))
    print(f"Amount: {amount}, Size: {contract_size}")
    print(f"  Exact Contracts: {num_contracts}")
    print(f"  Using int():     {using_int}")
    print(f"  Using round():   {using_round}")
    return using_round

print("--- Case 1: Perfect Alignment ---")
simulate_conversion(1.0, 0.001)

print("\n--- Case 2: Floating Point Undershoot (0.999...) ---")
# Simulating a slightly smaller amount due to float math
amount = 1.0 - 1e-9
simulate_conversion(amount, 0.001)

print("\n--- Case 3: Small Trade (e.g. 0.005 oz -> 5 contracts) ---")
simulate_conversion(0.005, 0.001)

print("\n--- Case 4: Small Trade Undershoot ---")
amount_small = 0.005 - 1e-10
simulate_conversion(amount_small, 0.001)
