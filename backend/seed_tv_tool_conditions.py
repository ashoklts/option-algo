"""
seed_tv_tool_conditions.py
───────────────────────────
Seed the TradingView line-tool -> alert-condition mapping into MongoDB.
Collection: stock_data.signal_tv_tool_condition

Run once:
    python3 seed_tv_tool_conditions.py
"""

from signal_builder.service import get_tv_tool_conditions, seed_tv_tool_conditions


def main():
    result = seed_tv_tool_conditions()
    print(f"Upserted {result['upserted']} tool-condition records.")

    print("\nVerification:")
    for doc in get_tv_tool_conditions():
        labels = ", ".join(c["label"] for c in doc["conditions"])
        print(f"  {doc['tool_name']:18s} [{doc['tool_group']:11s}] _id={doc['_id']}  -> {labels}")


if __name__ == "__main__":
    main()
