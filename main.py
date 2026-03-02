import os
import json
from agents.agent_b_extraction import run_extraction


def main():
    bundles_dir = "input_bundles"

    # List available bundles
    bundles = sorted([
        b for b in os.listdir(bundles_dir)
        if os.path.isdir(os.path.join(bundles_dir, b))
    ])

    print("Available input bundles:")
    print("-" * 40)
    for i, bundle in enumerate(bundles, 1):
        print(f"  {i}. {bundle}")
    print()

    choice = input("Pick a bundle number (or press Enter for s01): ").strip()

    if choice == "":
        selected = bundles[0]
    elif choice.isdigit() and 1 <= int(choice) <= len(bundles):
        selected = bundles[int(choice) - 1]
    else:
        print(f"Invalid choice: {choice}")
        return

    bundle_path = os.path.join(bundles_dir, selected)
    print()

    # Run Agent B extraction
    output_path = run_extraction(bundle_path)

    # Print the result
    print()
    print("=" * 50)
    print("EXTRACTED DATA:")
    print("=" * 50)
    with open(output_path, "r") as f:
        data = json.load(f)
    print(json.dumps(data, indent=2))


if __name__ == "__main__":
    main()
