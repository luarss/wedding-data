"""Debug deduplication matching."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.shared.deduplication import (
    calculate_name_similarity,
    calculate_location_similarity,
    normalize_name,
)

# Test cases
test_cases = [
    ("Aloft Singapore Novena", "Capella Singapore"),
    ("Aloft Singapore Novena", "Singapore Zoo"),
    ("Aloft Singapore Novena", "Aloft Singapore Novena"),
    ("Andaz Singapore", "Amara Singapore"),
    ("Ah Yat Seafood Restaurant", "Ah Yat Seafood Restaurant"),
    ("1-Altitude Coast", "1-Atico"),
]

print("Name Similarity Tests")
print("=" * 80)

for name1, name2 in test_cases:
    sim = calculate_name_similarity(name1, name2)
    norm1 = normalize_name(name1)
    norm2 = normalize_name(name2)
    print(f"\n{name1!r} vs {name2!r}")
    print(f"  Normalized: {norm1!r} vs {norm2!r}")
    print(f"  Similarity: {sim:.1f}")
