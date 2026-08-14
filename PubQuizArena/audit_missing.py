import re

html_path = "/Users/mfondin/Opencode_projects/Word_docs/Gamenight/PubQuizArena/index.html"
html = open(html_path).read()

# Find all jQuery selectors used in the JS
selectors = re.findall(r"""\$\('#([^']+)'\s*""", html)
# Deduplicate
unique = sorted(set(selectors))

# Find all IDs
ids = set(re.findall(r'id="([^"]+)"', html))

# Find missing
missing = [s for s in unique if s not in ids]

print("=== MISSING DOM ELEMENTS ===")
for m in missing:
    # Find context
    pattern = r"\$\('#" + re.escape(m) + r"'\s*"
    matches = list(re.finditer(pattern, html))
    for match in matches:
        line = html[:match.start()].count('\n') + 1
        context = html[max(0, match.start()-80):match.start()+80]
        print(f"\n  #{m} at line {line}: ...{context}...")

print(f"\n=== TOTAL: {len(unique)} unique selectors, {len(missing)} missing ===")
PYEOF