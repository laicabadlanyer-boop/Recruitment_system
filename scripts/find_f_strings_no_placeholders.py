from pathlib import Path

p = Path("app.py")
for i, line in enumerate(p.read_text(encoding="utf-8").splitlines(), 1):
    if "f'" in line or 'f"' in line:
        if "{" not in line and "}" not in line:
            print(i, line)
