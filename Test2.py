
repos:
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.1.6  # Use the latest version
    hooks:
      - id: ruff
      - id: ruff-format



pip install flake8 black pylint

flake8 .
pylint my_script.py
black --check .


print("Keys in input_data:", input_data.keys())
print("Rules Key Exists:", "rules" in input_data)
