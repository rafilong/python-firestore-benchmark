# Install

```
pip install -r requirements.txt
```

# Usage

Benchmark searches for file named `upload` to test.

```
head -c 500000 </dev/urandom > upload
python benchmark.py
```
