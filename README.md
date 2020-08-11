# Install

```
pip install -r requirements.txt
```

# Usage

Benchmark uses `upload` file to test.

```
head -c 500000 </dev/urandom > upload
python benchmark.py
```
